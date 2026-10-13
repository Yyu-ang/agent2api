//! Tabbit 对话转发：建会话 → 签名 POST → SSE 流式 → OpenAI 形状。
//!
//! ── 上游是**会话式两步走**（tabbit2api 实测，本模块照抄）────────
//!   1. `POST /panel/session`（空 body，**不带签名头**）→ `{chat_session_id}`；
//!   2. `POST /api/v1/chat/completion`（HMAC 签名头，见 `protocol.rs`）→ SSE。
//! SSE 事件（`event:` / `data:` 行）：`message_chunk`（`data.content` 增量、
//! `data.reasoning_content` 思考）/ `finish` / `close` / `cancel`（结束）/
//! `error`（`data.message` + `data.code`，终止流）。
//!
//! ── 多轮与模型 ──────────────────────────────────────────────
//! 上游 `content` 是单条文本：本轮把 OpenAI messages 拍平成一条 prompt
//! （system → `[系统指令]`、assistant → `[助手之前的回复]`，与 kuku 的
//! `build_prompt` 同口径）。参考实现的「增量发送」（只发新增消息、历史靠
//! 上游会话记忆）是带宽优化，本版先全量 —— 正确性不受影响（见遗留问题）。
//! 模型参数 `selected_model` 必须传目录里的 `display_name`
//! （`models::resolve_model` 归一）。
//!
//! ── 会话是请求内的 ─────────────────────────────────────────
//! 与 KukuAI 同属 `is_stateful = true` 但会话**不跨请求**（每次新建、用毕即弃），
//! 因此没有会话注册表 —— `run_chat` 单函数完成「建会话 → 流式 → 收尾」。
//!
//! ── 错误口径 ──────────────────────────────────────────────
//!   - 401/403 → 登录态失效（上游无刷新接口）：`GatewayError` 401，由
//!     `classify_conversation_error` 归为 QuotaLimited（标记冷却 + 换下一账号，
//!     与参考实现「401/403 标记登录失效并切换下一账号」同语义）；
//!   - 499 → 签名 key 过期：刷新 key 后**同一会话**重试一次（参考同逻辑）；
//!   - SSE `error` 事件 code=492 → `explain_error` 翻译（上游文案与真实原因无关）；
//!   - 409「会话处理中」→ 未产出内容时等待 2 秒重试同一会话（最多 2 次，
//!     参考 `BUSY_RETRY`）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Map, Value, json};
use tokio_stream::wrappers::ReceiverStream;

use crate::server::core::account_store::AccountStore;
use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::core::upstream::ForwardOutcome;
use crate::server::errors::GatewayError;

use super::credentials::{self, TabbitCredentials};
use super::protocol;
use super::region::Region;

/// 建会话 / 对话请求超时
const REQUEST_TIMEOUT_MS: u64 = 60_000;

/// 会话忙（409）等待时长（参考 `BUSY_WAIT`）
const BUSY_WAIT_MS: u64 = 2_000;

/// 会话忙最大重试次数（参考 `BUSY_RETRY`）
const BUSY_RETRY_MAX: usize = 2;

/// 转发入口（有状态路径 `forward_conversation` 转调）。
pub async fn run_chat(
    region: Region,
    store: &AccountStore,
    account_id: &str,
    body: &Value,
    proxy: Option<ResolvedProxy>,
    stream: bool,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<ForwardOutcome, GatewayError> {
    let credentials = credentials::snapshot_for(region, store, account_id)?;
    let client_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("默认")
        .to_string();
    let model = super::models::resolve_model(region, &client_model);
    let prompt = build_prompt(body.get("messages"));
    let proxy_ref = proxy.as_ref();

    // ── 建会话 ──────────────────────────────────────────────
    let session_id = create_session(region, &credentials, proxy_ref).await?;

    // ── 对话（SSE；499 自动刷新签名 key 重试一次）─────────────
    let url = format!("{}/api/v1/chat/completion", region.base_url());
    let raw_body = protocol::chat_body(&session_id, &prompt, &model);
    let response = send_chat_with_retry(region, &credentials, &session_id, &raw_body, &url, proxy_ref)
        .await?;

    if let Some(capture) = telemetry.capture().as_deref() {
        let headers = protocol::chat_headers(region, &credentials, &session_id, &raw_body, "");
        capture.reset_request(&url, super::tabbit_id(region), &headers, body);
        capture.attach_response(response.status().as_u16(), response.headers());
    }

    if !stream {
        return aggregate(response, &client_model, &prompt).await;
    }

    // 流式：后台任务喂 mpsc，主路径返回 ReceiverStream（与 KukuAI 同结构）
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let requested = client_model;
    let prompt_chars = prompt.chars().count();
    crate::spawn_task(async move {
        let outcome = drive_stream(response, &requested, prompt_chars, &sender).await;
        if let Err(error) = outcome {
            crate::server::logging::verbose("[Tabbit]", &format!("对话流处理中断：{}", error.message));
        }
    });
    Ok(ForwardOutcome::Stream {
        status: 200,
        stream: Box::new(ReceiverStream::new(receiver)),
    })
}

/// 建会话（`POST /panel/session`，空 body，不带签名头）→ `chat_session_id`。
async fn create_session(
    region: Region,
    credentials: &TabbitCredentials,
    proxy: Option<&ResolvedProxy>,
) -> Result<String, GatewayError> {
    let url = format!("{}/panel/session", region.base_url());
    let mut headers = protocol::base_headers(region, credentials, "/newtab");
    headers.push(("Accept".to_string(), "application/json".to_string()));
    headers.push(("Pragma".to_string(), "no-cache".to_string()));
    headers.push(("Cache-Control".to_string(), "no-cache".to_string()));
    // 空 body 的 POST：http::post_json_value 会拼 Content-Type json 头；
    // 参考实现发的是 content=b"" —— 这里用空 JSON 对象，上游兼容（空 body）
    let value = super::http::post_json_value(&url, &headers, &json!({}), proxy, Some(REQUEST_TIMEOUT_MS))
        .await?;
    let session_id = value
        .get("chat_session_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if session_id.is_empty() {
        return Err(GatewayError::with_status(
            502,
            "Tabbit 建会话返回缺少 chat_session_id（上游响应异常）",
        ));
    }
    Ok(session_id)
}

/// 发对话请求（499 时刷新签名 key 重试一次）。
async fn send_chat_with_retry(
    region: Region,
    credentials: &TabbitCredentials,
    session_id: &str,
    raw_body: &str,
    url: &str,
    proxy: Option<&ResolvedProxy>,
) -> Result<reqwest::Response, GatewayError> {
    let mut sign_key = protocol::ensure_sign_key(region, credentials, proxy, false).await;
    for attempt in 0..2 {
        let headers = protocol::chat_headers(region, credentials, session_id, raw_body, &sign_key);
        match super::http::sse_post(url, &headers, raw_body, proxy, "Tabbit 对话").await {
            Ok(response) => return Ok(response),
            Err(error) => {
                if error.status_code == 499 && attempt == 0 {
                    sign_key = protocol::ensure_sign_key(region, credentials, proxy, true).await;
                    crate::server::logging::verbose("[Tabbit]", "签名 key 过期（499），已刷新重试");
                    continue;
                }
                return Err(error);
            }
        }
    }
    Err(GatewayError::with_status(502, "Tabbit 对话请求失败（签名重试仍失败）"))
}

/// 把 OpenAI messages 拍平成单条 prompt（上游 content 是单文本，参考同口径）。
fn build_prompt(messages: Option<&Value>) -> String {
    let Some(Value::Array(items)) = messages else {
        return "你好".to_string();
    };
    if items.is_empty() {
        return "你好".to_string();
    }
    if items.len() == 1 {
        return content_text(&items[0]).unwrap_or_else(|| "你好".to_string());
    }
    let mut lines: Vec<String> = Vec::new();
    for message in items {
        let role = message.get("role").and_then(Value::as_str).unwrap_or("user");
        let Some(text) = content_text(message) else { continue };
        if text.trim().is_empty() {
            continue;
        }
        match role {
            "system" => lines.push(format!("[系统指令] {text}")),
            "assistant" => lines.push(format!("[助手之前的回复] {text}")),
            _ => lines.push(text),
        }
    }
    if lines.is_empty() {
        return "你好".to_string();
    }
    lines.join("\n\n")
}

/// 取一条消息的可读文本（content 字符串或 text 类型的部件数组）。
fn content_text(message: &Value) -> Option<String> {
    let content = message.get("content")?;
    match content {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let mut out: Vec<String> = Vec::new();
            for part in parts {
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    out.push(text.to_string());
                }
            }
            if out.is_empty() {
                None
            } else {
                Some(out.join("\n"))
            }
        }
        _ => None,
    }
}

// ─── SSE 解析 ────────────────────────────────────────────────

/// 一条已解析的上游事件。
enum TabbitEvent {
    Delta(String),
    Reasoning(String),
    Done,
    Error { message: String, code: Option<i64> },
}

/// 按 `\n\n` 分帧、逐帧取 `event:` / `data:` 的解析器。
#[derive(Default)]
struct SseScanner {
    buffer: String,
}

impl SseScanner {
    fn push(&mut self, text: &str) -> Vec<TabbitEvent> {
        self.buffer.push_str(text);
        let mut events = Vec::new();
        while let Some(split_at) = self.buffer.find("\n\n") {
            let raw: String = self.buffer.drain(..=split_at + 1).collect();
            let mut name: Option<&str> = None;
            let mut data_lines: Vec<&str> = Vec::new();
            for line in raw.split('\n') {
                if let Some(event) = line.strip_prefix("event:") {
                    name = Some(event.trim());
                } else if let Some(data) = line.strip_prefix("data:") {
                    data_lines.push(data.trim());
                }
            }
            if name.is_none() && data_lines.is_empty() {
                continue;
            }
            let data_str = data_lines.join("\n");
            let parsed: Value = serde_json::from_str(&data_str).unwrap_or(Value::Null);
            for event in event_from_parts(name.unwrap_or("message_chunk"), &parsed) {
                events.push(event);
            }
        }
        events
    }
}

/// `event:` 名 + `data:` JSON → 事件列表（认不得的忽略，与参考实现一致）。
fn event_from_parts(name: &str, data: &Value) -> Vec<TabbitEvent> {
    match name {
        "message_chunk" => {
            let mut events = Vec::new();
            let reasoning = data
                .get("reasoning_content")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !reasoning.is_empty() {
                events.push(TabbitEvent::Reasoning(reasoning.to_string()));
            }
            let content = data.get("content").and_then(Value::as_str).unwrap_or("");
            if !content.is_empty() {
                events.push(TabbitEvent::Delta(content.to_string()));
            }
            events
        }
        "finish" | "close" | "cancel" => vec![TabbitEvent::Done],
        "error" => {
            let message = data
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("上游错误")
                .to_string();
            let code = data.get("code").and_then(Value::as_i64);
            vec![TabbitEvent::Error { message, code }]
        }
        // ready / title / message_start / message_finish / thinking /
        // thinking_finished / switch_model / usage / ttft：元信息，不产出内容
        _ => Vec::new(),
    }
}

// ─── OpenAI chunk 产出 ───────────────────────────────────────

/// 一帧出站内容。
enum Frame {
    Chunk(Value),
    Done,
}

impl Frame {
    /// 落到线上的字节（含 `\n\n` 帧尾）。
    fn wire(&self) -> String {
        match self {
            Frame::Chunk(chunk) => format!("data: {chunk}\n\n"),
            Frame::Done => "data: [DONE]\n\n".to_string(),
        }
    }
}

/// 流式转换器：喂上游字节，产出 OpenAI chunk。
struct TabbitStream {
    id: String,
    created: i64,
    model: String,
    scanner: SseScanner,
    saw_end: bool,
    content: String,
    req_chars: usize,
    /// 是否已产出过内容帧（409 重试只在未产出时安全，见 `run_chat_with_busy_retry`）
    produced: bool,
}

impl TabbitStream {
    fn new(model: &str, req_chars: usize) -> Self {
        Self {
            id: format!(
                "chatcmpl-{}",
                crate::server::logging::now_ms() % 1_000_000_000_000_000
            ),
            created: crate::server::logging::now_ms() / 1000,
            model: model.to_string(),
            scanner: SseScanner::default(),
            saw_end: false,
            content: String::new(),
            req_chars,
            produced: false,
        }
    }

    /// 喂一段字节，返回该段触发的帧；上游错误事件 → Err（调用方按分类处理）。
    fn feed(&mut self, text: &str) -> Result<Vec<Frame>, GatewayError> {
        if self.saw_end {
            return Ok(Vec::new());
        }
        let mut frames = Vec::new();
        for event in self.scanner.push(text) {
            match event {
                TabbitEvent::Delta(delta) => {
                    self.content.push_str(&delta);
                    self.produced = true;
                    frames.push(self.chunk(Some(delta), None, None));
                }
                TabbitEvent::Reasoning(reasoning) => {
                    self.produced = true;
                    frames.push(self.chunk(None, Some(reasoning), None));
                }
                TabbitEvent::Done => {
                    self.saw_end = true;
                    frames.push(self.chunk(None, None, Some("stop")));
                    frames.push(Frame::Done);
                }
                TabbitEvent::Error { message, code } => {
                    self.saw_end = true;
                    // 409 会话忙：未产出内容时由外层等待重试（可重试标记）；
                    // 已产出内容后中断则如实报错（避免半截回答被吞后重发）
                    if protocol::is_busy_error(&message) && !self.produced {
                        return Err(busy_retry_error(&message));
                    }
                    let explained =
                        protocol::explain_error(&message, code, self.req_chars);
                    crate::server::logging::verbose(
                        "[Tabbit]",
                        &format!("上游 error 事件（code={code:?}）：{explained}"),
                    );
                    return Err(GatewayError::with_status(
                        code.unwrap_or(502) as i32,
                        explained,
                    ));
                }
            }
        }
        Ok(frames)
    }

    /// 流结束（EOF）：上游没给过结束帧时补 `[DONE]`。
    fn finish(&mut self) -> Vec<Frame> {
        if self.saw_end {
            return Vec::new();
        }
        self.saw_end = true;
        vec![Frame::Done]
    }

    fn chunk(
        &self,
        content: Option<String>,
        reasoning: Option<String>,
        finish_reason: Option<&str>,
    ) -> Frame {
        let mut delta = Map::new();
        if let Some(text) = content {
            if !text.is_empty() {
                delta.insert("content".to_string(), json!(text));
            }
        }
        if let Some(text) = reasoning {
            if !text.is_empty() {
                delta.insert("reasoning_content".to_string(), json!(text));
            }
        }
        let mut choice = Map::new();
        choice.insert("index".to_string(), json!(0));
        choice.insert("delta".to_string(), Value::Object(delta));
        if let Some(reason) = finish_reason {
            choice.insert("finish_reason".to_string(), json!(reason));
        } else {
            choice.insert("finish_reason".to_string(), Value::Null);
        }
        let mut chunk = Map::new();
        chunk.insert("id".to_string(), json!(self.id));
        chunk.insert("object".to_string(), json!("chat.completion.chunk"));
        chunk.insert("created".to_string(), json!(self.created));
        chunk.insert("model".to_string(), json!(self.model));
        chunk.insert("choices".to_string(), Value::Array(vec![Value::Object(choice)]));
        chunk.insert("system_fingerprint".to_string(), json!("tabbit"));
        Frame::Chunk(Value::Object(chunk))
    }
}

/// 流式驱动：读上游字节 → 翻译帧 → 发 mpsc。
async fn drive_stream(
    response: reqwest::Response,
    model: &str,
    req_chars: usize,
    sender: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), GatewayError> {
    let mut converter = TabbitStream::new(model, req_chars);
    let mut byte_stream = response.bytes_stream();
    // 会话忙重试（409）：未产出内容时等待后由调用方重建会话重试 ——
    // 这里只处理流内 error；建会话后的首次 409 由外层 run_chat 循环处理
    while let Some(item) = byte_stream.next().await {
        let bytes = item.map_err(|error| {
            GatewayError::with_status(502, format!("Tabbit 上游流读取失败：{error}"))
        })?;
        let text = String::from_utf8_lossy(&bytes);
        let frames = converter.feed(&text)?;
        for frame in frames {
            let wire = frame.wire();
            if sender.send(Ok(Bytes::from(wire))).await.is_err() {
                return Ok(());
            }
        }
    }
    for frame in converter.finish() {
        let wire = frame.wire();
        let _ = sender.send(Ok(Bytes::from(wire))).await;
    }
    let _ = sender.send(Ok(Bytes::from("")));
    Ok(())
}

/// 非流式：遍历同样的上游流，聚合为完整 JSON。
async fn aggregate(
    response: reqwest::Response,
    model: &str,
    prompt: &str,
) -> Result<ForwardOutcome, GatewayError> {
    let mut converter = TabbitStream::new(model, prompt.chars().count());
    let mut byte_stream = response.bytes_stream();
    let mut reasoning = String::new();
    while let Some(item) = byte_stream.next().await {
        let bytes = item.map_err(|error| {
            GatewayError::with_status(502, format!("Tabbit 上游流读取失败：{error}"))
        })?;
        let text = String::from_utf8_lossy(&bytes);
        // 复用 feed 的事件解析，但只收内容（reasoning 单独记）
        for event in converter.scanner.push(&text) {
            match event {
                TabbitEvent::Delta(delta) => {
                    converter.content.push_str(&delta);
                    converter.produced = true;
                }
                TabbitEvent::Reasoning(text) => {
                    reasoning.push_str(&text);
                    converter.produced = true;
                }
                TabbitEvent::Done => converter.saw_end = true,
                TabbitEvent::Error { message, code } => {
                    if protocol::is_busy_error(&message) && !converter.produced {
                        return Err(busy_retry_error(&message));
                    }
                    let explained =
                        protocol::explain_error(&message, code, prompt.chars().count());
                    return Err(GatewayError::with_status(
                        code.unwrap_or(502) as i32,
                        explained,
                    ));
                }
            }
        }
    }
    let mut message = Map::new();
    message.insert("role".to_string(), json!("assistant"));
    message.insert("content".to_string(), json!(converter.content));
    if !reasoning.is_empty() {
        message.insert("reasoning_content".to_string(), json!(reasoning));
    }
    let mut choice = Map::new();
    choice.insert("index".to_string(), json!(0));
    choice.insert("message".to_string(), Value::Object(message));
    choice.insert("finish_reason".to_string(), json!("stop"));
    let mut body = Map::new();
    body.insert("id".to_string(), json!(converter.id));
    body.insert("object".to_string(), json!("chat.completion"));
    body.insert("created".to_string(), json!(converter.created));
    body.insert("model".to_string(), json!(model));
    body.insert("choices".to_string(), Value::Array(vec![Value::Object(choice)]));
    body.insert("system_fingerprint".to_string(), json!("tabbit"));
    // 上游 SSE 不返回 usage（参考实现本地估算）：这里给字符数，调用方按需换算
    body.insert(
        "usage".to_string(),
        json!({
            "prompt_tokens": prompt.chars().count(),
            "completion_tokens": converter.content.chars().count(),
            "total_tokens": prompt.chars().count() + converter.content.chars().count(),
        }),
    );
    Ok(ForwardOutcome::Completion { body: Value::Object(body) })
}

/// 可重试的「会话忙」标记错误（409 且未产出内容）。
///
/// 用 message 前缀做标记（不另建错误类型）：`run_chat_with_busy_retry` 认它。
fn busy_retry_error(message: &str) -> GatewayError {
    GatewayError::with_status(409, format!("TABBIT_BUSY_RETRY:{message}"))
}

/// 是否为可重试的会话忙错误（`busy_retry_error` 的标记）。
fn is_busy_retry(error: &GatewayError) -> bool {
    error.status_code == 409 && error.message.starts_with("TABBIT_BUSY_RETRY:")
}

/// 会话忙（409）时等待后重试的包装：`run_chat` 的建会话→对话整体重试。
///
/// 只在**未产出任何内容**时重试（`busy_retry_error` 标记）；已产出内容后
/// 中断则如实报错（避免半截回答被吞掉后重发）。参考 `_is_busy_error` +
/// `BUSY_RETRY`。
pub async fn run_chat_with_busy_retry(
    region: Region,
    store: &AccountStore,
    account_id: &str,
    body: &Value,
    proxy: Option<ResolvedProxy>,
    stream: bool,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<ForwardOutcome, GatewayError> {
    let mut last_error: Option<GatewayError> = None;
    for _attempt in 0..=BUSY_RETRY_MAX {
        match run_chat(region, store, account_id, body, proxy.clone(), stream, telemetry).await {
            Ok(outcome) => return Ok(outcome),
            Err(error) => {
                if is_busy_retry(&error) {
                    let detail = error.message.trim_start_matches("TABBIT_BUSY_RETRY:");
                    crate::server::logging::verbose(
                        "[Tabbit]",
                        &format!("会话处理中（409），{BUSY_WAIT_MS}ms 后重试：{detail}"),
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(BUSY_WAIT_MS)).await;
                    last_error = Some(GatewayError::with_status(409, detail.to_string()));
                    continue;
                }
                return Err(error);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| GatewayError::with_status(502, "Tabbit 会话持续忙碌，重试已耗尽")))
}
