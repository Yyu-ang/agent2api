//! TeleAgent SSE 事件流解析：`/global/event` → OpenAI chat SSE。
//!
//! ── 移植自 ────────────────────────────────────────────────────
//! `teleagent-openai-proxy/openai_proxy.py` 的 `SSEListener`
//!（MIT，原作者 lizhun781021），事件语义逐字对应：
//!   - 只处理 `properties.sessionID == 本次会话` 的事件（全局流是多会话共享的）；
//!   - `message.updated`：登记 assistant 消息 id；`info.finish == "error"`
//!     或 `info.error` → 错误；
//!   - `message.part.updated`：assistant 文本 part 的增量收集；
//!     `step-finish` 带 `tokens`（记账用）；`part.error` → 错误；
//!   - `message.part.delta`：`field == "text"` 的增量 —— part 属于已登记的
//!     assistant 文本 part 则为正文，否则为推理过程（原实现
//!     `reasoning_chunks`；本移植只下发正文，与 kuku 同口径）；
//!   - `session.status`：`status.type == "idle"` → 本轮生成结束；
//!   - `permission.asked` / `question.asked`：上游在等**用户确认**
//!     （授权访问目录/执行命令、或向用户提问）。
//!
//! ── 待确认事件 ─────────────────────────────────────────────
//! 当前会话的 permission/question 事件写入受保护的人工确认队列，后台监听
//! 持续等待用户在管理页/API 回复后继续执行，绝不自动授权；10 分钟超时兜底。
//!
//! ── 与 Python 实现的两处传输层差异 ────────────────────────────
//!   - Python 用裸 socket 手写 HTTP（要自己剥 chunked 编码）；这里走
//!     reqwest，chunked 由传输层解掉，只需按 `data:` 行切帧；
//!   - 十六进制 chunk 长度行在 reqwest 层已不存在，无需跳过。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Map, Value, json};
use tokio_stream::wrappers::ReceiverStream;

use crate::server::core::egress;
use crate::server::errors::GatewayError;

use super::auth::{base_http_url, sign_headers, sign_headers_with_key};

/// 事件流监听超时（原实现 `SSEListener` 默认 120 秒）
const LISTEN_TIMEOUT_MS: u64 = 10 * 60_000;
/// 流空闲兜底（与 kuku 的 `STREAM_IDLE_MS` 同值；编排层另有统一 idle_guard）
const STREAM_IDLE_MS: u64 = 10 * 60_000;

/// 一条已解析的上游事件。
enum TeleAgentEvent {
    /// 正文增量
    Delta(String),
    /// 本轮生成结束（`session.status` → idle）
    Done,
    /// 上游错误（message/part 的 error）
    Error(String),
    /// 上游在等待用户确认（网关无法代答，中断本轮）
    NeedConfirmation { kind: &'static str, properties: Value },
}

/// 按 `\n\n` 分帧、逐帧取 `data:` 前缀 JSON 的解析器（与 kuku 的
/// `SseScanner` 同构；super-agent 的帧也是标准 SSE）。
#[derive(Default)]
struct SseScanner {
    buffer: Vec<u8>,
    /// 已登记的 assistant 消息 id（`message.updated`）
    assistant_msg_ids: Vec<String>,
    /// 已登记的 assistant 文本 part id（`message.part.updated`）
    assistant_text_part_ids: Vec<String>,
    /// 本次会话 id（过滤条件）
    session_id: String,
}

impl SseScanner {
    fn new(session_id: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            ..Self::default()
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Vec<TeleAgentEvent> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        // 完整 SSE 帧到齐后才解码，避免 UTF-8 汉字跨网络 chunk 损坏。
        while let Some((at, width)) = {
            let crlf = self.buffer.windows(4).position(|w| w == b"\r\n\r\n").map(|i| (i, 4));
            let lf = self.buffer.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
            match (crlf, lf) {
                (Some(a), Some(b)) => Some(if a.0 < b.0 { a } else { b }),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                _ => None,
            }
        } {
            let raw: Vec<u8> = self.buffer.drain(..at + width).collect();
            let Ok(text) = String::from_utf8(raw) else { continue };
            let mut payload_lines = Vec::new();
            for line in text.lines() {
                if let Some(payload) = line.trim_end_matches('\r').strip_prefix("data:") {
                    payload_lines.push(payload.trim_start());
                }
            }
            let payload = payload_lines.join("\n");
            if payload.is_empty() || payload == "[DONE]" { continue; }
            let Ok(parsed) = serde_json::from_str::<Value>(&payload) else { continue };
            if let Some(event) = self.event_from_json(&parsed) { events.push(event); }
        }
        events
    }

    /// 单个事件 JSON → 上游事件（认不得的 type 忽略，与原实现一致）。
    fn event_from_json(&mut self, event: &Value) -> Option<TeleAgentEvent> {
        let payload = event.get("payload")?;
        let etype = payload.get("type").and_then(Value::as_str).unwrap_or("");
        let props = payload.get("properties")?;
        // 只关注本次会话的事件（原实现 `_handle_event` 开头的过滤）
        let event_session = props.get("sessionID").and_then(Value::as_str).unwrap_or("");
        if !event_session.is_empty() && event_session != self.session_id {
            return None;
        }
        // 确认/结束事件是会话级控制信号：缺少归属也不可当成本会话事件。
        // 全局 SSE 同时服务多个 TeleAgent 任务，不能跨会话关联审批。
        if matches!(etype, "permission.asked" | "question.asked" | "session.status")
            && event_session != self.session_id
        {
            return None;
        }
        match etype {
            "message.updated" => {
                let info = props.get("info")?;
                let role = info.get("role").and_then(Value::as_str).unwrap_or("");
                let msg_id = info.get("id").and_then(Value::as_str).unwrap_or("");
                if role == "assistant" && !msg_id.is_empty()
                    && !self.assistant_msg_ids.iter().any(|id| id == msg_id)
                {
                    self.assistant_msg_ids.push(msg_id.to_string());
                }
                if info.get("finish").and_then(Value::as_str) == Some("error")
                    || info.get("error").is_some_and(|error| !error.is_null())
                {
                    let message = info
                        .get("error")
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("上游生成出错")
                        .to_string();
                    return Some(TeleAgentEvent::Error(message));
                }
                None
            }
            "message.part.updated" => {
                let part = props.get("part")?;
                let part_id = part.get("id").and_then(Value::as_str).unwrap_or("");
                let part_type = part.get("type").and_then(Value::as_str).unwrap_or("");
                let text = part.get("text").and_then(Value::as_str).unwrap_or("");
                let msg_id = part
                    .get("messageID")
                    .and_then(Value::as_str)
                    .or_else(|| props.get("messageID").and_then(Value::as_str))
                    .unwrap_or("");
                // 只跟踪 assistant 消息的 text part（原实现口径）
                if part_type == "text" && !text.is_empty()
                    && self.assistant_msg_ids.iter().any(|id| id == msg_id)
                    && !self.assistant_text_part_ids.iter().any(|id| id == part_id)
                {
                    self.assistant_text_part_ids.push(part_id.to_string());
                    return Some(TeleAgentEvent::Delta(text.to_string()));
                }
                if part_type == "step-finish" {
                    // tokens 在这里（原实现记 `self.tokens`；本移植暂不做
                    // token 记账 —— 上游 tokens 字典形态未在参考里固定，
                    // 强行解析不如不记，见 `chat.rs` 的说明）
                }
                if part.get("error").is_some_and(|error| !error.is_null()) {
                    let message = part
                        .get("error")
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("上游生成出错")
                        .to_string();
                    return Some(TeleAgentEvent::Error(message));
                }
                None
            }
            "message.part.delta" => {
                let field = props.get("field").and_then(Value::as_str).unwrap_or("");
                let delta = props.get("delta").and_then(Value::as_str).unwrap_or("");
                if field == "text" && !delta.is_empty() {
                    let part_id = props.get("partID").and_then(Value::as_str).unwrap_or("");
                    // 登记过的 assistant 文本 part → 正文；否则是推理过程
                    // （原实现 `reasoning_chunks`；本移植不下发，与 kuku 同口径）
                    if self.assistant_text_part_ids.iter().any(|id| id == part_id) {
                        return Some(TeleAgentEvent::Delta(delta.to_string()));
                    }
                }
                None
            }
            "session.status" => {
                let status_type = props
                    .get("status")
                    .and_then(|status| status.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if status_type == "idle" {
                    return Some(TeleAgentEvent::Done);
                }
                None
            }
            "permission.asked" | "question.asked" => {
                let kind = if etype == "permission.asked" { "permission" } else { "question" };
                Some(TeleAgentEvent::NeedConfirmation { kind, properties: props.clone() })
            }
            _ => None,
        }
    }
}

// ─── OpenAI chunk 产出（与 kuku 的 `KukuStream` 同构） ──────────

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
struct TeleAgentStream {
    id: String,
    created: i64,
    model: String,
    scanner: SseScanner,
    saw_end: bool,
    /// 非流式聚合用（也用于判断「发过内容就不再改判错误」）
    content: String,
    tools_enabled: bool,
    key: String,
}

impl TeleAgentStream {
    fn new(model: &str, session_id: &str, tools_enabled: bool, key: &str) -> Self {
        Self {
            id: format!("chatcmpl-tg{}", crate::server::logging::now_ms()),
            created: crate::server::logging::now_ms() / 1000,
            model: model.to_string(),
            scanner: SseScanner::new(session_id),
            saw_end: false,
            content: String::new(),
            tools_enabled,
            key: key.to_string(),
        }
    }

    /// 喂一段字节，返回该段触发的帧；`Err` = 中断本轮（上游错误/待确认）。
    fn feed(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, GatewayError> {
        if self.saw_end {
            return Ok(Vec::new());
        }
        let mut frames = Vec::new();
        for event in self.scanner.push(bytes) {
            match event {
                TeleAgentEvent::Delta(delta) => {
                    self.content.push_str(&delta);
                    // 工具请求模式先缓冲，解析完成后再输出，避免泄漏伪协议标记。
                    if !self.tools_enabled { frames.push(self.chunk(delta, None)); }
                }
                TeleAgentEvent::Done => {
                    // 等待人工确认时上游可能暂时进入 idle，不能提前关闭回答流。
                    if super::confirmations::has_pending(&self.scanner.session_id) { continue; }
                    self.saw_end = true;
                    let mut finish = "stop";
                    if self.tools_enabled {
                        let (clean, calls) = parse_tool_calls(&self.content);
                        if !clean.is_empty() { frames.push(self.chunk(clean, None)); }
                        if !calls.is_empty() {
                            finish = "tool_calls";
                            frames.push(self.tool_chunk(calls));
                        }
                    }
                    frames.push(self.chunk(String::new(), Some(finish)));
                    frames.push(Frame::Done);
                }
                TeleAgentEvent::Error(message) => {
                    crate::server::logging::verbose("[TeleAgent]", &format!("上游错误事件：{message}"));
                    // 已发过内容：保留已发、正常收尾（与 kuku 的 ERROR 口径一致）；
                    // 没发过内容：中断并报错（客户端能看到原因）
                    self.saw_end = true;
                    if self.content.is_empty() {
                        return Err(GatewayError::with_status(502, format!("TeleAgent 上游错误：{message}")));
                    }
                    frames.push(self.chunk(String::new(), Some("stop")));
                    frames.push(Frame::Done);
                }
                TeleAgentEvent::NeedConfirmation { kind, properties } => {
                    super::confirmations::record(
                        kind, &self.scanner.session_id, &properties, &self.key,
                    )?;
                    crate::server::logging::log(
                        "[TeleAgent]", "上游等待人工确认，可通过 /api/teleagent/confirmations 处理",
                    );
                    // 不批准也不中断原会话；继续监听直到用户在受保护的管理 API 里回复。
                }
            }
        }
        Ok(frames)
    }

    /// 流结束（EOF）：上游没给过结束帧时补 `[DONE]`，保证客户端能收尾。
    fn finish(&mut self) -> Vec<Frame> {
        if self.saw_end {
            return Vec::new();
        }
        self.saw_end = true;
        vec![self.chunk(String::new(), Some("stop")), Frame::Done]
    }

    fn tool_chunk(&self, calls: Vec<Value>) -> Frame {
        // OpenAI Chat Completions 的流式 tool_calls 每项都需要 index；
        // 非流式 message.tool_calls 则保持普通 id/type/function 结构。
        let indexed = calls.into_iter().enumerate().map(|(index, mut call)| {
            if let Value::Object(ref mut object) = call {
                object.insert("index".to_string(), json!(index));
            }
            call
        }).collect::<Vec<_>>();
        Frame::Chunk(json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{"index": 0, "delta": {"tool_calls": indexed}, "finish_reason": null}]
        }))
    }

    fn chunk(&mut self, delta: String, finish_reason: Option<&str>) -> Frame {
        let mut choice = Map::new();
        choice.insert("index".to_string(), json!(0));
        if delta.is_empty() {
            choice.insert("delta".to_string(), json!({}));
        } else {
            choice.insert("delta".to_string(), json!({ "content": delta }));
        }
        if let Some(reason) = finish_reason {
            choice.insert("finish_reason".to_string(), json!(reason));
        }
        let mut chunk = Map::new();
        chunk.insert("id".to_string(), json!(self.id));
        chunk.insert("object".to_string(), json!("chat.completion.chunk"));
        chunk.insert("created".to_string(), json!(self.created));
        chunk.insert("model".to_string(), json!(self.model));
        chunk.insert("choices".to_string(), Value::Array(vec![Value::Object(choice)]));
        Frame::Chunk(Value::Object(chunk))
    }
}

/// 打开 `/global/event` 的签名 SSE 连接（原实现 `SSEListener._make_request`
/// + `listen` 的建连部分；chunked 由 reqwest 解）。
pub async fn open_event_stream() -> Result<reqwest::Response, GatewayError> {
    let path = "/global/event";
    let headers = sign_headers("GET", path)?;
    let url = format!("{}{path}", base_http_url());
    // 直连本机回环（见 `mod.rs`「出网」）
    let client = egress::client_for(None);
    let mut builder = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .timeout(std::time::Duration::from_millis(LISTEN_TIMEOUT_MS));
    for (name, value) in &headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let response = builder.send().await.map_err(|error| {
        if error.is_timeout() {
            GatewayError::with_status(504, "TeleAgent 事件流连接超时，请稍后重试")
        } else {
            GatewayError::with_status(
                502,
                format!(
                    "TeleAgent 事件流连接失败：{}",
                    egress::describe_error_detail(&error)
                ),
            )
        }
    })?;
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 事件流返回 HTTP {status}"),
        ));
    }
    Ok(response)
}

/// 打开 `/global/event` 的签名 SSE 连接（显式 key 版）。
///
/// 转发时用账号里存的 key 建连；key 无效时上游会直接拒绝建连（401/403），
/// 调用方按认证失败处理（提示重新添加账号）。
pub async fn open_event_stream_with_key(key: &str) -> Result<reqwest::Response, GatewayError> {
    let path = "/global/event";
    let headers = sign_headers_with_key("GET", path, key)?;
    let url = format!("{}{path}", base_http_url());
    // 直连本机回环（见 `mod.rs`「出网」）
    let client = egress::client_for(None);
    let mut builder = client
        .get(&url)
        .header("Accept", "text/event-stream")
        .timeout(std::time::Duration::from_millis(LISTEN_TIMEOUT_MS));
    for (name, value) in &headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    let response = builder.send().await.map_err(|error| {
        if error.is_timeout() {
            GatewayError::with_status(504, "TeleAgent 事件流连接超时，请稍后重试")
        } else {
            GatewayError::with_status(
                502,
                format!(
                    "TeleAgent 事件流连接失败：{}",
                    egress::describe_error_detail(&error)
                ),
            )
        }
    })?;
    let status = response.status().as_u16();
    if status == 401 || status == 403 {
        return Err(GatewayError::with_status(
            401,
            "TeleAgent 认证失败（存的 session key 已失效，请重新添加该账号）",
        ));
    }
    if !(200..300).contains(&status) {
        return Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 事件流返回 HTTP {status}"),
        ));
    }
    Ok(response)
}

// ─── 流式 / 非流式驱动（与 kuku 的 `drive_stream` / `aggregate` 同构） ─

/// 流式：遍历上游事件流，把 OpenAI 帧写进 mpsc。
pub async fn drive_stream(
    response: reqwest::Response,
    model: &str,
    session_id: &str,
    tools_enabled: bool,
    key: &str,
    sender: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<(), GatewayError> {
    let source = response
        .bytes_stream()
        .map(|item| item.map_err(std::io::Error::other));
    let mut source = crate::server::core::upstream::stall::idle_guard(
        Box::pin(source),
        std::time::Duration::from_millis(STREAM_IDLE_MS),
    );
    let mut stream = TeleAgentStream::new(model, session_id, tools_enabled, key);
    while let Some(item) = source.next().await {
        let chunk = item.map_err(|error| {
            GatewayError::with_status(502, format!("TeleAgent 上游流式传输中断: {error}"))
        })?;
        for frame in stream.feed(&chunk)? {
            send_frame(sender, &frame).await?;
        }
    }
    if !stream.saw_end {
        return Err(GatewayError::with_status(502, "TeleAgent 上游流提前结束，缺少 session.status=idle"));
    }
    Ok(())
}

async fn send_frame(
    sender: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
    frame: &Frame,
) -> Result<(), GatewayError> {
    sender
        .send(Ok(Bytes::from(frame.wire())))
        .await
        .map_err(|error| GatewayError::with_status(502, format!("TeleAgent 对话流下发失败：{error}")))
}

/// 非流式：遍历同样的上游流，聚合为完整 JSON。
pub async fn aggregate(
    response: reqwest::Response,
    client_model: &str,
    session_id: &str,
    tools_enabled: bool,
    key: &str,
) -> Result<crate::server::core::upstream::ForwardOutcome, GatewayError> {
    let source = response
        .bytes_stream()
        .map(|item| item.map_err(std::io::Error::other));
    let mut source = crate::server::core::upstream::stall::idle_guard(
        Box::pin(source),
        std::time::Duration::from_millis(STREAM_IDLE_MS),
    );
    let mut stream = TeleAgentStream::new(client_model, session_id, tools_enabled, key);
    while let Some(item) = source.next().await {
        let chunk = item.map_err(|error| {
            GatewayError::with_status(502, format!("TeleAgent 上游流式传输中断: {error}"))
        })?;
        let _ = stream.feed(&chunk)?;
    }
    if !stream.saw_end {
        return Err(GatewayError::with_status(502, "TeleAgent 上游流提前结束，缺少 session.status=idle"));
    }
    let (content, tool_calls) = if tools_enabled {
        parse_tool_calls(&stream.content)
    } else {
        (stream.content.clone(), Vec::new())
    };
    let id = stream.id.clone();
    let created = stream.created;
    let model = stream.model.clone();
    let mut message = Map::new();
    message.insert("role".to_string(), json!("assistant"));
    message.insert("content".to_string(), if content.is_empty() && !tool_calls.is_empty() { Value::Null } else { json!(content) });
    if !tool_calls.is_empty() { message.insert("tool_calls".to_string(), json!(tool_calls)); }
    let mut choice = Map::new();
    choice.insert("index".to_string(), json!(0));
    choice.insert("message".to_string(), Value::Object(message));
    choice.insert("finish_reason".to_string(), json!(if tool_calls.is_empty() { "stop" } else { "tool_calls" }));
    let mut body = Map::new();
    body.insert("id".to_string(), json!(id));
    body.insert("object".to_string(), json!("chat.completion"));
    body.insert("created".to_string(), json!(created));
    body.insert("model".to_string(), json!(model));
    body.insert("choices".to_string(), Value::Array(vec![Value::Object(choice)]));
    Ok(crate::server::core::upstream::ForwardOutcome::Completion {
        body: Value::Object(body),
    })
}

/// 流式出口：后台任务喂 mpsc，主路径返回 `ReceiverStream`（与 kuku 同结构）。
pub fn stream_outcome(
    response: reqwest::Response,
    model: &str,
    session_id: &str,
    tools_enabled: bool,
    key: &str,
    conversation: Option<tokio::sync::OwnedMutexGuard<super::session::Conversation>>,
    fingerprints: Vec<String>,
) -> crate::server::core::upstream::ForwardOutcome {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    let model = model.to_string();
    let session_id = session_id.to_string();
    let key = key.to_string();
    crate::spawn_task(async move {
        let mut conversation = conversation;
        let outcome = drive_stream(response, &model, &session_id, tools_enabled, &key, &sender).await;
        if let Some(state) = conversation.as_mut() {
            if outcome.is_ok() { state.commit(session_id.clone(), fingerprints); }
            else { state.reset(); }
        }
        if let Err(error) = outcome {
            crate::server::logging::verbose("[TeleAgent]", &format!("对话流处理中断：{}", error.message));
            let _ = sender.send(Err(std::io::Error::other(error.message))).await;
        }
    });
    crate::server::core::upstream::ForwardOutcome::Stream {
        status: 200,
        stream: Box::new(ReceiverStream::new(receiver)),
    }
}


/// 将 TeleAgent 文本工具协议转为 OpenAI tool_calls。
/// 解析完整的 JSON 标记片段，支持嵌套 arguments 对象。
fn parse_tool_calls(text: &str) -> (String, Vec<Value>) {
    const START: &str = "[TOOL_CALL]";
    const END: &str = "[/TOOL_CALL]";
    let mut rest = text;
    let mut clean = String::new();
    let mut calls = Vec::new();
    while let Some(start) = rest.find(START) {
        clean.push_str(&rest[..start]);
        let tail = &rest[start + START.len()..];
        let Some(end) = tail.find(END) else {
            clean.push_str(&rest[start..]);
            return (clean.trim().to_string(), calls);
        };
        let fragment = tail[..end].trim();
        if let Ok(call) = serde_json::from_str::<Value>(fragment) {
            if let Some(name) = call.get("name").and_then(Value::as_str).filter(|s| !s.trim().is_empty()) {
                let args = call.get("arguments").cloned().unwrap_or_else(|| json!({}));
                let args_text = match args { Value::String(s) => s, other => other.to_string() };
                calls.push(json!({
                    "id": format!("call_{}", calls.len()),
                    "type": "function",
                    "function": {"name": name, "arguments": args_text}
                }));
            } else { clean.push_str(&rest[start..start + START.len() + end + END.len()]); }
        } else { clean.push_str(&rest[start..start + START.len() + end + END.len()]); }
        rest = &tail[end + END.len()..];
    }
    clean.push_str(rest);
    (clean.trim().to_string(), calls)
}

#[cfg(test)]
mod parity_tests {
    use super::parse_tool_calls;
    #[test]
    fn nested_tool_arguments_and_plain_answer() {
        let (clean, calls) = parse_tool_calls(r#"before [TOOL_CALL]{"name":"run","arguments":{"payload":{"x":1}}}[/TOOL_CALL] after"#);
        assert_eq!(clean, "before  after");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["function"]["name"], "run");
        assert_eq!(calls[0]["function"]["arguments"], r#"{"payload":{"x":1}}"#);
        let (text, calls) = parse_tool_calls("normal answer");
        assert_eq!(text, "normal answer");
        assert!(calls.is_empty());
    }

    #[test]
    fn undecodable_tool_call_is_not_erased() {
        let (text, calls) = parse_tool_calls("[TOOL_CALL]oops[/TOOL_CALL]");
        assert_eq!(text, "[TOOL_CALL]oops[/TOOL_CALL]");
        assert!(calls.is_empty());
    }

    #[test]
    fn stream_tool_calls_include_index() {
        let stream=super::TeleAgentStream::new("NewApi/chat-pro","test-session",true,"unit-test-key");
        let (_, calls)=parse_tool_calls(r#"[TOOL_CALL]{"name":"search","arguments":{"q":"a"}}[/TOOL_CALL]"#);
        let frame=stream.tool_chunk(calls);
        let value=match frame { super::Frame::Chunk(v)=>v, _=>panic!("expected chunk") };
        assert_eq!(value["choices"][0]["delta"]["tool_calls"][0]["index"],0);
    }


    #[test]
    fn confirmation_events_require_exact_session_id() {
        let mut scanner = super::SseScanner::new("mine");
        let foreign = serde_json::json!({"payload":{"type":"permission.asked","properties":{"id":"foreign","sessionID":"other"}}});
        let missing = serde_json::json!({"payload":{"type":"permission.asked","properties":{"id":"missing"}}});
        assert!(scanner.event_from_json(&foreign).is_none());
        assert!(scanner.event_from_json(&missing).is_none());
        let own = serde_json::json!({"payload":{"type":"permission.asked","properties":{"id":"own","sessionID":"mine"}}});
        assert!(matches!(scanner.event_from_json(&own), Some(super::TeleAgentEvent::NeedConfirmation{..})));
    }

}
