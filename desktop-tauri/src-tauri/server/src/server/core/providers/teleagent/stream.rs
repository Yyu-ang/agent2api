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
//!     （授权访问目录/执行命令、或向用户提问）；
//!   - `permission.replied` / `question.replied` / `question.rejected`：确认已被
//!     处理（可能是在 TeleAgent 原生界面里），对应待确认项随即删除。
//!
//! ── 待确认事件 ─────────────────────────────────────────────
//! 默认（未开启人工确认）与旧版一致：立即中断本轮并报 502。开启后写入受保护
//! 的人工确认队列，继续监听直到用户在管理页/API 回复，绝不自动授权；只有
//! **本轮登记**且未处理的项才会推迟 Done；流结束时清掉本会话的全部待确认项。
//!
//! ── 超时与保活 ─────────────────────────────────────────────
//! 事件流请求不设总时长上限（旧实现的 `.timeout()` 是整条请求的总时长）；
//! 建连有单独上限，之后按**空闲**判定：普通情况用设置里的流空闲超时，
//! 等人工确认时放宽到确认队列的 TTL。上游安静超过 15 秒就给客户端发一条
//! SSE 注释（`: keepalive`），避免客户端/反代空闲断开，同时尽早发现客户端
//! 已断开（及时释放会话锁）。等待期间响应 `telemetry.cancel_token()`。
//!
//! ── 工具模式的流式 ─────────────────────────────────────────
//! 开启 tools 时普通文本照常增量下发，只有出现 `[TOOL_CALL]`（或可能是它的
//! 前缀）之后的内容才缓冲到本轮结束再解析成 `tool_calls`。
//!
//! ── token 记账 ─────────────────────────────────────────────
//! `step-finish` part 的 `tokens`（opencode 形态 `{input, output, reasoning,
//! cache:{read}}`）逐步累加后 `report_usage`；拿不到时按字符粗估（带
//! `estimated: true`）。
//!
//! ── 与 Python 实现的两处传输层差异 ────────────────────────────
//!   - Python 用裸 socket 手写 HTTP（要自己剥 chunked 编码）；这里走
//!     reqwest，chunked 由传输层解掉，只需按 `data:` 行切帧；
//!   - 十六进制 chunk 长度行在 reqwest 层已不存在，无需跳过。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use serde_json::{Map, Value, json};
use tokio_stream::wrappers::ReceiverStream;

use crate::server::core::egress;
use crate::server::core::providers::stateful_session::{self, Turn};
use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::errors::GatewayError;

use super::auth::{base_http_url, sign_headers, sign_headers_with_key};

/// 事件流建连（拿到响应头）上限；之后不设总时长，按空闲判定。
const CONNECT_TIMEOUT_MS: u64 = 30_000;
/// 上游安静多久给客户端发一次 SSE 注释保活
const KEEPALIVE_MS: u64 = 15_000;
/// 等人工确认时的空闲上限（与确认队列 TTL 一致）
const CONFIRM_WAIT_MS: u64 = 10 * 60_000;

/// 工具协议的起始标记（流式时它及其后的内容要缓冲）
const TOOL_START: &str = "[TOOL_CALL]";

type FrameSender = tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>;

/// 一条已解析的上游事件。
enum TeleAgentEvent {
    /// 正文增量
    Delta(String),
    /// 本轮生成结束（`session.status` → idle）
    Done,
    /// 上游错误（message/part 的 error）
    Error(String),
    /// 上游在等待用户确认
    NeedConfirmation { kind: &'static str, properties: Value },
    /// 某条确认已被处理（`*.replied` / `question.rejected`）
    Resolved(String),
    /// `step-finish` 的 tokens 字典
    StepTokens(Value),
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
        if matches!(
            etype,
            "permission.asked" | "question.asked" | "session.status"
                | "permission.replied" | "question.replied" | "question.rejected"
        )
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
                if part.get("error").is_some_and(|error| !error.is_null()) {
                    let message = part
                        .get("error")
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("上游生成出错")
                        .to_string();
                    return Some(TeleAgentEvent::Error(message));
                }
                if part_type == "step-finish" {
                    // 形态按 opencode 读（input/output/reasoning/cache.read），
                    // 读不到的字段按 0，整体读不到就退回估算（见 `usage`）。
                    if let Some(tokens) = part.get("tokens").filter(|t| t.is_object()) {
                        return Some(TeleAgentEvent::StepTokens(tokens.clone()));
                    }
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
            "permission.replied" | "question.replied" | "question.rejected" => {
                let id = super::confirmations::event_id(props);
                (!id.is_empty()).then(|| TeleAgentEvent::Resolved(id.to_string()))
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

/// 一轮对话的参数（流式与非流式共用）。
#[derive(Clone)]
pub struct TurnOptions {
    /// 回显给客户端的模型名
    pub model: String,
    pub session_id: String,
    pub tools_enabled: bool,
    pub key: String,
    /// 是否开启人工确认（见 `confirmations::manual_enabled`）
    pub manual: bool,
    /// 本轮 prompt（usage 估算用）
    pub prompt: String,
}

/// `step-finish` 累加出的用量
#[derive(Default)]
struct StepUsage {
    input: i64,
    output: i64,
    cache_read: i64,
}

impl StepUsage {
    fn add(&mut self, tokens: &Value) {
        let read = |value: Option<&Value>| value.and_then(Value::as_i64).unwrap_or(0).max(0);
        self.input += read(tokens.get("input"));
        self.output += read(tokens.get("output")) + read(tokens.get("reasoning"));
        self.cache_read += read(tokens.get("cache").and_then(|c| c.get("read")));
    }
}

/// 流式转换器：喂上游字节，产出 OpenAI chunk。
struct TeleAgentStream {
    id: String,
    created: i64,
    model: String,
    scanner: SseScanner,
    saw_end: bool,
    /// 本轮完整正文（非流式聚合、工具解析、usage 估算用）
    content: String,
    tools_enabled: bool,
    key: String,
    manual: bool,
    /// 本轮登记的待确认 id（只有它们能推迟 Done）
    registered: Vec<String>,
    /// 工具模式：`content` 里已下发到哪个字节位置
    emitted: usize,
    /// 工具模式：已出现 `[TOOL_CALL]`，其后内容缓冲到结束
    holding: bool,
    usage: StepUsage,
    prompt_tokens: i64,
}

impl TeleAgentStream {
    fn new(options: &TurnOptions) -> Self {
        Self {
            id: format!("chatcmpl-tg{}", crate::server::logging::now_ms()),
            created: crate::server::logging::now_ms() / 1000,
            model: options.model.clone(),
            scanner: SseScanner::new(&options.session_id),
            saw_end: false,
            content: String::new(),
            tools_enabled: options.tools_enabled,
            key: options.key.clone(),
            manual: options.manual,
            registered: Vec::new(),
            emitted: 0,
            holding: false,
            usage: StepUsage::default(),
            prompt_tokens: stateful_session::estimate_tokens(&options.prompt),
        }
    }

    /// 本轮是否还在等本轮登记的人工确认。
    fn waiting_confirmation(&self) -> bool {
        self.manual && super::confirmations::has_pending_ids(&self.scanner.session_id, &self.registered)
    }

    /// 本轮 usage：上游 step-finish 有数就用，否则按字符估算。
    fn usage(&self) -> Value {
        if self.usage.input + self.usage.output > 0 {
            return stateful_session::usage_json(self.usage.input, self.usage.output, self.usage.cache_read, false);
        }
        stateful_session::usage_json(self.prompt_tokens, stateful_session::estimate_tokens(&self.content), 0, true)
    }

    /// 工具模式下此刻可以安全下发的文本：`[TOOL_CALL]` 之前的部分，且不含
    /// 末尾可能是标记前缀的几个字符（例如 `"[TOO"`）。
    fn flushable(&mut self) -> Option<String> {
        if self.holding {
            return None;
        }
        let pending = &self.content[self.emitted..];
        let take = match pending.find(TOOL_START) {
            Some(at) => {
                self.holding = true;
                at
            }
            None => {
                let keep = (1..TOOL_START.len())
                    .rev()
                    .find(|&k| pending.ends_with(&TOOL_START[..k]))
                    .unwrap_or(0);
                pending.len() - keep
            }
        };
        let text = pending[..take].to_string();
        self.emitted += take;
        (!text.is_empty()).then_some(text)
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
                    if !self.tools_enabled {
                        frames.push(self.chunk(delta, None));
                    } else if let Some(text) = self.flushable() {
                        // 工具模式：标记之前的普通文本照常流式下发
                        frames.push(self.chunk(text, None));
                    }
                }
                TeleAgentEvent::StepTokens(tokens) => self.usage.add(&tokens),
                TeleAgentEvent::Resolved(id) => {
                    super::confirmations::resolve(&self.scanner.session_id, &id);
                    self.registered.retain(|item| item != &id);
                }
                TeleAgentEvent::Done => {
                    // 等待**本轮登记**的人工确认时上游可能暂时进入 idle，不能提前收尾；
                    // 历史残留项不算（B2）。
                    if self.waiting_confirmation() { continue; }
                    self.saw_end = true;
                    let mut finish = "stop";
                    if self.tools_enabled {
                        let rest = self.content[self.emitted..].to_string();
                        self.emitted = self.content.len();
                        let (clean, calls) = parse_tool_calls_raw(&rest);
                        let clean = if self.holding || clean.trim().is_empty() {
                            clean.trim().to_string()
                        } else {
                            clean
                        };
                        if !clean.is_empty() { frames.push(self.chunk(clean, None)); }
                        if !calls.is_empty() {
                            finish = "tool_calls";
                            frames.push(self.tool_chunk(calls));
                        }
                    }
                    frames.push(self.finish_chunk(finish));
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
                    frames.push(self.finish_chunk("stop"));
                    frames.push(Frame::Done);
                }
                TeleAgentEvent::NeedConfirmation { kind, properties } => {
                    if !self.manual {
                        // 默认与旧版一致：网关场景没有可确认的人，直接中断本轮。
                        self.saw_end = true;
                        let description = properties
                            .get("description")
                            .or_else(|| properties.get("prompt"))
                            .and_then(Value::as_str)
                            .filter(|text| !text.is_empty())
                            .unwrap_or("（无描述）");
                        let label = if kind == "permission" { "权限确认" } else { "问题确认" };
                        return Err(GatewayError::with_status(
                            502,
                            format!("TeleAgent 请求用户{label}（网关无法代答）：{description}"),
                        ));
                    }
                    let id = super::confirmations::record(
                        kind, &self.scanner.session_id, &properties, &self.key,
                    )?;
                    if !self.registered.contains(&id) {
                        self.registered.push(id);
                    }
                    crate::server::logging::log(
                        "[TeleAgent]", "上游等待人工确认，可通过 /api/teleagent/confirmations 处理",
                    );
                    // 不批准也不中断原会话；继续监听直到用户在受保护的管理 API 里回复。
                }
            }
        }
        Ok(frames)
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

    /// 收尾帧：带 finish_reason 与本轮 usage。
    fn finish_chunk(&mut self, finish_reason: &str) -> Frame {
        let mut frame = self.chunk(String::new(), Some(finish_reason));
        if let Frame::Chunk(Value::Object(object)) = &mut frame {
            object.insert("usage".to_string(), self.usage());
        }
        frame
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
        .header("Accept", "text/event-stream");
    for (name, value) in &headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    // 只限制「拿到响应头」的时长；长连接本身不设总时长（空闲判定见 `consume`）。
    let sent = tokio::time::timeout(
        std::time::Duration::from_millis(CONNECT_TIMEOUT_MS),
        builder.send(),
    )
    .await
    .map_err(|_| GatewayError::with_status(504, "TeleAgent 事件流连接超时，请稍后重试"))?;
    let response = sent.map_err(|error| {
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
        .header("Accept", "text/event-stream");
    for (name, value) in &headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    // 只限制「拿到响应头」的时长；长连接本身不设总时长（空闲判定见 `consume`）。
    let sent = tokio::time::timeout(
        std::time::Duration::from_millis(CONNECT_TIMEOUT_MS),
        builder.send(),
    )
    .await
    .map_err(|_| GatewayError::with_status(504, "TeleAgent 事件流连接超时，请稍后重试"))?;
    let response = sent.map_err(|error| {
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

// ─── 流式 / 非流式驱动 ─────────────────────────────────────────

/// 读完一轮上游事件。`sender` 为 Some（流式）时下发帧与保活注释；
/// 空闲超时、手动终止、客户端断开都会返回 Err。
async fn consume(
    response: reqwest::Response,
    stream: &mut TeleAgentStream,
    sender: Option<&FrameSender>,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<(), GatewayError> {
    let mut source = response.bytes_stream();
    let idle_ms = crate::server::config::timeout_settings().stream_idle_ms();
    let cancel = telemetry.cancel_token();
    let mut last_activity = std::time::Instant::now();
    loop {
        let next = tokio::time::timeout(std::time::Duration::from_millis(KEEPALIVE_MS), source.next());
        let item = match &cancel {
            Some(token) => tokio::select! {
                item = next => item,
                _ = token.cancelled() => {
                    return Err(crate::server::core::upstream::cancellation::cancelled_error());
                }
            },
            None => next.await,
        };
        match item {
            Ok(Some(Ok(chunk))) => {
                last_activity = std::time::Instant::now();
                let frames = stream.feed(&chunk)?;
                if let Some(sender) = sender {
                    for frame in frames {
                        send_wire(sender, frame.wire()).await?;
                    }
                }
                if stream.saw_end {
                    break;
                }
            }
            Ok(Some(Err(error))) => {
                return Err(GatewayError::with_status(502, format!("TeleAgent 上游流式传输中断: {error}")));
            }
            Ok(None) => break,
            Err(_quiet) => {
                let waiting = stream.waiting_confirmation();
                let limit = if waiting { CONFIRM_WAIT_MS.max(idle_ms) } else { idle_ms };
                if last_activity.elapsed() >= std::time::Duration::from_millis(limit) {
                    let reason = if waiting { "等待人工确认超时" } else { "上游长时间无事件" };
                    return Err(GatewayError::with_status(
                        504,
                        format!("TeleAgent {reason}（{} 秒）", limit / 1000),
                    ));
                }
                if let Some(sender) = sender {
                    let note = if waiting { ": waiting for teleagent confirmation\n\n" } else { ": keepalive\n\n" };
                    send_wire(sender, note.to_string()).await?;
                }
            }
        }
    }
    if !stream.saw_end {
        return Err(GatewayError::with_status(502, "TeleAgent 上游流提前结束，缺少 session.status=idle"));
    }
    Ok(())
}

async fn send_wire(sender: &FrameSender, wire: String) -> Result<(), GatewayError> {
    sender
        .send(Ok(Bytes::from(wire)))
        .await
        .map_err(|_| GatewayError::with_status(499, "TeleAgent 客户端断开连接"))
}

/// 非流式：遍历同样的上游流，聚合为完整 JSON。
pub async fn aggregate(
    response: reqwest::Response,
    options: &TurnOptions,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<crate::server::core::upstream::ForwardOutcome, GatewayError> {
    let mut stream = TeleAgentStream::new(options);
    let outcome = consume(response, &mut stream, None, telemetry).await;
    super::confirmations::clear_session(&options.session_id);
    outcome?;
    let (content, tool_calls) = if options.tools_enabled {
        parse_tool_calls(&stream.content)
    } else {
        (stream.content.clone(), Vec::new())
    };
    let usage = stream.usage();
    telemetry.report_usage(&usage);
    let mut message = Map::new();
    message.insert("role".to_string(), json!("assistant"));
    message.insert("content".to_string(), if content.is_empty() && !tool_calls.is_empty() { Value::Null } else { json!(content) });
    if !tool_calls.is_empty() { message.insert("tool_calls".to_string(), json!(tool_calls)); }
    let mut choice = Map::new();
    choice.insert("index".to_string(), json!(0));
    choice.insert("message".to_string(), Value::Object(message));
    choice.insert("finish_reason".to_string(), json!(if tool_calls.is_empty() { "stop" } else { "tool_calls" }));
    let mut body = Map::new();
    body.insert("id".to_string(), json!(stream.id));
    body.insert("object".to_string(), json!("chat.completion"));
    body.insert("created".to_string(), json!(stream.created));
    body.insert("model".to_string(), json!(stream.model));
    body.insert("choices".to_string(), Value::Array(vec![Value::Object(choice)]));
    body.insert("usage".to_string(), usage);
    Ok(crate::server::core::upstream::ForwardOutcome::Completion {
        body: Value::Object(body),
    })
}

/// 流式出口：后台任务喂 mpsc，主路径返回 `ReceiverStream`（与 kuku 同结构）。
/// `turn` 持有到本轮结束：成功才 commit，否则 drop 时作废会话。
pub fn stream_outcome(
    response: reqwest::Response,
    options: TurnOptions,
    turn: Turn,
    fingerprints: Vec<String>,
    telemetry: Arc<RequestTelemetry>,
) -> crate::server::core::upstream::ForwardOutcome {
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    crate::spawn_task(async move {
        let mut stream = TeleAgentStream::new(&options);
        let outcome = consume(response, &mut stream, Some(&sender), &telemetry).await;
        // 无论成败都清掉本会话的待确认项，残留项会吞掉下一轮的 Done（B2）。
        super::confirmations::clear_session(&options.session_id);
        match outcome {
            Ok(()) => {
                telemetry.report_usage(&stream.usage());
                turn.commit(options.session_id.clone(), fingerprints);
            }
            Err(error) => {
                drop(turn);
                crate::server::logging::verbose("[TeleAgent]", &format!("对话流处理中断：{}", error.message));
                let _ = sender.send(Err(std::io::Error::other(error.message))).await;
            }
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
    let (clean, calls) = parse_tool_calls_raw(text);
    (clean.trim().to_string(), calls)
}

/// 同上，但不裁剪首尾空白（流式剩余部分拼接在已下发文本之后）。
fn parse_tool_calls_raw(text: &str) -> (String, Vec<Value>) {
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
            return (clean, calls);
        };
        let fragment = tail[..end].trim();
        if let Ok(call) = serde_json::from_str::<Value>(fragment) {
            if let Some(name) = call.get("name").and_then(Value::as_str).filter(|s| !s.trim().is_empty()) {
                let args = call.get("arguments").cloned().unwrap_or_else(|| json!({}));
                let args_text = match args { Value::String(s) => s, other => other.to_string() };
                // id 跨轮唯一：客户端靠它把 tool 结果对回调用，`call_0` 每轮重复会串。
                let id = format!("call_{:x}{:02}", crate::server::logging::now_ms(), calls.len());
                calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {"name": name, "arguments": args_text}
                }));
            } else { clean.push_str(&rest[start..start + START.len() + end + END.len()]); }
        } else { clean.push_str(&rest[start..start + START.len() + end + END.len()]); }
        rest = &tail[end + END.len()..];
    }
    clean.push_str(rest);
    (clean, calls)
}

#[cfg(test)]
mod parity_tests {
    use super::parse_tool_calls;

    fn options(session: &str, tools: bool, manual: bool) -> super::TurnOptions {
        super::TurnOptions {
            model: "NewApi/chat-pro".to_string(),
            session_id: session.to_string(),
            tools_enabled: tools,
            key: "unit-test-key".to_string(),
            manual,
            prompt: "hello".to_string(),
        }
    }

    fn wire(event: serde_json::Value) -> Vec<u8> {
        format!("data: {event}\n\n").into_bytes()
    }

    fn idle(session: &str) -> Vec<u8> {
        wire(serde_json::json!({"payload":{"type":"session.status","properties":{"sessionID":session,"status":{"type":"idle"}}}}))
    }

    fn texts(frames: &[super::Frame]) -> String {
        frames.iter().filter_map(|frame| match frame {
            super::Frame::Chunk(value) => value["choices"][0]["delta"]["content"].as_str().map(str::to_string),
            super::Frame::Done => None,
        }).collect()
    }

    /// 喂一段 assistant 文本（登记消息 + part，再追加 delta）。
    fn assistant_text(stream: &mut super::TeleAgentStream, session: &str, first: &str, more: &[&str]) -> Vec<super::Frame> {
        let mut frames = Vec::new();
        let msg = wire(serde_json::json!({"payload":{"type":"message.updated","properties":{"sessionID":session,"info":{"id":"m1","role":"assistant"}}}}));
        frames.extend(stream.feed(&msg).unwrap_or_default());
        let part = wire(serde_json::json!({"payload":{"type":"message.part.updated","properties":{"sessionID":session,"part":{"id":"p1","type":"text","text":first,"messageID":"m1"}}}}));
        frames.extend(stream.feed(&part).unwrap_or_default());
        for delta in more {
            let event = wire(serde_json::json!({"payload":{"type":"message.part.delta","properties":{"sessionID":session,"partID":"p1","field":"text","delta":delta}}}));
            frames.extend(stream.feed(&event).unwrap_or_default());
        }
        frames
    }

    /// B2 回归（流层面）：上一轮残留的待确认项不能吞掉本轮的 Done。
    #[test]
    fn leftover_pending_does_not_swallow_done() {
        let session = "sess-stream-b2";
        let stale = serde_json::json!({"id":"old-perm","sessionID":session});
        assert!(super::super::confirmations::record("permission", session, &stale, "k").is_ok());
        let mut stream = super::TeleAgentStream::new(&options(session, false, true));
        let frames = stream.feed(&idle(session)).unwrap_or_default();
        assert!(stream.saw_end, "残留项不属于本轮，Done 必须生效");
        assert!(matches!(frames.last(), Some(super::Frame::Done)));
        super::super::confirmations::clear_session(session);
    }

    /// 本轮登记的确认会推迟 Done；上游 `permission.replied` 之后 Done 生效。
    #[test]
    fn own_pending_defers_done_until_replied() {
        let session = "sess-stream-own";
        let mut stream = super::TeleAgentStream::new(&options(session, false, true));
        let ask = wire(serde_json::json!({"payload":{"type":"permission.asked","properties":{"id":"perm-1","sessionID":session}}}));
        assert!(stream.feed(&ask).is_ok());
        assert!(stream.feed(&idle(session)).unwrap_or_default().is_empty());
        assert!(!stream.saw_end);
        let replied = wire(serde_json::json!({"payload":{"type":"permission.replied","properties":{"permissionID":"perm-1","sessionID":session,"response":"once"}}}));
        assert!(stream.feed(&replied).is_ok());
        assert!(!stream.feed(&idle(session)).unwrap_or_default().is_empty());
        assert!(stream.saw_end);
        super::super::confirmations::clear_session(session);
    }

    /// S1：默认（未开启人工确认）与旧版一致，立即 502 中断，不登记队列。
    #[test]
    fn confirmation_defaults_to_immediate_error() {
        let session = "sess-default";
        let mut stream = super::TeleAgentStream::new(&options(session, false, false));
        let ask = wire(serde_json::json!({"payload":{"type":"question.asked","properties":{"id":"q-1","sessionID":session,"description":"pick one"}}}));
        match stream.feed(&ask) {
            Err(error) => {
                assert_eq!(error.status_code, 502);
                assert!(error.message.contains("pick one"));
            }
            Ok(_) => panic!("expected immediate error"),
        }
        assert!(!super::super::confirmations::list().to_string().contains(session));
    }

    /// S5：工具模式下标记前的文本照常流式下发，标记之后缓冲解析。
    #[test]
    fn tools_mode_streams_plain_text_before_marker() {
        let session = "sess-tools";
        let mut stream = super::TeleAgentStream::new(&options(session, true, false));
        let frames = assistant_text(&mut stream, session, "Let me ", &["check. [TO", "OL_CALL]{\"name\":\"run\",\"arguments\":{}}[/TOOL_CALL]"]);
        assert_eq!(texts(&frames), "Let me check. ");
        let end = stream.feed(&idle(session)).unwrap_or_default();
        let has_call = end.iter().any(|frame| matches!(frame, super::Frame::Chunk(v) if v["choices"][0]["delta"]["tool_calls"][0]["function"]["name"] == "run"));
        assert!(has_call);
        assert!(!texts(&end).contains("TOOL_CALL"));
    }

    #[test]
    fn step_finish_tokens_feed_usage() {
        let session = "sess-usage";
        let mut stream = super::TeleAgentStream::new(&options(session, false, false));
        let step = wire(serde_json::json!({"payload":{"type":"message.part.updated","properties":{"sessionID":session,"part":{"id":"s1","type":"step-finish","tokens":{"input":120,"output":30,"reasoning":5,"cache":{"read":100,"write":0}}}}}}));
        assert!(stream.feed(&step).is_ok());
        let usage = stream.usage();
        assert_eq!(usage["prompt_tokens"], 120);
        assert_eq!(usage["completion_tokens"], 35);
        assert_eq!(usage["prompt_tokens_details"]["cached_tokens"], 100);
        assert!(usage.get("estimated").is_none());
    }
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
        let stream=super::TeleAgentStream::new(&options("test-session", true, false));
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
