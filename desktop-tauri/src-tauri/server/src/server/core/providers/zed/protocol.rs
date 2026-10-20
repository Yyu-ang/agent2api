//! Zed 转发的协议纯函数：四家 wire 的请求变换、错误塑形、NDJSON 行的
//! 解读与「流说了什么」的判定。全部逐字对照插件 `index.mjs` 的
//! `providerRequest` / `refusal` / `failedStatus` / `failure` / `said` /
//! `eventError` / `errorBody` / `lines` / `chatSaid`，网络与翻译状态机的
//! 编排在 `forward.rs`。
//!
//! ── wire 与 vendor 的关系 ─────────────────────────────────
//! vendor 是 Zed 侧的提供商名（`anthropic` / `open_ai` / `x_ai` /
//! `google`，进 `/completions` 的 `provider` 字段）；wire 是这家讲的 API
//! 方言（anthropic = Messages、open_ai = Responses、x_ai = Chat
//! Completions、google = Gemini）。插件按「SDK 发到哪个路径」判 wire，
//! 本仓入站已在 api 层归一成 Chat 形态、没有那条路径可看，因此按 vendor
//! 直接定 wire（一一对应，见 [`Wire::of_vendor`]），vendor 本身由目录
//! 条目记录（`models::vendor_of`，没记录时按模型名猜）。

use serde_json::{json, Value};

/// 一家上游的 API 方言。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Wire {
    Anthropic,
    Responses,
    Chat,
    Gemini,
}

impl Wire {
    /// vendor 名 → wire（插件 `WIRE_VENDOR` 的逆映射，一一对应）。
    pub fn of_vendor(vendor: &str) -> Wire {
        match vendor {
            "anthropic" => Wire::Anthropic,
            "google" => Wire::Gemini,
            "x_ai" => Wire::Chat,
            // open_ai 与任何兜底都走 Responses（插件 guess 的兜底也是 open_ai）
            _ => Wire::Responses,
        }
    }

    /// wire → 这家在 `/completions` 里报的 provider 名。
    pub fn vendor(self) -> &'static str {
        match self {
            Wire::Anthropic => "anthropic",
            Wire::Responses => "open_ai",
            Wire::Chat => "x_ai",
            Wire::Gemini => "google",
        }
    }
}

/// 把一份 vendor 原生请求改成 Zed 要的 `provider_request`（插件
/// `providerRequest` 逐字），返回（改后的请求, 调用方原本是否要流式）。
///
/// Gemini 分支在插件里没有 stream 概念（由路径后缀决定，而本仓的调用方
/// 意图由 `forward_conversation` 的 `stream` 参数给出），因此这里对
/// Gemini 返回的 stream 恒为它在请求里读到的值（通常缺失 = false），
/// 调用方不采用它 —— 差异只在这一点，已在 `forward.rs` 注明。
pub fn provider_request(wire: Wire, mut req: Value, model: &str) -> (Value, bool) {
    match wire {
        Wire::Anthropic => {
            let stream = req
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            // 云端恒流式：请求里不许带 stream 字段
            if let Some(object) = req.as_object_mut() {
                object.remove("stream");
            }
            // 云端拒绝缺 is_error 的 tool_result（"missing field `is_error`"），
            // 而 Anthropic 自家 API 在 false 时是省略它的 —— 没有就补 false
            if let Some(messages) = req.get_mut("messages").and_then(Value::as_array_mut) {
                for message in messages.iter_mut() {
                    let Some(blocks) =
                        message.get_mut("content").and_then(Value::as_array_mut)
                    else {
                        continue;
                    };
                    for block in blocks.iter_mut() {
                        let is_tool_result = block.get("type").and_then(Value::as_str)
                            == Some("tool_result");
                        if is_tool_result {
                            if let Some(object) = block.as_object_mut() {
                                object
                                    .entry("is_error".to_string())
                                    .or_insert(Value::Bool(false));
                            }
                        }
                    }
                }
            }
            (req, stream)
        }
        Wire::Responses => {
            let stream = req
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(object) = req.as_object_mut() {
                object.insert("stream".to_string(), Value::Bool(true));
            }
            (req, stream)
        }
        Wire::Chat => {
            let stream = req
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(object) = req.as_object_mut() {
                object.insert("stream".to_string(), Value::Bool(true));
                // xAI 的写法：max_tokens 改名 max_completion_tokens
                if let Some(max_tokens) = object.remove("max_tokens") {
                    object.insert("max_completion_tokens".to_string(), max_tokens);
                }
            }
            (req, stream)
        }
        Wire::Gemini => {
            if let Some(object) = req.as_object_mut() {
                object.remove("session_id");
                object.remove("sessionId");
                object.insert(
                    "model".to_string(),
                    Value::String(format!("models/{model}")),
                );
            }
            (req, false)
        }
    }
}

// ─── 错误塑形（插件 failure / failedStatus / refusal / errorBody）───

/// 错误体里的人读文案（插件 `failure`）：`{message}` → `{error: "…"}` →
/// `{code}` → 短原文 → 402 的套餐文案 → 状态名兜底（本仓没有 Node 的
/// STATUS_CODES 表，用 reqwest 的状态名近似，差异只在极冷门的码上）。
pub fn failure(status: u16, body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<Value>(body) {
        if let Some(object) = parsed.as_object() {
            if let Some(message) = object.get("message").and_then(Value::as_str) {
                if !message.is_empty() {
                    return message.to_string();
                }
            }
            if let Some(error) = object.get("error").and_then(Value::as_str) {
                if !error.is_empty() {
                    return error.to_string();
                }
            }
            if let Some(code) = object.get("code") {
                let text = match code {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                if !text.is_empty() {
                    return text;
                }
            }
        }
    }
    let trimmed = body.trim();
    if !trimmed.is_empty() && trimmed.len() < 500 {
        return trimmed.to_string();
    }
    if status == 402 {
        return "payment required: this Zed account has no plan that calls models, or its allowance is used up".to_string();
    }
    reqwest::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason())
        .map(str::to_string)
        .unwrap_or_else(|| status.to_string())
}

/// 失败码 → HTTP 状态（插件 `failedStatus` 逐字）：`upstream_http_<n>` /
/// `http_<n>` 前缀 → n；含 rate_limit → 429；overloaded → 529；
/// billing / payment → 402；context_length → 400；其余 502。
pub fn failed_status(code: &str) -> u16 {
    for prefix in ["upstream_http_", "http_"] {
        if let Some(rest) = code.strip_prefix(prefix) {
            if let Ok(number) = rest.parse::<u16>() {
                return number;
            }
        }
    }
    if code.contains("rate_limit") {
        return 429;
    }
    if code.contains("overloaded") {
        return 529;
    }
    if code.contains("billing") || code.contains("payment") {
        return 402;
    }
    if code.contains("context_length") {
        return 400;
    }
    502
}

/// 一次被 Zed 拒绝的请求最终报什么状态与文案（插件 `refusal` 逐字）：
/// 402 与「刚铸好的令牌立刻被拒」的 401 有写死的文案；其余优先错误体里的
/// `upstream_status`（400–599），再 `code` 映射（映射出 502 则不采用），
/// 最后把状态夹回 400–599；`retry_after > 0` 时把秒数拼进文案。
pub fn refusal(status: u16, body: &str) -> (u16, String) {
    if status == 402 {
        return (
            402,
            "payment required — this account's plan doesn't include Zed's hosted models, or its allowance is used up (see zed.dev/account)".to_string(),
        );
    }
    if status == 401 {
        return (401, "the sign-in was refused — sign in again".to_string());
    }
    let parsed = serde_json::from_str::<Value>(body).unwrap_or(Value::Null);
    let mut code = status;
    let upstream = parsed.get("upstream_status").and_then(Value::as_f64);
    if let Some(upstream) = upstream {
        if (400.0..=599.0).contains(&upstream) {
            code = upstream as u16;
        }
    } else if let Some(raw_code) = parsed.get("code") {
        let text = match raw_code {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let mapped = failed_status(&text);
        if mapped != 502 {
            code = mapped;
        }
    }
    if !(400..=599).contains(&code) {
        code = 502;
    }
    let mut message = failure(status, body);
    if let Some(retry_after) = parsed.get("retry_after").and_then(Value::as_f64) {
        if retry_after > 0.0 {
            message = format!("{message} (retry after {}s)", retry_after as i64);
        }
    }
    (code, message)
}

/// 失败在调用方 API 里的形状（插件 `errorBody` 逐字）。本仓的客户端面是
/// Chat 形态，因此转发路径实际只用默认分支；其余三家保留全貌，是因为
/// 「流中失败要以该 wire 的形状发回去再翻译」时 anthropic / responses
/// 分支会被 `forward` 用到，gemini 分支供完整性对照。
pub fn error_body(wire: Wire, status: u16, message: &str) -> Value {
    match wire {
        Wire::Anthropic => {
            let kind = match status {
                400 => "invalid_request_error",
                401 => "authentication_error",
                402 => "billing_error",
                403 => "permission_error",
                404 => "not_found_error",
                429 => "rate_limit_error",
                529 => "overloaded_error",
                _ => "api_error",
            };
            json!({ "type": "error", "error": { "type": kind, "message": message } })
        }
        Wire::Responses => json!({
            "type": "error",
            "code": status.to_string(),
            "message": message,
            "error": { "message": message, "code": status.to_string() },
        }),
        Wire::Gemini => {
            let name = match status {
                400 => "INVALID_ARGUMENT",
                401 => "UNAUTHENTICATED",
                402 | 429 => "RESOURCE_EXHAUSTED",
                403 => "PERMISSION_DENIED",
                404 => "NOT_FOUND",
                529 => "UNAVAILABLE",
                _ => "INTERNAL",
            };
            json!({ "error": { "code": status, "message": message, "status": name } })
        }
        Wire::Chat => json!({
            "error": { "message": message, "type": "api_error", "code": status }
        }),
    }
}

// ─── NDJSON 行的解读（插件 lines）───

/// 解开一行 NDJSON 后的东西。
#[derive(Debug, PartialEq)]
pub enum Line {
    /// 一个 vendor 原生流事件
    Event(Value),
    /// `{"status": "stream_ended"}`：回复完整结束
    Ended,
    /// `{"status": {"failed": {code, message}}}`：流中失败
    Failed { code: String, message: String },
}

/// 解一行（插件 `lines` 的单行逻辑逐字）：未 wrapped 时整行就是事件；
/// wrapped 时 `event` 键是事件、`status` 是状态行，其余状态（started /
/// queued 等）不产生任何东西（返回 None）。解不出 JSON 是**错误**
/// （插件在此抛 "an unreadable line"），由调用方按 502 处置。
pub fn parse_line(raw: &str, wrapped: bool) -> Result<Option<Line>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed: Value = serde_json::from_str(trimmed)
        .map_err(|_| "Zed: an unreadable line".to_string())?;
    if !wrapped {
        return Ok(Some(Line::Event(parsed)));
    }
    if let Some(event) = parsed.get("event") {
        if !event.is_null() {
            return Ok(Some(Line::Event(event.clone())));
        }
    }
    match parsed.get("status") {
        Some(Value::String(text)) if text == "stream_ended" => Ok(Some(Line::Ended)),
        Some(Value::Object(status)) => {
            if let Some(failed) = status.get("failed") {
                let code = failed
                    .get("code")
                    .map(|code| match code {
                        Value::String(text) => text.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                let message = failed
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                return Ok(Some(Line::Failed { code, message }));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

// ─── 「流说了什么」（插件 said / eventError）───

fn truthy_text(value: Option<&Value>) -> bool {
    value.and_then(Value::as_str).is_some_and(|text| !text.is_empty())
}

/// 一个事件算不算「说了话」（插件 `said` 逐字）：框架事件（message_start /
/// ping / response.created / 只带 usage 的 chat 分片…）不算。首个「说了话」
/// 的事件之前若失败，整轮按被拒处理而不是 200 空流（见 `forward`）。
pub fn said(wire: Wire, event: &Value) -> bool {
    if !event.is_object() {
        return true;
    }
    match wire {
        Wire::Anthropic => match event.get("type").and_then(Value::as_str) {
            Some("message_start" | "ping" | "content_block_stop") => false,
            Some("content_block_start") => {
                // 插件：`!["text","thinking","redacted_thinking"].includes(type)
                // || !!(text || thinking)` —— 非文本块恒算说话；文本/思考块要
                // 真带了内容才算（空的开场块只是框架）。
                let block = event.get("content_block");
                let kind = block
                    .and_then(|block| block.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !matches!(kind, "text" | "thinking" | "redacted_thinking") {
                    return true;
                }
                block.is_some_and(|block| {
                    truthy_text(block.get("text")) || truthy_text(block.get("thinking"))
                })
            }
            Some("content_block_delta") => {
                let delta = event.get("delta");
                let empty = delta.is_some_and(|delta| {
                    delta.get("text").and_then(Value::as_str) == Some("")
                        || delta.get("thinking").and_then(Value::as_str) == Some("")
                        || delta.get("partial_json").and_then(Value::as_str) == Some("")
                });
                !empty
            }
            _ => true,
        },
        Wire::Responses => match event.get("type").and_then(Value::as_str) {
            Some(
                "response.created"
                | "response.in_progress"
                | "response.queued"
                | "response.content_part.added"
                | "response.reasoning_summary_part.added",
            ) => false,
            Some("response.output_item.added") => {
                let kind = event
                    .get("item")
                    .and_then(|item| item.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                !matches!(kind, "message" | "reasoning")
            }
            Some(
                "response.output_text.delta"
                | "response.reasoning_summary_text.delta"
                | "response.reasoning_text.delta",
            ) => truthy_text(event.get("delta")),
            _ => true,
        },
        Wire::Chat => {
            if event.get("error").is_some_and(|error| !error.is_null()) {
                return true;
            }
            let Some(choices) = event.get("choices").and_then(Value::as_array) else {
                return false; // 只有 usage 的分片
            };
            choices.iter().any(|choice| {
                let delta = choice.get("delta");
                choice
                    .get("finish_reason")
                    .is_some_and(|reason| !reason.is_null())
                    || delta.is_some_and(|delta| {
                        truthy_text(delta.get("content"))
                            || truthy_text(delta.get("reasoning_content"))
                            || truthy_text(delta.get("reasoning"))
                            || delta
                                .get("tool_calls")
                                .and_then(Value::as_array)
                                .is_some_and(|calls| !calls.is_empty())
                    })
            })
        }
        Wire::Gemini => {
            if event.get("error").is_some_and(|error| !error.is_null()) {
                return true;
            }
            event
                .get("candidates")
                .and_then(Value::as_array)
                .is_some_and(|candidates| {
                    candidates.iter().any(|candidate| {
                        candidate
                            .get("finishReason")
                            .is_some_and(|reason| !reason.is_null())
                            || candidate
                                .pointer("/content/parts")
                                .and_then(Value::as_array)
                                .is_some_and(|parts| {
                                    parts.iter().any(|part| {
                                        truthy_text(part.get("text"))
                                            || part.get("functionCall").is_some_and(|call| !call.is_null())
                                            || part.get("thought").is_some_and(|thought| !thought.is_null())
                                    })
                                })
                    })
                })
        }
    }
}

/// 事件自带的错误文案（插件 `eventError` 逐字）：不是错误事件 → None。
pub fn event_error(wire: Wire, event: &Value) -> Option<String> {
    if !event.is_object() {
        return None;
    }
    let message_of = |holder: Option<&Value>| -> String {
        holder
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    match wire {
        Wire::Anthropic => {
            if event.get("type").and_then(Value::as_str) == Some("error") {
                Some(message_of(event.get("error")))
            } else {
                None
            }
        }
        Wire::Responses => {
            let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
            if matches!(
                kind,
                "response.completed" | "response.incomplete" | "response.failed"
            ) {
                if let Some(error) = event
                    .get("response")
                    .and_then(|response| response.get("error"))
                    .filter(|error| !error.is_null())
                {
                    return Some(message_of(Some(error)));
                }
                return None;
            }
            if kind == "error" {
                if event.get("error").is_some_and(|error| !error.is_null()) {
                    return Some(message_of(event.get("error")));
                }
                return Some(
                    event
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                );
            }
            None
        }
        Wire::Chat | Wire::Gemini => {
            if event.get("error").is_some_and(|error| !error.is_null()) {
                Some(message_of(event.get("error")))
            } else {
                None
            }
        }
    }
}

/// 把一个 vendor 事件写成它自家 SSE 的字节形（插件 `sse` 逐字）：
/// anthropic / responses 带 `event:` 行，chat / gemini 只有 `data:` 行。
pub fn sse_bytes(wire: Wire, event: &Value) -> Vec<u8> {
    let data = format!("data: {}\n\n", event);
    match wire {
        Wire::Anthropic | Wire::Responses => {
            if let Some(kind) = event.get("type").and_then(Value::as_str) {
                return format!("event: {kind}\n{data}").into_bytes();
            }
            data.into_bytes()
        }
        _ => data.into_bytes(),
    }
}

// ─── Chat wire 的截断收尾（插件 chatSaid / #91 特例）───

/// 截断说明（插件 `CUT_SHORT_NOTE` 逐字）。
pub const CUT_SHORT_NOTE: &str =
    "(The reply was cut short: Zed closed the stream before it finished. Ask again to go on.)";

/// 跟踪一条 Chat 流「说了什么」（插件 `chatSaid` 逐字）：哪些 index 写过
/// 正文、哪些开始过、哪些动过 tool call、哪些收过尾。流被 Zed 掐断
/// （wrapped 且没有 stream_ended）时，只有「已说过话且没有半截 tool
/// call」才配用收尾帧补完；其余情形必须如实报错。
#[derive(Default)]
pub struct ChatSaid {
    wrote: std::collections::HashSet<i64>,
    open: std::collections::HashSet<i64>,
    tools: std::collections::HashSet<i64>,
    done: std::collections::HashSet<i64>,
    meta: Option<(String, i64, String)>,
}

impl ChatSaid {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn see(&mut self, event: &Value) {
        let Some(choices) = event.get("choices").and_then(Value::as_array) else {
            return;
        };
        if self.meta.is_none() {
            self.meta = Some((
                event
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                event.get("created").and_then(Value::as_i64).unwrap_or(0),
                event
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            ));
        }
        for choice in choices {
            let index = choice.get("index").and_then(Value::as_i64).unwrap_or(0);
            let delta = choice.get("delta");
            let content = delta.and_then(|delta| delta.get("content"));
            if truthy_text(content) {
                self.wrote.insert(index);
            }
            if truthy_text(content)
                || delta.is_some_and(|delta| {
                    truthy_text(delta.get("reasoning_content"))
                        || truthy_text(delta.get("reasoning"))
                })
            {
                self.open.insert(index);
            }
            if delta
                .and_then(|delta| delta.get("tool_calls"))
                .and_then(Value::as_array)
                .is_some_and(|calls| !calls.is_empty())
            {
                self.tools.insert(index);
            }
            if choice
                .get("finish_reason")
                .is_some_and(|reason| !reason.is_null())
            {
                self.done.insert(index);
            }
        }
    }

    /// 掐断时的收尾帧（插件 `chatSaid.end` 逐字）：不配补完 → None。
    pub fn end(&self) -> Option<Vec<Vec<u8>>> {
        if self.open.is_empty() && self.tools.is_empty() && self.done.is_empty() {
            return None;
        }
        if self.tools.iter().any(|index| !self.done.contains(index)) {
            return None;
        }
        let mut left: Vec<i64> = self
            .open
            .iter()
            .copied()
            .filter(|index| !self.done.contains(index))
            .collect();
        left.sort_unstable();
        if left.is_empty() {
            return Some(vec![b"data: [DONE]\n\n".to_vec()]);
        }
        let (id, created, model) = match &self.meta {
            Some((id, created, model)) => (id.clone(), *created, model.clone()),
            None => (
                "chatcmpl-cut-short".to_string(),
                crate::server::logging::now_ms() / 1000,
                String::new(),
            ),
        };
        let chunk = |choices: Vec<Value>| -> Vec<u8> {
            let body = json!({
                "id": id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": choices,
            });
            format!("data: {body}\n\n").into_bytes()
        };
        let note_choices: Vec<Value> = left
            .iter()
            .map(|index| {
                let prefix = if self.wrote.contains(index) { "\n\n" } else { "" };
                json!({
                    "index": index,
                    "delta": { "content": format!("{prefix}{CUT_SHORT_NOTE}") },
                    "finish_reason": null,
                })
            })
            .collect();
        let stop_choices: Vec<Value> = left
            .iter()
            .map(|index| json!({ "index": index, "delta": {}, "finish_reason": "stop" }))
            .collect();
        Some(vec![
            chunk(note_choices),
            chunk(stop_choices),
            b"data: [DONE]\n\n".to_vec(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_request_drops_stream_and_fills_is_error() {
        let req = json!({
            "model": "claude-sonnet-5",
            "stream": true,
            "messages": [
                { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "t1", "content": "ok" },
                    { "type": "tool_result", "tool_use_id": "t2", "content": "bad", "is_error": true },
                    { "type": "text", "text": "hi" },
                ]},
            ],
        });
        let (out, stream) = provider_request(Wire::Anthropic, req, "claude-sonnet-5");
        assert!(stream, "调用方原本要流式这一事实要带回来");
        assert!(out.get("stream").is_none(), "stream 字段必须删掉");
        let blocks = out
            .pointer("/messages/0/content")
            .and_then(Value::as_array)
            .expect("content 应在");
        assert_eq!(blocks[0]["is_error"], Value::Bool(false), "缺失的 is_error 补 false");
        assert_eq!(blocks[1]["is_error"], Value::Bool(true), "已有的 is_error 不许改");
        assert!(blocks[2].get("is_error").is_none(), "非 tool_result 不许动");
    }

    #[test]
    fn chat_request_renames_max_tokens() {
        let req = json!({ "model": "grok-4.7", "max_tokens": 512, "stream": false });
        let (out, stream) = provider_request(Wire::Chat, req, "grok-4.7");
        assert!(!stream);
        assert_eq!(out["stream"], Value::Bool(true), "云端恒流式，强制 stream=true");
        assert!(out.get("max_tokens").is_none());
        assert_eq!(out["max_completion_tokens"], 512);
    }

    #[test]
    fn responses_request_forces_stream() {
        let req = json!({ "model": "gpt-5.5", "input": [] });
        let (out, _) = provider_request(Wire::Responses, req, "gpt-5.5");
        assert_eq!(out["stream"], Value::Bool(true));
    }

    #[test]
    fn gemini_request_prefixes_model_and_drops_session() {
        let req = json!({ "contents": [], "sessionId": "abc", "session_id": "def" });
        let (out, _) = provider_request(Wire::Gemini, req, "gemini-3.5-flash");
        assert_eq!(out["model"], "models/gemini-3.5-flash");
        assert!(out.get("sessionId").is_none());
        assert!(out.get("session_id").is_none());
    }

    #[test]
    fn failed_status_maps_codes() {
        assert_eq!(failed_status("upstream_http_429"), 429);
        assert_eq!(failed_status("http_404"), 404);
        assert_eq!(failed_status("rate_limit_exceeded"), 429);
        assert_eq!(failed_status("overloaded"), 529);
        assert_eq!(failed_status("billing_error"), 402);
        assert_eq!(failed_status("payment_required"), 402);
        assert_eq!(failed_status("context_length_exceeded"), 400);
        assert_eq!(failed_status("whatever"), 502);
    }

    #[test]
    fn refusal_prefers_upstream_status_then_code() {
        // 402 / 401 是写死文案
        assert_eq!(refusal(402, "{}").0, 402);
        assert!(refusal(402, "{}").1.contains("payment required"));
        assert_eq!(refusal(401, "{}").0, 401);
        // upstream_status 优先于 HTTP 状态
        let (code, _) = refusal(500, r#"{"upstream_status": 429, "message": "slow down"}"#);
        assert_eq!(code, 429);
        // code 映射次之
        let (code, _) = refusal(500, r#"{"code": "upstream_http_529", "message": "x"}"#);
        assert_eq!(code, 529);
        // code 映射出 502 时不采用，回到原状态
        let (code, _) = refusal(503, r#"{"code": "mystery", "message": "x"}"#);
        assert_eq!(code, 503);
        // retry_after 拼进文案
        let (_, message) = refusal(500, r#"{"message": "wait", "retry_after": 7}"#);
        assert!(message.contains("(retry after 7s)"), "{message}");
    }

    #[test]
    fn parse_line_wrapped_and_bare() {
        assert_eq!(
            parse_line(r#"{"event": {"type": "ping"}}"#, true).expect("应可解"),
            Some(Line::Event(json!({ "type": "ping" })))
        );
        assert_eq!(
            parse_line(r#"{"status": "stream_ended"}"#, true).expect("应可解"),
            Some(Line::Ended)
        );
        match parse_line(r#"{"status": {"failed": {"code": "rate_limit", "message": "slow"}}}"#, true)
            .expect("应可解")
        {
            Some(Line::Failed { code, message }) => {
                assert_eq!(code, "rate_limit");
                assert_eq!(message, "slow");
            }
            other => panic!("应是 Failed，实际 {other:?}"),
        }
        // 其它状态行不产生任何东西
        assert_eq!(parse_line(r#"{"status": "started"}"#, true).expect("应可解"), None);
        // 未 wrapped：整行即事件
        assert_eq!(
            parse_line(r#"{"type": "ping"}"#, false).expect("应可解"),
            Some(Line::Event(json!({ "type": "ping" })))
        );
        // 解不出是错误而不是静默跳过
        assert!(parse_line("not json", true).is_err());
    }

    #[test]
    fn said_distinguishes_framing_from_content() {
        assert!(!said(Wire::Anthropic, &json!({ "type": "message_start" })));
        assert!(!said(
            Wire::Anthropic,
            &json!({ "type": "content_block_delta", "delta": { "type": "text_delta", "text": "" } })
        ));
        assert!(said(
            Wire::Anthropic,
            &json!({ "type": "content_block_delta", "delta": { "type": "text_delta", "text": "hi" } })
        ));
        assert!(!said(Wire::Responses, &json!({ "type": "response.created" })));
        assert!(said(
            Wire::Responses,
            &json!({ "type": "response.output_text.delta", "delta": "x" })
        ));
        // chat 的 usage-only 分片不算说话
        assert!(!said(Wire::Chat, &json!({ "usage": { "total_tokens": 3 } })));
        assert!(said(
            Wire::Chat,
            &json!({ "choices": [{ "index": 0, "delta": { "content": "x" } }] })
        ));
        assert!(said(
            Wire::Gemini,
            &json!({ "candidates": [{ "content": { "parts": [{ "text": "x" }] } }] })
        ));
    }

    #[test]
    fn chat_said_cut_short_rules() {
        // 什么都没说 → 不能补完
        let tracker = ChatSaid::new();
        assert!(tracker.end().is_none());
        // 说了话、没收尾、没有 tool call → 补说明 + stop + DONE
        let mut tracker = ChatSaid::new();
        tracker.see(&json!({ "id": "c1", "created": 10, "model": "grok-4.7",
            "choices": [{ "index": 0, "delta": { "content": "hello" } }] }));
        let frames = tracker.end().expect("应可补完");
        assert_eq!(frames.len(), 3);
        let first = String::from_utf8(frames[0].clone()).expect("utf8");
        assert!(first.contains(CUT_SHORT_NOTE), "{first}");
        assert!(first.contains("\\n\\n(The reply"), "写过正文的 index 补 \\n\\n 前缀: {first}");
        // tool call 开始了却没收尾 → 不能补完（如实报错）
        let mut tracker = ChatSaid::new();
        tracker.see(&json!({ "choices": [{ "index": 0, "delta": {
            "tool_calls": [{ "index": 0, "function": { "arguments": "{" } }] } }] }));
        assert!(tracker.end().is_none());
    }
}
