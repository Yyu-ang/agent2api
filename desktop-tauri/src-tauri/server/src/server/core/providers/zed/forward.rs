//! Zed 的转发（**有状态路径**：`is_stateful() = true`）：把归一成 Chat
//! 形态的入站请求翻成 vendor 原生请求，包进 `POST {CLOUD}/completions`，
//! 再把 NDJSON 响应翻回 Chat SSE / Chat completion。
//!
//! ── 一轮的形状（对照插件 `complete`）──────────────────────
//!   铸造模型令牌 → POST /completions → 令牌被判 stale 就重铸一次再发；
//!   组织被 402 拒绝就 repick 一次（回写账号）再发；其余非 2xx 按
//!   `refusal` 塑形为带状态的错误返回 —— 这些都在本文件内完成，编排层
//!   只保留它必然共用的部分（账号选路循环、限额冷却记账、telemetry）。
//!
//! ── 首包门（对照插件 `streamed`）──────────────────────────
//! 流式响应先把 NDJSON 读到**第一个「说了话」的事件**为止：这之前出现
//! 的失败（failed 状态行 / vendor 自己的 error 事件 / 流断了或结束却没
//! 有任何答案）一律变成带状态的 `GatewayError` 返回，一个字节都不下发
//! —— 客户端因此拿到的是可轮换的错误，而不是「200 开头、里面是失败」
//! 的空流。与 Trae 的首包门同一手法。
//!
//! ── 翻译 ──────────────────────────────────────────────────
//! vendor 事件先按插件 `sse()` 写回它自家的 SSE 字节，再喂给 `protocol`
//! 既有的三台翻译状态机（Anthropic / Responses / Gemini → Chat SSE）；
//! xAI 的 Chat wire 本身就是 Chat SSE，直通（但要跟踪 `chatSaid` 以备
//! 截断收尾）。非流式不另写聚合器：把全部事件喂完翻译机，再用
//! `upstream::aggregate::aggregate_frame_stream` 把 Chat SSE 聚成
//! completion —— 与插件 `whole()` 的四家聚合等价（它聚合的是 vendor
//! 形态，我们聚合的是翻译后的 Chat 形态，结果同形）。
//!
//! ── 流被掐断时（wrapped 且没有 stream_ended）─────────────
//! 插件的规则逐家落：Chat wire 已说过话且没有半截 tool call → 补截断
//! 说明 + stop + [DONE]（#91 特例）；其余情形一律以该 wire 形状的错误
//! 帧收尾（502 "the reply ended before it was complete"）。未 wrapped 的
//! 流没有「完整」信号可判，EOF 即正常结束（Chat wire 补 [DONE]）。
//!
//! ── 硬约束 ────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap / expect / panic；网络与
//! 锁不交叉持有。

use std::sync::Arc;
use std::time::Duration;

use axum::http::HeaderMap;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::server::config;
use crate::server::core::account_store::AccountStore;
use crate::server::core::egress;
use crate::server::core::protocol::anthropic_outbound::{
    anthropic_request_from_chat, ChatFromAnthropicStream,
};
use crate::server::core::protocol::antigravity_outbound::antigravity_request_from_chat;
use crate::server::core::protocol::antigravity_stream::ChatFromAntigravityStream;
use crate::server::core::protocol::responses_outbound::{
    responses_request_from_chat, ChatFromResponsesStream,
};
use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::upstream::aggregate::aggregate_frame_stream;
use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::core::upstream::{stall, ForwardOutcome};
use crate::server::errors::GatewayError;
use crate::server::logging;

use super::credentials::Credential;
use super::protocol::{
    error_body, event_error, failed_status, parse_line, provider_request, refusal, said,
    sse_bytes, ChatSaid, Line, Wire,
};
use super::{http, models, tokens};

/// 转发一轮（适配器 `forward_conversation` 的实现体）。
pub async fn forward(
    store: &AccountStore,
    account_id: &str,
    body: &Value,
    _client_headers: &HeaderMap,
    proxy: Option<ResolvedProxy>,
    stream: bool,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<ForwardOutcome, GatewayError> {
    let record = store.zed_account_record(account_id).ok_or_else(|| {
        GatewayError::with_status(503, "Zed 没有可用账号：请在账号页添加并启用账号")
    })?;
    let credential = Credential::from_record(&record).ok_or_else(|| {
        GatewayError::with_status(
            401,
            "Zed 账号缺少访问令牌或 userId，无法转发（请重新登录）",
        )
    })?;
    let resolved_account_id = if account_id.is_empty() {
        record
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    } else {
        account_id.to_string()
    };
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if model.is_empty() {
        return Err(GatewayError::with_status(400, "请求里缺少 model"));
    }
    let vendor = models::vendor_of(&model);
    let wire = Wire::of_vendor(&vendor);
    let vendor_request = build_vendor_request(wire, body, &model)?;
    let (provider_request, _caller_stream) = provider_request(wire, vendor_request, &model);
    let payload = serde_json::json!({
        "provider": wire.vendor(),
        "model": model,
        "provider_request": provider_request,
    });

    let (response, _credential) = complete(
        store,
        &resolved_account_id,
        &credential,
        &payload,
        proxy.as_ref(),
    )
    .await?;
    let wrapped = http::wrapped(response.headers());
    let byte_stream = response.bytes_stream().map(|item| {
        item.map_err(|error| {
            std::io::Error::other(egress::describe_error_detail(&error))
        })
    });
    let byte_stream = stall::idle_guard(
        Box::pin(byte_stream),
        Duration::from_millis(config::timeout_settings().stream_idle_ms()),
    );
    let mut reader = LineReader::new(byte_stream);

    if !stream {
        return whole(wire, &mut reader, wrapped, &model, telemetry).await;
    }
    streamed(wire, &mut reader, wrapped, &model).await
}

/// Chat 形态的入站 body → vendor 原生请求（复用 `protocol` 既有转换器）。
fn build_vendor_request(wire: Wire, body: &Value, model: &str) -> Result<Value, GatewayError> {
    match wire {
        Wire::Anthropic => anthropic_request_from_chat(body, model)
            .map_err(|error| GatewayError::with_status(400, format!("Zed：请求转 Anthropic 形态失败：{error}"))),
        Wire::Responses => responses_request_from_chat(body, model)
            .map_err(|error| GatewayError::with_status(400, format!("Zed：请求转 Responses 形态失败：{error}"))),
        // Chat wire 的原生请求就是 Chat 请求本身（变换在 provider_request 里）
        Wire::Chat => Ok(body.clone()),
        Wire::Gemini => {
            // Antigravity 的出站转换器产的信封里 `request` 键就是 Gemini
            // generateContent 形态（contents / systemInstruction / tools /
            // generationConfig）；Zed 的 google 面要的正是它（sessionId 与
            // model 由 provider_request 那一步处理）。
            let envelope = antigravity_request_from_chat(body, model, "", false)
                .map_err(|error| GatewayError::with_status(400, format!("Zed：请求转 Gemini 形态失败：{error}")))?;
            envelope
                .get("request")
                .cloned()
                .ok_or_else(|| GatewayError::with_status(502, "Zed：Gemini 请求转换未产出 request"))
        }
    }
}

/// 插件 `complete` 的发送循环：铸造 → 发送 → stale 重铸一次 / 402
/// repick 一次。成功返回（未读的响应, 本轮最终生效的凭证）。
async fn complete(
    store: &AccountStore,
    account_id: &str,
    credential: &Credential,
    payload: &Value,
    proxy: Option<&ResolvedProxy>,
) -> Result<(reqwest::Response, Credential), GatewayError> {
    let mut credential = credential.clone();
    let mut renewed = false;
    let mut moved = false;
    loop {
        let token = match tokens::model_token(&credential, renewed, proxy).await {
            Ok(token) => token,
            Err(error) => {
                if tokens::is_sign_in_expired(&error) {
                    return Err(error);
                }
                if error.status_code == 402 && !moved {
                    moved = true;
                    if let Some(next) = http::repick(&credential, proxy).await {
                        save_rep_elected(store, account_id, &next);
                        credential = next;
                        continue;
                    }
                    let (_, message) = refusal(402, "");
                    return Err(GatewayError::with_status(402, format!("Zed: {message}")));
                }
                return Err(error);
            }
        };
        let response = http::post_completions(&token, payload, proxy)
            .await
            .map_err(|detail| {
                GatewayError::with_status(502, format!("Zed：请求 cloud.zed.dev 失败（{detail}）"))
            })?;
        let status = response.status().as_u16();
        // 令牌被判 stale：重铸一次再发（stale 判定含响应头，见 http::HttpFailure::is_stale 的同名规则）
        let stale = status == 401
            || response.headers().contains_key("x-zed-expired-token")
            || response.headers().contains_key("x-zed-outdated-token");
        if !renewed && stale {
            renewed = true;
            continue;
        }
        if status == 402 && !moved {
            moved = true;
            let text = response.text().await.unwrap_or_default();
            if let Some(next) = http::repick(&credential, proxy).await {
                save_rep_elected(store, account_id, &next);
                credential = next;
                continue;
            }
            let (code, message) = refusal(402, &text);
            return Err(GatewayError::with_status(
                i32::from(code),
                format!("Zed: {message}"),
            ));
        }
        if !(200..300).contains(&status) {
            let text = response.text().await.unwrap_or_default();
            let (code, message) = refusal(status, &text);
            return Err(GatewayError::with_status(
                i32::from(code),
                format!("Zed: {message}"),
            ));
        }
        return Ok((response, credential));
    }
}

fn save_rep_elected(store: &AccountStore, account_id: &str, next: &Credential) {
    if account_id.is_empty() {
        return;
    }
    if let Err(error) = store.update_zed_organization(account_id, &next.org, &next.plan) {
        logging::verbose(
            "[Zed]",
            &format!("repick 后的组织未能回写账号 {account_id}：{}", error.message),
        );
    }
}

// ─── NDJSON 逐行读取 ──────────────────────────────────────

type ByteStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// 按行读 NDJSON（行尾的 `\r` 由 parse_line 的 trim 处理；EOF 时把残段
/// 作为最后一行交出，与插件 `lines` 的收尾一致）。
struct LineReader {
    stream: ByteStream,
    buffer: String,
    exhausted: bool,
}

impl LineReader {
    fn new(stream: ByteStream) -> Self {
        Self {
            stream,
            buffer: String::new(),
            exhausted: false,
        }
    }

    /// 下一行：None = 流已结束；Err = 传输层错误（文案已描述）。
    async fn next_line(&mut self) -> Option<Result<String, String>> {
        loop {
            if let Some(index) = self.buffer.find('\n') {
                let line: String = self.buffer.drain(..=index).collect();
                return Some(Ok(line));
            }
            if self.exhausted {
                if self.buffer.is_empty() {
                    return None;
                }
                return Some(Ok(std::mem::take(&mut self.buffer)));
            }
            match self.stream.next().await {
                Some(Ok(bytes)) => {
                    self.buffer
                        .push_str(&String::from_utf8_lossy(&bytes));
                }
                Some(Err(error)) => {
                    self.exhausted = true;
                    return Some(Err(error.to_string()));
                }
                None => {
                    self.exhausted = true;
                }
            }
        }
    }
}

// ─── vendor 事件 → Chat SSE 的翻译机 ───────────────────────

enum Translator {
    Anthropic(ChatFromAnthropicStream),
    Responses(ChatFromResponsesStream),
    Gemini(ChatFromAntigravityStream),
    /// Chat wire 直通，只跟踪 chatSaid（截断收尾要用）
    Chat(ChatSaid),
}

impl Translator {
    fn new(wire: Wire, model: &str) -> Self {
        match wire {
            Wire::Anthropic => Translator::Anthropic(ChatFromAnthropicStream::new(model)),
            Wire::Responses => Translator::Responses(ChatFromResponsesStream::new(model)),
            Wire::Gemini => {
                Translator::Gemini(ChatFromAntigravityStream::new(model, ""))
            }
            Wire::Chat => Translator::Chat(ChatSaid::new()),
        }
    }

    fn wire(&self) -> Wire {
        match self {
            Translator::Anthropic(_) => Wire::Anthropic,
            Translator::Responses(_) => Wire::Responses,
            Translator::Gemini(_) => Wire::Gemini,
            Translator::Chat(_) => Wire::Chat,
        }
    }

    /// 喂一个 vendor 事件，吐出要下发的 Chat SSE 字节（可能为空）。
    fn push_event(&mut self, event: &Value) -> Vec<Bytes> {
        match self {
            Translator::Anthropic(inner) => {
                inner.push(&sse_bytes(Wire::Anthropic, event))
            }
            Translator::Responses(inner) => {
                inner.push(&sse_bytes(Wire::Responses, event))
            }
            Translator::Gemini(inner) => inner.push(&sse_bytes(Wire::Gemini, event)),
            Translator::Chat(tracker) => {
                tracker.see(event);
                vec![Bytes::from(sse_bytes(Wire::Chat, event))]
            }
        }
    }

    /// 流中失败：以该 wire 自己的错误形状发一帧（经翻译机折回 Chat 形），
    /// 然后收尾（插件 `fail` 逐字：发完即关流，不再补 [DONE] 之外的东西；
    /// 翻译机对 error 事件自带的那份收尾帧包含在内）。
    fn fail(&mut self, status: u16, message: &str) -> Vec<Bytes> {
        let wire = self.wire();
        let body = error_body(wire, status, message);
        match self {
            Translator::Chat(_) => {
                vec![Bytes::from(format!("data: {body}\n\n").into_bytes())]
            }
            _ => self.push_event(&body),
        }
    }

    /// 流正常读完后的收尾帧。
    ///
    /// `ended` 是 wrapped 流的 stream_ended 是否出现：没出现 = 回复不
    /// 完整 —— Chat wire 走 chatSaid 的截断补完（补不出就失败帧），其余
    /// wire 直接失败帧；未 wrapped 的流没有完整性信号，EOF 即完成。
    fn finish(&mut self, wrapped: bool, ended: bool) -> Vec<Bytes> {
        if wrapped && !ended {
            return match self {
                Translator::Chat(tracker) => match tracker.end() {
                    Some(frames) => frames
                        .into_iter()
                        .map(Bytes::from)
                        .collect(),
                    None => self.fail(502, "the reply ended before it was complete"),
                },
                _ => self.fail(502, "the reply ended before it was complete"),
            };
        }
        match self {
            Translator::Anthropic(inner) => inner.finish(),
            Translator::Responses(inner) => inner.finish(),
            Translator::Gemini(inner) => inner.finish(),
            Translator::Chat(_) => vec![Bytes::from_static(b"data: [DONE]\n\n")],
        }
    }
}

// ─── 流式 ──────────────────────────────────────────────────

/// 流式一轮：先过首包门，再把剩余行泵进翻译机下发。
async fn streamed(
    wire: Wire,
    reader: &mut LineReader,
    wrapped: bool,
    model: &str,
) -> Result<ForwardOutcome, GatewayError> {
    // ── 首包门：读到第一个「说了话」的事件为止 ──
    let mut head: Vec<Value> = Vec::new();
    let mut ended = false;
    let mut first: Option<Value> = None;
    while first.is_none() {
        let Some(line) = reader.next_line().await else {
            break; // EOF
        };
        let line = match line {
            Ok(line) => line,
            Err(detail) => {
                return Err(GatewayError::with_status(
                    502,
                    format!("Zed：读取回复流失败（{detail}）"),
                ))
            }
        };
        let parsed = parse_line(&line, wrapped)
            .map_err(|message| GatewayError::with_status(502, message))?;
        let Some(parsed) = parsed else { continue };
        match parsed {
            Line::Ended => ended = true,
            Line::Failed { code, message } => {
                let mut status = failed_status(&code);
                if !(400..=599).contains(&status) {
                    status = 502;
                }
                return Err(GatewayError::with_status(
                    i32::from(status),
                    if message.is_empty() { code } else { message },
                ));
            }
            Line::Event(event) => {
                if let Some(bad) = event_error(wire, &event) {
                    return Err(GatewayError::with_status(502, bad));
                }
                if said(wire, &event) {
                    first = Some(event);
                } else {
                    head.push(event);
                }
            }
        }
    }
    let Some(first_event) = first else {
        if wrapped && !ended {
            return Err(GatewayError::with_status(
                502,
                "Zed: the reply ended before it was complete",
            ));
        }
        return Err(GatewayError::with_status(
            502,
            "Zed ended without an answer",
        ));
    };

    // ── 泵送：剩余行逐行翻译下发 ──
    let model = model.to_string();
    let (sender, receiver) = mpsc::channel::<Result<Bytes, std::io::Error>>(32);
    let mut rest = std::mem::replace(reader, LineReader::new(Box::pin(futures::stream::empty())));
    crate::spawn_task(async move {
        let mut translator = Translator::new(wire, &model);
        let send = |bytes: Vec<Bytes>| async {
            for chunk in bytes {
                if sender.send(Ok(chunk)).await.is_err() {
                    return false;
                }
            }
            true
        };
        for event in &head {
            if !send(translator.push_event(event)).await {
                return;
            }
        }
        if !send(translator.push_event(&first_event)).await {
            return;
        }
        let mut ended = ended;
        loop {
            let Some(line) = rest.next_line().await else {
                let _ = send(translator.finish(wrapped, ended)).await;
                return;
            };
            let line = match line {
                Ok(line) => line,
                Err(detail) => {
                    let _ = send(translator.fail(
                        502,
                        &format!("Zed：读取回复流失败（{detail}）"),
                    ))
                    .await;
                    return;
                }
            };
            let parsed = match parse_line(&line, wrapped) {
                Ok(parsed) => parsed,
                Err(message) => {
                    let _ = send(translator.fail(502, &message)).await;
                    return;
                }
            };
            let Some(parsed) = parsed else { continue };
            match parsed {
                Line::Ended => ended = true,
                Line::Failed { code, message } => {
                    let status = failed_status(&code);
                    let _ = send(translator.fail(
                        status,
                        if message.is_empty() { &code } else { &message },
                    ))
                    .await;
                    return;
                }
                Line::Event(event) => {
                    if !send(translator.push_event(&event)).await {
                        return;
                    }
                }
            }
        }
    });
    Ok(ForwardOutcome::Stream {
        status: 200,
        stream: Box::new(tokio_stream::wrappers::ReceiverStream::new(receiver)),
    })
}

// ─── 非流式 ────────────────────────────────────────────────

/// 非流式一轮（插件 `whole` 的语义）：先把全部行读完判失败/完整性，
/// 再整批喂翻译机聚合成 completion。
async fn whole(
    wire: Wire,
    reader: &mut LineReader,
    wrapped: bool,
    model: &str,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<ForwardOutcome, GatewayError> {
    let mut events: Vec<Value> = Vec::new();
    let mut ended = false;
    while let Some(line) = reader.next_line().await {
        let line = line.map_err(|detail| {
            GatewayError::with_status(502, format!("Zed：读取回复流失败（{detail}）"))
        })?;
        let Some(parsed) = parse_line(&line, wrapped)
            .map_err(|message| GatewayError::with_status(502, message))?
        else {
            continue;
        };
        match parsed {
            Line::Ended => ended = true,
            Line::Failed { code, message } => {
                let mut status = failed_status(&code);
                if !(400..=599).contains(&status) {
                    status = 502;
                }
                return Err(GatewayError::with_status(
                    i32::from(status),
                    if message.is_empty() { code } else { message },
                ));
            }
            Line::Event(event) => {
                if let Some(bad) = event_error(wire, &event) {
                    return Err(GatewayError::with_status(502, bad));
                }
                events.push(event);
            }
        }
    }
    if wrapped && !ended {
        return Err(GatewayError::with_status(
            502,
            "Zed: the reply ended before it was complete",
        ));
    }
    let mut translator = Translator::new(wire, model);
    let mut bytes: Vec<u8> = Vec::new();
    for event in &events {
        for chunk in translator.push_event(event) {
            bytes.extend_from_slice(&chunk);
        }
    }
    for chunk in translator.finish(wrapped, ended) {
        bytes.extend_from_slice(&chunk);
    }
    let frame_stream: ByteStream = Box::pin(futures::stream::iter(vec![Ok::<
        Bytes,
        std::io::Error,
    >(Bytes::from(bytes))]));
    let aggregated = aggregate_frame_stream(
        frame_stream,
        Arc::clone(telemetry),
        crate::server::core::upstream::sse::FramePolicy::with_rewrite(None),
    )
    .await?;
    Ok(ForwardOutcome::Completion {
        body: aggregated.body,
    })
}
