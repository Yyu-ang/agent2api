//! TeleAgent 对话转发：建会话 → prompt_async → SSE 事件流 → OpenAI 形状。
//!
//! ── 上游时序（移植自 teleagent-openai-proxy，见 `protocol.rs` / `stream.rs`）
//!   1. `POST /session` 建会话（首次或历史变更）；明确 conversation_id 时可复用；
//!   2. `POST /session/{id}/prompt_async` 异步投递 prompt（204/200 即成功）；
//!   3. `GET /global/event` SSE 按 `sessionID` 过滤收回复，直到
//!      `session.status → idle`。
//!
//! ── 与原实现的一处有意差异 ────────────────────────────────
//! 原实现是「先 prompt_async、再开事件流监听」，中间有竞态（首个 delta
//! 可能在监听建连前发出）。这里**先开流、再投递**：事件流是全局的、
//! 按会话过滤，早开只会多看到别人的事件（被过滤掉），不会丢自己的。
//!
//! ── 多轮与模型 ──────────────────────────────────────────────
//! super-agent 的会话自行维护上下文；明确 conversation_id 时按账号、密钥、
//! 模型、工作目录和工具定义隔离，发送历史新增部分。无会话 ID 时每请求独立，
//! 历史改写也会新建会话。模型由 `models::resolve_model` 校验，不静默替换。
//!
//! ── token 记账 ──────────────────────────────────────────────
//! 原实现从 `step-finish` part 的 `tokens` 字典提取输入/输出 token，
//! 但该字典的形态在参考实现里没有固定 schema（`part.get("tokens", {})`
//! 后直接存）。本移植**不解析它**（强行按猜测的字段读，不如不记），
//! 只做请求/响应的 telemetry 捕获（与 kuku 同口径）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::sync::Arc;

use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::core::upstream::ForwardOutcome;
use crate::server::errors::GatewayError;

use super::{models, protocol, stream, teleagent_id};

/// 转发入口（有状态路径 `forward_conversation` 转调）。
///
/// 凭证：先定 key —— 有账号用账号里存的，无账号自动提取本机 key
/// （见 `teleagent_accounts::session_key_for`）；之后建会话、发消息、
/// 开事件流**全部用同一把 key 签名**，不混用。
///
/// `proxy`：忽略，永远直连本机回环（见 `mod.rs`「出网」）。
pub async fn run_chat(
    store: &AccountStore,
    account_id: &str,
    body: &Value,
    _proxy: Option<ResolvedProxy>,
    stream: bool,
    telemetry: &Arc<RequestTelemetry>,
) -> Result<ForwardOutcome, GatewayError> {
    // 先定 key：账号指定的用账号的，否则自动提取。取不到直接失败，
    // 后面全是无用功（之前这里只认自动提取，手动粘贴的 key 用不上）。
    let key = crate::server::core::account_store::teleagent_accounts::session_key_for(
        store, account_id,
    )?;
    let client_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let (provider_id, model_id) = models::resolve_model(&client_model)?;
    // 广告出去的模型名（`NewApi/chat-pro` 形态），流式帧里回显它
    let display_model = if client_model.trim().is_empty() {
        format!("{provider_id}/{model_id}")
    } else {
        client_model.clone()
    };
    let tools = if body.get("tool_choice").and_then(Value::as_str) == Some("none") {
        None
    } else {
        body.get("tools")
    };
    let tools_enabled = tools.and_then(Value::as_array).is_some_and(|items| !items.is_empty());
    let directory = protocol::default_directory();
    let model_key = format!("{provider_id}/{model_id}");
    let fingerprints = super::session::fingerprints(body.get("messages"));
    let mut conversation = match super::session::entry(account_id, &key, &model_key, &directory, body) {
        Some(state) => Some(state.lock_owned().await),
        None => None,
    };
    if body.get("reset_session").and_then(Value::as_bool) == Some(true) {
        if let Some(state) = conversation.as_mut() { state.reset(); }
    }
    let reuse = conversation.as_ref().and_then(|state| {
        let id = state.session_id.as_ref()?;
        let start = super::session::delta_start(&state.sent, &fingerprints)?;
        Some((id.clone(), start))
    });
    let (session_id, prompt) = if let Some((id, start)) = reuse {
        let messages = super::session::incremental_messages(body, start);
        (id, protocol::build_prompt(messages.as_ref(), tools))
    } else {
        if let Some(state) = conversation.as_mut() { state.reset(); }
        let id = protocol::create_session_with_key(&directory, &key).await?;
        (id, protocol::build_prompt(body.get("messages"), tools))
    };

    // 先建立事件监听再投递消息，减少漏掉早期 delta 的竞态。
    let event_stream = stream::open_event_stream_with_key(&key).await?;
    protocol::prompt_async_with_key(&session_id, &prompt, &directory, &provider_id, &model_id, &key)
        .await?;

    if let Some(capture) = telemetry.capture().as_deref() {
        let url = format!(
            "{}/session/{}/prompt_async",
            super::auth::base_http_url(),
            session_id
        );
        // 明细里记的是投递请求（事件流是 GET 长连接，记它没有意义）
        capture.reset_request(&url, teleagent_id(), &[], body);
        capture.attach_response(event_stream.status().as_u16(), event_stream.headers());
    }

    if stream {
        Ok(stream::stream_outcome(
            event_stream, &display_model, &session_id, tools_enabled, &key,
            conversation, fingerprints,
        ))
    } else {
        let result = stream::aggregate(event_stream, &display_model, &session_id, tools_enabled, &key).await;
        if let Some(state) = conversation.as_mut() {
            if result.is_ok() { state.commit(session_id, fingerprints); }
            else { state.reset(); }
        }
        result
    }
}
