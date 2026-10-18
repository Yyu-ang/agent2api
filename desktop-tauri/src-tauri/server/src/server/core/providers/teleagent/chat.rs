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
//! super-agent 的会话自行维护上下文。**流式**请求带显式会话 ID（body 字段或
//! `x-session-id` 头）时按账号、密钥、模型、工作目录、工具定义与网关调用方隔离，
//! 只发历史新增部分（不重发 system / 工具 schema / 上游自己的回复）；复用会话
//! 上投递失败时作废会话，用新会话 + 全量历史重试一次。无会话 ID 或非流式请求
//! 每请求独立；历史改写或完全重复也新建会话。模型由 `models::resolve_model`
//! 校验，不静默替换。
//!
//! ── token 记账 ──────────────────────────────────────────────
//! 见 `stream.rs`：`step-finish.tokens` 累加，拿不到时按字符估算，两路都
//! `report_usage`。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::sync::Arc;

use axum::http::HeaderMap;
use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::core::upstream::ForwardOutcome;
use crate::server::errors::GatewayError;

use super::session::Turn;
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
    headers: &HeaderMap,
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
    // 工具 schema 只看有没有定义（建会话时注入一次）；tool_choice 是逐轮提示。
    let tools = body.get("tools").filter(|v| v.as_array().is_some_and(|items| !items.is_empty()));
    let tools_enabled = tools.is_some() && body.get("tool_choice").and_then(Value::as_str) != Some("none");
    let directory = protocol::default_directory();
    let model_key = format!("{provider_id}/{model_id}");
    let fingerprints = super::session::fingerprints(body.get("messages"));
    // 只有「流式 + 显式会话 ID」才复用；非流式辅助请求不读写会话映射（CatPaw 同口径）。
    let slot = if stream {
        super::session::entry(account_id, &key, &model_key, &directory, body, headers)
    } else {
        None
    };
    // 排队上限 30 秒（超时 409）、可取消；未 commit 就 drop → 会话作废（B1）。
    let mut turn = Turn::acquire(slot, telemetry, "TeleAgent").await?;
    if body.get("reset_session").and_then(Value::as_bool) == Some(true) {
        turn.reset();
    }

    // 先建立事件监听再投递消息，减少漏掉早期 delta 的竞态。
    let event_stream = stream::open_event_stream_with_key(&key).await?;
    let target = Target { directory: &directory, provider_id: &provider_id, model_id: &model_id, key: &key };
    let (session_id, prompt) = open_turn(&mut turn, &fingerprints, body, tools, &target).await?;

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

    let options = stream::TurnOptions {
        model: display_model,
        session_id,
        tools_enabled,
        key: key.clone(),
        manual: super::confirmations::manual_enabled(body),
        prompt,
    };
    if stream {
        Ok(stream::stream_outcome(event_stream, options, turn, fingerprints, Arc::clone(telemetry)))
    } else {
        // 非流式不持有会话（见上），`turn` 是空持有。
        let result = stream::aggregate(event_stream, &options, telemetry).await;
        drop(turn);
        result
    }
}

/// 上游投递目标（同一把 key 签名）。
struct Target<'a> {
    directory: &'a str,
    provider_id: &'a str,
    model_id: &'a str,
    key: &'a str,
}

/// 能复用就投递增量；复用会话上投递失败（会话被删、过期等）时作废，
/// 用新会话 + 全量历史重试一次。认证失败（401）不重试，交给编排层处理。
async fn open_turn(
    turn: &mut Turn,
    fingerprints: &[String],
    body: &Value,
    tools: Option<&Value>,
    target: &Target<'_>,
) -> Result<(String, String), GatewayError> {
    if let Some((id, start)) = turn.reusable(fingerprints) {
        if let Some(messages) = super::session::incremental_messages(body, start) {
            // system 与工具 schema 建会话时已发，增量只带新消息 + 本轮 tool_choice 提示。
            let prompt = protocol::turn_prompt(Some(&messages), None, body);
            match protocol::prompt_async_with_key(
                &id, &prompt, target.directory, target.provider_id, target.model_id, target.key,
            )
            .await
            {
                Ok(()) => return Ok((id, prompt)),
                Err(error) if error.status_code == 401 => return Err(error),
                Err(error) => crate::server::logging::verbose(
                    "[TeleAgent]",
                    &format!("复用会话投递失败（{}），改用新会话 + 全量历史重试", error.message),
                ),
            }
        }
    }
    turn.reset();
    let id = protocol::create_session_with_key(target.directory, target.key).await?;
    let prompt = protocol::turn_prompt(body.get("messages"), tools, body);
    protocol::prompt_async_with_key(
        &id, &prompt, target.directory, target.provider_id, target.model_id, target.key,
    )
    .await?;
    Ok((id, prompt))
}
