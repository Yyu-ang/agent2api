//! TeleAgent 适配器：把会话式协议层接进 provider 注册表。
//!
//! ── 这一层解决什么问题 ──────────────────────────────────────
//! 协议实现（签名 / 建会话 / prompt_async / SSE 翻译）全部收在
//! `providers/teleagent/`（`auth.rs` / `protocol.rs` / `stream.rs`），
//! 对外只留一个口子：`chat::run_chat(...) -> ForwardOutcome`。
//! 本文件就是那个「接线」：把转发编排给的散装入参转调 `run_chat`，
//! 外加 trait 契约要求的错误分类 / 凭证 / 模型目录。装配以外的逻辑
//! 一行都不在这里（与 kuku 的 `adapter.rs` 同分工）。
//!
//! ── 有状态 ──────────────────────────────────────────────────
//! `is_stateful()` 为 true：上游一次对话 = 建会话 → prompt_async → SSE
//! 三步，塞不进单请求契约（`build_chat_request` 返回 503 防御，理由与
//! KukuAI / CatPaw 相同）。会话是**请求内**的（新建即用、用毕即弃），
//! 因此没有会话注册表 / 轮次状态机。
//!
//! ── 凭证：无账号，session key 即凭证 ─────────────────────────
//!   - `ensure_access_token`：返回 `auth::session_key()`（60 秒缓存）；
//!     取不到 → 503「请确保 TeleAgent 桌面应用正在运行」；
//!   - `refresh_access_token`：**强制**重取（跳过缓存）—— 对应原实现
//!     `sa_request` 里 401/403 后 `_force_refresh_session_key()` 的语义；
//!   - `allows_anonymous_default_session() = true`：零账号可转发
//!     （TeleAgent 没有账号概念）；
//!   - `env_credentials_present()`：此刻能否取到 key —— 决定 `/v1/models`
//!     广告不广告 TeleAgent 的模型（`catalog::provider_available` 的口径）；
//!   - `supports_refresh` 保持默认 false：key 走 60 秒缓存 + 401 懒刷新，
//!     没有「过期时间」可供维护任务做临期判断（见 trait 文档「判不出来
//!     就返回 false」）。
//!
//! ── 错误分类（trait 四档语义，见 `adapter.rs` 模块头）─────────
//!   - 401/403 → `TokenExpired`：key  stale，重取后同一（空）账号重试一次
//!     —— 正好是原实现「认证失败刷新 key 重试一次」的语义；
//!   - 429 → `QuotaLimited`（上游真限流时走冷却 + 换账号；本家无账号，
//!     效果是按冷却退避）；
//!   - 其余 → `Fatal` 原样透传。`ContentBlocked` 不声明：super-agent 是
//!     本地 agent，没有内容审核拦截的实测证据（trait 契约：没证据不声明）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic，不持任何锁跨 await。

use std::sync::Arc;

use axum::http::HeaderMap;
use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::core::upstream::ForwardOutcome;
use crate::server::errors::GatewayError;

use super::{auth, chat, models};
use crate::server::core::providers::adapter::{
    ChatRequestPlan, ModelRefreshOutcome, ProviderAdapter, UpstreamErrorClass,
};
use crate::server::core::providers::{kind_id, ProviderKind};

/// TeleAgent 适配器（无状态单例：会话是请求内的，凭证是进程级 key 缓存，
/// 本结构不持有任何字段）。
pub struct TeleAgentAdapter;

/// 进程级实例（`adapter_for` 返回它的 `&'static` 引用）
pub static TELEAGENT_ADAPTER: TeleAgentAdapter = TeleAgentAdapter;

impl ProviderAdapter for TeleAgentAdapter {
    fn kind(&self) -> ProviderKind {
        ProviderKind::TeleAgent
    }

    /// 上游是「建会话 → prompt_async → SSE」的三步时序（固有性质）。
    ///
    /// 编排层据此把本 provider 分流到 [`Self::forward_conversation`]，
    /// 而不是 `build_chat_request` + `send_chat_request` 那条单请求路径。
    fn is_stateful(&self) -> bool {
        true
    }

    /// 模型清单：**远程目录优先**，未拉到用静态兜底（`models.rs`）。
    fn list_models(&self) -> Vec<Value> {
        models::list()
    }

    /// **防御性报错**：TeleAgent 走会话式转发，不走单请求路径（模块头）。
    fn build_chat_request(
        &self,
        _account: &Value,
        _body: &Value,
        _client_headers: &HeaderMap,
    ) -> Result<ChatRequestPlan, GatewayError> {
        Err(GatewayError::with_status(
            503,
            format!(
                "{} 走会话式转发，不走单请求路径（内部错误：编排层未按 is_stateful 分流）",
                kind_id(self.kind())
            ),
        ))
    }

    /// 错误分类（四档语义见模块头）。
    fn classify_error(&self, status: u16, error_body: &Value) -> UpstreamErrorClass {
        let message = error_body
            .get("message")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .unwrap_or("上游错误")
            .to_string();
        match status {
            // session key 失效 → 刷新凭证（强制重取 key）后同一账号重试一次
            401 | 403 => UpstreamErrorClass::TokenExpired { message },
            429 => UpstreamErrorClass::QuotaLimited {
                reset_at: None,
                message,
                upstream_code: None,
                status,
            },
            _ => UpstreamErrorClass::Fatal {
                status,
                message,
                upstream_code: None,
            },
        }
    }

    /// 拉取远程模型目录（`GET /provider`，见 `models.rs`）。
    ///
    /// TeleAgent 无账号概念：`store` / `account_id` 忽略（签名 key 即凭证）。
    /// 没有可用登录态（TeleAgent 没跑）时返回 `unchanged()`（不是错误）。
    /// `force` 一路透传：`false` 走 TTL 早退（自动路径），`true` 真打上游
    /// （用户手动点了「刷新模型清单」）。
    fn refresh_models<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        force: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ModelRefreshOutcome> + Send + 'a>,
    > {
        Box::pin(async move { models::refresh(store, account_id, force).await })
    }

    /// 有远程模型目录（`GET /provider`）。
    fn supports_model_refresh(&self) -> bool {
        true
    }

    /// 模型目录刷新不走「账号」维度（无账号概念，`models::refresh` 忽略
    /// `account_id`）——「获取模型」弹窗不显示无意义的账号下拉
    /// （与 [`Self::refresh_models`] 的参数配对，见 trait 文档）。
    fn refresh_uses_account(&self) -> bool {
        false
    }

    /// 凭证：session key（`auth::session_key`，60 秒缓存）。
    ///
    /// TeleAgent 没有账号：`account_id` 忽略，永远取本机 key。
    /// 取不到 → 503，文案告诉用户去启动 TeleAgent 桌面应用。
    fn ensure_access_token<'a>(
        &'a self,
        _store: &'a AccountStore,
        _account_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<String, GatewayError>> + Send + 'a>,
    > {
        Box::pin(async move {
            auth::session_key(false).ok_or_else(|| {
                GatewayError::with_status(
                    503,
                    "无法获取 SUPER_AGENT_LOCAL_SESSION_KEY，请确保 TeleAgent 桌面应用正在运行",
                )
            })
        })
    }

    /// 401 后的**强制**刷新：跳过 60 秒缓存，真取一次（对应原实现
    /// `_force_refresh_session_key()` 的语义）。
    fn refresh_access_token<'a>(
        &'a self,
        _store: &'a AccountStore,
        _account_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<String, GatewayError>> + Send + 'a>,
    > {
        Box::pin(async move {
            auth::session_key(true).ok_or_else(|| {
                GatewayError::with_status(
                    401,
                    "TeleAgent 认证失败且无法刷新 session key，请确认 TeleAgent 桌面应用正在运行",
                )
            })
        })
    }

    /// 零账号可转发：TeleAgent 没有账号概念，本机 key 就是默认登录态。
    ///
    /// 编排层据此在「没有任何账号记录」时放行（否则直接 503
    /// 「请在账号页添加并启用账号」）。
    fn allows_anonymous_default_session(&self) -> bool {
        true
    }

    /// 此刻能否取到 session key（`catalog::provider_available` 的口径：
    /// 决定 `/v1/models` 广告不广告 TeleAgent 的模型）。
    ///
    /// 只做「存在且非空」判定：走 60 秒缓存，不发网络（读本机进程环境
    /// 不算网络，见 `auth.rs`）。
    fn env_credentials_present(&self) -> bool {
        auth::session_key(false).is_some_and(|key| !key.is_empty())
    }

    /// 会话式转发入口：转调 `chat::run_chat`。
    ///
    /// ── 分工（哪些判断不属于这里）──────────────────────────────
    ///   账号选路、限额冷却、provider 轮询、telemetry 记账都在编排层；
    ///   协议时序（建会话 / prompt_async / SSE 翻译）都在 `chat.rs` /
    ///   `protocol.rs` / `stream.rs`。本函数只做**装配**。
    fn forward_conversation<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        body: &'a Value,
        _client_headers: &'a HeaderMap,
        proxy: Option<ResolvedProxy>,
        stream: bool,
        telemetry: &'a Arc<RequestTelemetry>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ForwardOutcome, GatewayError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { chat::run_chat(store, account_id, body, proxy, stream, telemetry).await })
    }
}
