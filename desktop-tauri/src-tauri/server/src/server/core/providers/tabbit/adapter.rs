//! Tabbit 适配器：把会话式协议层接进 provider 注册表。
//!
//! ── 这一层解决什么问题 ──────────────────────────────────────
//! 协议实现（签名 / 建会话 / SSE 翻译）全部收在 `providers/tabbit/`
//! （`chat.rs`），对外只留一个口子：`chat::run_chat_with_busy_retry(...)`。
//! 本文件就是那个「接线」：把转发编排给的散装入参转调过去。装配以外的逻辑
//! 一行都不在这里。
//!
//! ── 与 KukuAI 同属「有状态」────────────────────────────────
//! `is_stateful()` 返回 true：上游一次对话 = 建会话 → 签名 POST → SSE，
//! 塞不进单请求契约（`build_chat_request` 返回 503 防御，理由与 KukuAI 相同）。
//! 默认每次新建会话；流式请求带显式会话 ID 时复用（注册表见 `session.rs`），
//! 会话 ID 可来自 body 或 `x-session-id` 头（`client_headers` 转交给 `chat.rs`）。
//!
//! ── 两个地区是两个实例 ──────────────────────────────────────
//! `TabbitAdapter` 持有一个 `region::Region`，两个静态实例
//! （`TABBIT_ADAPTER` / `TABBIT_INTL_ADAPTER`）由 `adapter_for` 按 kind 给出
//! （与 AutoClaw / Qoder / Accio / ZCode 同款参数化）。
//!
//! ── 错误分类（四个档的语义，见 trait 文档）───────────────────
//!   - 401/403（登录态失效）→ **QuotaLimited**：标记该账号对本次模型冷却 +
//!     换下一账号 —— 参考实现「401/403 标记登录失效并切换下一账号」的语义
//!     在本架构下的对应动作（本家无刷新接口，TokenExpired 那档的「刷新后同一
//!     账号重试」只会白打一次）；
//!   - 492（无权限 / 超长 / 权益）→ Fatal：`explain_error` 已翻译成可读文案，
//!     换账号重发同一份 body 也一样被拦，不罚账号但也不换家；
//!   - 409（会话忙）→ run_chat 内部已等待重试，重试耗尽才到这里，按 Fatal 透传；
//!   - 其余 → Fatal。
//!
//! ── 凭证：JWT Cookie，**无法刷新** ──────────────────────────
//! 上游没有刷新接口（`supports_refresh = false`），过期只能重新登录 / 重新
//! 粘贴（与 KukuAI / CatPaw 同一处境）。`supports_web_login` 为 true：
//! 官方登录页 + 壳侧读 Cookie 收尾（见 `login.rs`）。
//!
//! ── 思考等级绑定不接（诚实声明）──────────────────────────────
//! 上游没有思考档位参数（参考实现也没有），按 trait 契约「没有证据就不注入」
//! 返回 Skip。
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

use super::{balance, chat, credentials, login, models};
use crate::server::core::providers::adapter::{
    ChatRequestPlan, ModelRefreshOutcome, ProviderAdapter, UpstreamErrorClass,
};
use crate::server::core::providers::{kind_id, ProviderKind};

use super::region::Region;

/// Tabbit 适配器（无字段单例：会话注册表在 `session.rs`，凭证在账号存储里，
/// 本结构只持有一个地区标记）。
pub struct TabbitAdapter {
    region: Region,
}

impl TabbitAdapter {
    const fn new(region: Region) -> Self {
        Self { region }
    }
}

/// 进程级实例（`adapter_for` 返回它们的 `&'static` 引用）
pub static TABBIT_ADAPTER: TabbitAdapter = TabbitAdapter::new(Region::Cn);
pub static TABBIT_INTL_ADAPTER: TabbitAdapter = TabbitAdapter::new(Region::Intl);

/// 把 HTTP 状态 + 上游文案归入四个档（无状态路径的防御性实现 + 会话式路径复用）。
fn classify(region: Region, status: u16, message: &str, upstream_code: Option<i64>) -> UpstreamErrorClass {
    match status {
        401 | 403 => UpstreamErrorClass::QuotaLimited {
            // 登录态失效：标记冷却 + 换下一账号（参考「标记登录失效并切换」）。
            // reset_at 给 None（编排层兜底冷却）；文案点名「重新登录」
            reset_at: None,
            message: format!(
                "Tabbit（{}）登录态已失效，请重新登录或重新添加该账号：{message}",
                region.label()
            ),
            upstream_code,
            status,
        },
        _ => UpstreamErrorClass::Fatal {
            status,
            message: message.to_string(),
            upstream_code,
        },
    }
}

impl ProviderAdapter for TabbitAdapter {
    fn kind(&self) -> ProviderKind {
        self.region.kind()
    }

    /// 上游是「建会话 → 签名 POST → SSE」的两步时序（固有性质）。
    ///
    /// 编排层据此把本 provider 分流到 [`Self::forward_conversation`]，
    /// 而不是 `build_chat_request` + `send_chat_request` 那条单请求路径。
    fn is_stateful(&self) -> bool {
        true
    }

    /// 模型清单：**远程目录优先**，未拉到用静态兜底（`models.rs`）。
    fn list_models(&self) -> Vec<Value> {
        models::list(self.region)
    }

    /// **防御性报错**：Tabbit 走会话式转发，不走单请求路径（模块头）。
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

    /// 错误分类（模块头的四档口径）。
    ///
    /// 正常路径上走不到（会话式路径的错误在 `chat.rs` 内已归一），这里是
    /// 防御性实现 + `classify_conversation_error` 复用同一口径。
    fn classify_error(&self, status: u16, error_body: &Value) -> UpstreamErrorClass {
        let message = error_body
            .get("message")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .unwrap_or("上游错误")
            .to_string();
        let upstream_code = error_body.get("code").and_then(Value::as_i64);
        classify(self.region, status, &message, upstream_code)
    }

    /// 会话式路径的错误分类：401/403 按 QuotaLimited 记账（换下一账号），
    /// 其余 Fatal 透传（默认行为）。
    ///
    /// ── 为什么覆写（默认 Fatal 不够）─────────────────────────
    /// 参考实现的多账号池语义是「401/403 标记登录失效并切换下一账号」；
    /// 默认的 Fatal 会让死凭证的账号每次都被选中、白撞一次。用 QuotaLimited
    /// 的记账语义（标记冷却后按队列顺延）把这条语义接进来 —— 编排层只取
    /// QuotaLimited 的记账动作，不会再套无状态路径的重试/刷新。
    fn classify_conversation_error(&self, error: &GatewayError) -> UpstreamErrorClass {
        let status = u16::try_from(error.status_code).unwrap_or(500);
        classify(self.region, status, &error.message, error.upstream_code)
    }

    /// 拉取远程模型目录（`GET /proxy/v1/model_config/models`，见 `models.rs`）。
    ///
    /// 凭证取当前账号（与转发同一套 Cookie）；没有可用登录态时返回
    /// `unchanged()`（不是错误）。`force` 一路透传。
    fn refresh_models<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        force: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ModelRefreshOutcome> + Send + 'a>,
    > {
        let region = self.region;
        Box::pin(async move { models::refresh(region, store, account_id, force).await })
    }

    /// 有远程模型目录（`/proxy/v1/model_config/models`）。
    fn supports_model_refresh(&self) -> bool {
        true
    }

    /// 凭证：从账号记录取 JWT（返回 token 字符串；其余字段在
    /// `forward_conversation` 里随完整凭证取）。
    ///
    /// 本家**没有**环境变量旁路 —— `account_id` 为空时走本地区组内最优账号，
    /// 取不到就报错并说明该怎么补（与 KukuAI 同口径）。
    fn ensure_access_token<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<String, GatewayError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let credentials = credentials::snapshot_for(self.region, store, account_id)?;
            Ok(credentials.token)
        })
    }

    /// 有余额概念：`GET /api/commerce/quota/v1/usage`（`balance.rs`，上游真实值）。
    fn supports_usage(&self) -> bool {
        true
    }

    /// 查余额（`balance.rs`，归一化形状见 trait 文档）。
    fn query_usage<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Value, GatewayError>> + Send + 'a>,
    > {
        let region = self.region;
        Box::pin(async move { balance::query_usage(region, store, account_id).await })
    }

    /// 支持**网页登录**：打开 Tabbit 官方登录页（Google / Apple 第三方登录都在
    /// 这张页上，官方页面自己处理风控），登录成功后由壳侧把 `token` Cookie
    /// 交回网关（见 `login.rs` 模块头）。
    fn supports_web_login(&self) -> bool {
        true
    }

    /// 构造登录页地址 + 一次性 state（`login.rs`）。
    fn build_login_url(&self) -> Option<(String, String)> {
        let state = login::new_login_state();
        Some((login::build_login_url(self.region, &state), state))
    }

    /// 账号未显式配代理时是否跟随系统代理：国际版在国内直连固定 403，
    /// 跟随系统代理（见 `region.rs`）。
    fn system_proxy_when_unset(&self) -> bool {
        self.region.system_proxy_when_unset()
    }

    /// 会话式转发入口：转调 `chat::run_chat_with_busy_retry`。
    ///
    /// ── 分工（哪些判断不属于这里）──────────────────────────────
    ///   账号选路、限额冷却、provider 轮询、telemetry 记账都在编排层；
    ///   协议时序（建会话 / 签名 POST / SSE 翻译 / 409 重试）都在 `chat.rs`。
    ///   本函数只做**装配**：把散装入参原样转调。
    fn forward_conversation<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        body: &'a Value,
        client_headers: &'a HeaderMap,
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
        let region = self.region;
        Box::pin(async move {
            chat::run_chat_with_busy_retry(
                region, store, account_id, body, client_headers, proxy, stream, telemetry,
            )
                .await
        })
    }
}
