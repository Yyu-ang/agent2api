//! Zed 的 `ProviderAdapter` 实现：能力声明与各子模块的接线。
//!
//! ── 能力一览（理由都在各子模块头里）───────────────────────
//!   * `is_stateful() = true`：转发走 `forward_conversation`（四家 wire
//!     各自原生、响应是 NDJSON，无状态路径容纳不了，取舍见 `mod.rs`）；
//!   * `supports_model_refresh() = true`：上游有 `GET /models` 目录接口；
//!   * `supports_usage() = true`：套餐/账期读数（`balance.rs`）；
//!   * `supports_refresh()` **保持默认 false**：Zed 没有 refresh token，
//!     令牌被拒即登录过期 —— 声称能刷只会让凭证维护每轮白跑一趟并灌
//!     失败日志（与 CatPaw 同一取舍）；
//!   * 网页登录**不**走 trait 的 `supports_web_login` 那套（那是「网关
//!     回调换码」形态）：Zed 是本机监听回调，走 `core::login::zed` 的
//!     专用编排（与 workbuddy / Trae 同一处置，理由见 `login.rs`）。

use axum::http::HeaderMap;
use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::providers::adapter::{
    ChatRequestPlan, ModelRefreshOutcome, ProviderAdapter, UpstreamErrorClass,
};
use crate::server::core::providers::ProviderKind;
use crate::server::errors::GatewayError;

use super::credentials::Credential;
use super::{balance, forward, models};

pub struct ZedAdapter;

pub static ZED_ADAPTER: ZedAdapter = ZedAdapter;

impl ProviderAdapter for ZedAdapter {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Zed
    }

    fn list_models(&self) -> Vec<Value> {
        models::list()
    }

    /// 本家走会话式转发，单请求路径不该被走到（与 CatPaw 同一处置）：
    /// 走到这里说明编排层没按 `is_stateful` 分流，是内部契约错误。
    fn build_chat_request(
        &self,
        _account: &Value,
        _body: &Value,
        _client_headers: &HeaderMap,
    ) -> Result<ChatRequestPlan, GatewayError> {
        Err(GatewayError::with_status(
            503,
            "zed 走会话式转发，不走单请求路径（内部错误：编排层未按 is_stateful 分流）",
        ))
    }

    /// 无状态路径的错误分类（本家转发不走这条，仅为契约完整实现）：
    /// 判据与转发路径共用同一套（`protocol::refusal`）。
    fn classify_error(&self, status: u16, error_body: &Value) -> UpstreamErrorClass {
        let message = error_body
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("上游错误")
            .to_string();
        let full = format!("上游返回 {status}: {message}");
        match status {
            402 | 429 | 529 => UpstreamErrorClass::QuotaLimited {
                reset_at: None,
                message: full,
                upstream_code: error_body.get("code").and_then(Value::as_i64),
                status,
            },
            401 => UpstreamErrorClass::TokenExpired { message: full },
            _ => UpstreamErrorClass::Fatal {
                status,
                message: full,
                upstream_code: error_body.get("code").and_then(Value::as_i64),
            },
        }
    }

    /// 会话式转发失败后的分类（编排层据此记账/顺延）：
    ///   * 402（套餐不含 hosted models 或额度用尽）与 429 / 529 →
    ///     QuotaLimited：标记该账号对该模型冷却，队列顺延到下一个账号
    ///     （插件把 402 当「换一个账号的回合」，同一语义）；
    ///   * 401（登录被拒）→ TokenExpired：本家没有刷新手段，编排层不会
    ///     真去刷（`supports_refresh` 为 false），分类只是把语义说准；
    ///   * 其余 → Fatal 透传（默认语义）。
    fn classify_conversation_error(&self, error: &GatewayError) -> UpstreamErrorClass {
        let status = u16::try_from(error.status_code).unwrap_or(500);
        match status {
            402 | 429 | 529 => UpstreamErrorClass::QuotaLimited {
                reset_at: None,
                message: error.message.clone(),
                upstream_code: error.upstream_code,
                status,
            },
            401 => UpstreamErrorClass::TokenExpired {
                message: error.message.clone(),
            },
            _ => UpstreamErrorClass::Fatal {
                status,
                message: error.message.clone(),
                upstream_code: error.upstream_code,
            },
        }
    }

    /// 取 access token（存在性校验 + 原样返回）：本家没有刷新，这一步
    /// 只回答「这条账号有没有可用的令牌」—— 真正被拒要等铸造/转发时
    /// 上游回 401 才知道（见 `tokens`）。
    fn ensure_access_token<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<String, GatewayError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let record = store.zed_account_record(account_id).ok_or_else(|| {
                GatewayError::with_status(503, "Zed 没有可用账号：请在账号页添加并启用账号")
            })?;
            let credential = Credential::from_record(&record).ok_or_else(|| {
                GatewayError::with_status(401, "Zed 账号缺少访问令牌或 userId，请重新登录")
            })?;
            Ok(credential.access)
        })
    }

    fn refresh_models<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        force: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = ModelRefreshOutcome> + Send + 'a>,
    > {
        Box::pin(async move {
            let record = store.zed_account_record(account_id);
            let Some(record) = record else {
                if account_id.is_empty() {
                    crate::server::logging::verbose(
                        "[Models]",
                        "Zed 模型目录刷新跳过：尚未添加账号",
                    );
                    return ModelRefreshOutcome::unchanged();
                }
                return ModelRefreshOutcome::failed("指定的账号不存在或不可用，请重新选择");
            };
            let resolved_id = if account_id.is_empty() {
                record
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            } else {
                account_id.to_string()
            };
            let Some(credential) = Credential::from_record(&record) else {
                return ModelRefreshOutcome::failed("账号缺少访问令牌或 userId，请重新登录");
            };
            models::refresh(store, &resolved_id, &credential, force).await
        })
    }

    fn supports_model_refresh(&self) -> bool {
        true
    }

    fn is_stateful(&self) -> bool {
        true
    }

    fn forward_conversation<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        body: &'a Value,
        client_headers: &'a HeaderMap,
        proxy: Option<crate::server::core::proxies::ResolvedProxy>,
        stream: bool,
        telemetry: &'a std::sync::Arc<
            crate::server::core::upstream::usage::RequestTelemetry,
        >,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<crate::server::core::upstream::ForwardOutcome, GatewayError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            forward::forward(store, account_id, body, client_headers, proxy, stream, telemetry)
                .await
        })
    }

    fn supports_usage(&self) -> bool {
        true
    }

    fn query_usage<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Value, GatewayError>> + Send + 'a>,
    > {
        Box::pin(async move { balance::query_usage(store, account_id).await })
    }
}
