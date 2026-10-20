//! MiniMax Code 的 `ProviderAdapter` 实现（两个地区各一个静态实例，共用
//! 一份参数化实现——与 tabbit / zcode 的拆家先例同款）。
//!
//! ── 转发链 ─────────────────────────────────────────────────
//! 无状态 Anthropic 转发：`POST {llm}/mavis/api/v1/llm/v1/messages`。
//! 请求体由网关统一的 `anthropic_request_from_chat` 从入站 Chat 形态
//! 转成 Anthropic Messages 形态（与 zcode 活动套餐通道同一条转换），
//! 响应声明 `UpstreamResponse::Anthropic`。请求头逐字照插件：
//! ```text
//! Authorization: Bearer <access>
//! x-api-key: sk-xxx                    （占位常量，mcode 的真实形态）
//! User-Agent: MiniMaxAgent
//! X-Mavis-Agent-Id: main
//! X-Mavis-Timezone-Offset: <本地时区偏移秒>
//! X-Mavis-Session-Id: <客户端会话 id，缺省进程级固定 uuid>
//! anthropic-version: 2023-06-01       （客户端带了就沿用客户端的）
//! ```
//! 凭证消费链：`build_chat_request` 读的 `account.auth.accessToken`
//! 来自会话快照，而会话快照由路由层在转发前经
//! `ensure_access_token`（本家的临期主动续期）刷新过——同一份 token，
//! 见 `super` 模块头的五链验证记录。
//!
//! ── 错误分类（插件 `answer()` 与本仓契约的合流）────────────────
//!   - 401 → `TokenExpired`；
//!   - 402 / 403 / 429 且响应体命中额度语义（insufficient / balance /
//!     credit / quota / exhaust / limit / 余额 / 积分 / 额度 / 不足 /
//!     用完 / 上限）→ `QuotaLimited`（插件把这类改写成 429
//!     rate_limit_error，本仓的 QuotaLimited 对外同样收敛到 429 语义）；
//!     其中 **429 一律判 QuotaLimited**（状态码本身就是限流语义，也与
//!     本仓 cline 等各家的既有口径一致；插件对「裸 429」的处理是原样
//!     透传成 429，外部效果相同）；
//!   - 其余 → 内容策略判定后落 Fatal（与 cline 同款收口）。

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::protocol::anthropic_outbound;
use crate::server::errors::GatewayError;

use super::super::adapter::{
    ChatRequestPlan, ModelRefreshOutcome, ProviderAdapter, UpstreamErrorClass, UpstreamResponse,
};
use super::super::content_block;
use super::super::ProviderKind;
use super::region::Region;
use super::{balance, credentials, models, refresh};

/// 占位 API key（插件 `PLACEHOLDER_KEY`：mcode 客户端就是这么发的，逐字保留）
const PLACEHOLDER_KEY: &str = "sk-xxx";
/// Anthropic 协议版本缺省值（插件逐字；客户端带了则沿用）
const ANTHROPIC_VERSION: &str = "2023-06-01";

/// 参数化适配器：字段只有地区，两家共用全部逻辑。
pub struct MinimaxCodeAdapter {
    region: Region,
}

/// 国内版适配器
pub static MINIMAX_CODE_ADAPTER: MinimaxCodeAdapter = MinimaxCodeAdapter { region: Region::Cn };
/// 国际版适配器
pub static MINIMAX_CODE_GLOBAL_ADAPTER: MinimaxCodeAdapter = MinimaxCodeAdapter {
    region: Region::Global,
};

/// 进程级固定会话 id（插件的进程级 `randomUUID()` 同语义：一次进程一份，
/// 客户端没给会话 id 时用它）
fn process_session_id() -> &'static str {
    static SESSION_ID: OnceLock<String> = OnceLock::new();
    SESSION_ID.get_or_init(credentials::random_uuid)
}

/// 客户端的会话 id（优先 `x-session-id`，其次 mcode 自己的头与 opencode 的头）
fn client_session_id(client_headers: &HeaderMap) -> Option<String> {
    for name in [
        "x-session-id",
        "x-mavis-session-id",
        "x-opencode-session",
        "x-session-affinity",
    ] {
        if let Some(value) = client_headers
            .get(name)
            .and_then(|value| value.to_str().ok())
        {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// 额度语义命中判定（插件 `answer()` 的 QUOTA 正则逐词对齐，纯函数）。
///
/// 在**整份响应体的 JSON 文本**上找词（插件是对原始文本跑正则）：英文词
/// 按小写不敏感匹配，中文词直接子串匹配。
pub fn quota_semantics_hit(error_body: &Value) -> bool {
    let text = error_body.to_string().to_lowercase();
    const WORDS: [&str; 12] = [
        "insufficient",
        "balance",
        "credit",
        "quota",
        "exhaust",
        "limit",
        "余额",
        "积分",
        "额度",
        "不足",
        "用完",
        "上限",
    ];
    WORDS.iter().any(|word| text.contains(word))
}

impl ProviderAdapter for MinimaxCodeAdapter {
    fn kind(&self) -> ProviderKind {
        self.region.kind()
    }

    fn list_models(&self) -> Vec<Value> {
        models::list(self.region)
    }

    fn build_chat_request(
        &self,
        account: &Value,
        body: &Value,
        client_headers: &HeaderMap,
    ) -> Result<ChatRequestPlan, GatewayError> {
        let access_token = account
            .get("auth")
            .and_then(|auth| auth.get("accessToken"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .ok_or_else(|| {
                GatewayError::with_status(401, "MiniMax Code 会话缺少 accessToken，请重新登录")
            })?;
        // 入站 model 的分隔符剥离/别名在网关路由完成（与各家契约一致），
        // 这里拿到的是上游真实模型名
        let model = body
            .get("model")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .ok_or_else(|| GatewayError::with_status(400, "请求缺少 model"))?;
        let converted = anthropic_outbound::anthropic_request_from_chat(body, model)
            .map_err(|error| GatewayError::with_status(400, error))?;
        let streaming = converted
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let anthropic_version = client_headers
            .get("anthropic-version")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(ANTHROPIC_VERSION)
            .to_string();
        let session_id =
            client_session_id(client_headers).unwrap_or_else(|| process_session_id().to_string());
        let headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            (
                "Accept".to_string(),
                if streaming {
                    "text/event-stream".to_string()
                } else {
                    "application/json".to_string()
                },
            ),
            (
                "Authorization".to_string(),
                format!("Bearer {access_token}"),
            ),
            ("x-api-key".to_string(), PLACEHOLDER_KEY.to_string()),
            ("User-Agent".to_string(), "MiniMaxAgent".to_string()),
            ("X-Mavis-Agent-Id".to_string(), "main".to_string()),
            (
                "X-Mavis-Timezone-Offset".to_string(),
                credentials::local_timezone_offset_seconds().to_string(),
            ),
            ("X-Mavis-Session-Id".to_string(), session_id),
            ("anthropic-version".to_string(), anthropic_version),
        ];
        Ok(ChatRequestPlan {
            url: format!("{}/mavis/api/v1/llm/v1/messages", self.region.llm_base()),
            headers,
            body: converted,
            response: UpstreamResponse::Anthropic,
        })
    }

    fn classify_error(&self, status: u16, error_body: &Value) -> UpstreamErrorClass {
        let raw = error_body
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|message| !message.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| error_body.to_string());
        let message = format!("上游返回 {status}: {raw}");
        let upstream_code = error_body.get("code").and_then(Value::as_i64);
        if status == 401 {
            return UpstreamErrorClass::TokenExpired { message };
        }
        // 429 一律限流；402/403 要命中额度语义才算（普通 403 是权限/内容
        // 问题，不能误判成额度把账号冷却掉）——判据分工见模块头。
        if status == 429 || ((status == 402 || status == 403) && quota_semantics_hit(error_body)) {
            return UpstreamErrorClass::QuotaLimited {
                reset_at: None,
                message,
                upstream_code,
                status,
            };
        }
        content_block::classify_or_fatal(status, error_body, message, upstream_code)
    }

    fn supports_model_refresh(&self) -> bool {
        true
    }

    fn supports_usage(&self) -> bool {
        true
    }

    fn supports_refresh(&self) -> bool {
        true
    }

    fn credentials_expiring(&self, store: &AccountStore, account_id: &str) -> bool {
        // 同步接口：只读快照判断（不打网络、不持锁跨 await，与契约一致；
        // 判不出（无 refresh / 无过期时间）一律 false，不让维护任务白跑）
        match refresh::snapshot(store, self.region, account_id) {
            Ok(credentials) => credentials.can_refresh() && credentials.is_expiring(),
            Err(_) => false,
        }
    }

    fn ensure_access_token<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, GatewayError>> + Send + 'a>> {
        let region = self.region;
        Box::pin(async move {
            let credentials = refresh::ensure_fresh(store, region, account_id, false).await?;
            Ok(credentials.access_token)
        })
    }

    fn refresh_access_token<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, GatewayError>> + Send + 'a>> {
        let region = self.region;
        Box::pin(async move {
            let credentials = refresh::ensure_fresh(store, region, account_id, true).await?;
            Ok(credentials.access_token)
        })
    }

    fn refresh_models<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
        force: bool,
    ) -> Pin<Box<dyn Future<Output = ModelRefreshOutcome> + Send + 'a>> {
        let region = self.region;
        Box::pin(async move { models::refresh(store, region, account_id, force).await })
    }

    fn query_usage<'a>(
        &'a self,
        store: &'a AccountStore,
        account_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, GatewayError>> + Send + 'a>> {
        let region = self.region;
        Box::pin(async move { balance::query_usage(store, region, account_id).await })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn adapter() -> MinimaxCodeAdapter {
        MinimaxCodeAdapter { region: Region::Cn }
    }

    #[test]
    fn quota_classification_follows_plugin_answer_rules() {
        let adapter = adapter();
        // 402 + 额度语义 → QuotaLimited
        match adapter.classify_error(
            402,
            &json!({ "code": 1008, "message": "insufficient balance, please recharge" }),
        ) {
            UpstreamErrorClass::QuotaLimited { status, .. } => assert_eq!(status, 402),
            other => panic!("应为 QuotaLimited，实际 {other:?}"),
        }
        // 403 + 中文额度语义 → QuotaLimited
        match adapter.classify_error(403, &json!({ "message": "您的积分不足" })) {
            UpstreamErrorClass::QuotaLimited { .. } => {}
            other => panic!("应为 QuotaLimited，实际 {other:?}"),
        }
        // 403 无额度语义 → 不是 QuotaLimited（落 Fatal 一侧）
        match adapter.classify_error(
            403,
            &json!({ "message": "permission denied for this resource" }),
        ) {
            UpstreamErrorClass::QuotaLimited { .. } => {
                panic!("普通 403 不应判成额度受限")
            }
            _ => {}
        }
        // 429 一律 QuotaLimited（状态码自身即限流语义）
        match adapter.classify_error(429, &json!({ "message": "too many requests" })) {
            UpstreamErrorClass::QuotaLimited { status, .. } => assert_eq!(status, 429),
            other => panic!("应为 QuotaLimited，实际 {other:?}"),
        }
        // 401 → TokenExpired
        match adapter.classify_error(401, &json!({ "message": "unauthorized" })) {
            UpstreamErrorClass::TokenExpired { .. } => {}
            other => panic!("应为 TokenExpired，实际 {other:?}"),
        }
        // 500 → Fatal
        match adapter.classify_error(500, &json!({ "message": "boom" })) {
            UpstreamErrorClass::Fatal { status, .. } => assert_eq!(status, 500),
            other => panic!("应为 Fatal，实际 {other:?}"),
        }
    }

    #[test]
    fn quota_semantics_covers_chinese_and_english_words() {
        assert!(quota_semantics_hit(
            &json!({ "message": "quota exhausted" })
        ));
        assert!(quota_semantics_hit(&json!({ "message": "额度已用完" })));
        assert!(quota_semantics_hit(
            &json!({ "error": { "message": "余额不足" } })
        ));
        assert!(!quota_semantics_hit(
            &json!({ "message": "model not found" })
        ));
    }

    #[test]
    fn build_chat_request_uses_mavis_endpoint_and_plugin_headers() {
        let adapter = adapter();
        let account = json!({ "auth": { "accessToken": "token-abc" } });
        let body = json!({
            "model": "MiniMax-M3",
            "messages": [{ "role": "user", "content": "你好" }],
        });
        let headers = HeaderMap::new();
        let plan = adapter
            .build_chat_request(&account, &body, &headers)
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(
            plan.url,
            "https://agent.minimax.cn/mavis/api/v1/llm/v1/messages"
        );
        let header = |name: &str| {
            plan.headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        assert_eq!(header("authorization"), "Bearer token-abc");
        assert_eq!(header("x-api-key"), "sk-xxx");
        assert_eq!(header("user-agent"), "MiniMaxAgent");
        assert_eq!(header("x-mavis-agent-id"), "main");
        assert_eq!(header("anthropic-version"), "2023-06-01");
        assert!(!header("x-mavis-session-id").is_empty());
        assert!(!header("x-mavis-timezone-offset").is_empty());
        // 请求体已转成 Anthropic 形态
        assert_eq!(plan.body["model"], "MiniMax-M3");
        assert!(plan.body["messages"].is_array());
        // 国际版走 .io 的 llm 站点
        let global = MinimaxCodeAdapter {
            region: Region::Global,
        };
        let plan_global = global
            .build_chat_request(&account, &body, &headers)
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(
            plan_global.url,
            "https://agent.minimax.io/mavis/api/v1/llm/v1/messages"
        );
    }

    #[test]
    fn build_chat_request_honors_client_session_and_version_headers() {
        let adapter = adapter();
        let account = json!({ "auth": { "accessToken": "t" } });
        let body = json!({
            "model": "MiniMax-M3",
            "messages": [{ "role": "user", "content": "hi" }],
        });
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-session-id",
            reqwest::header::HeaderValue::from_static("sess-42"),
        );
        headers.insert(
            "anthropic-version",
            reqwest::header::HeaderValue::from_static("2024-01-01"),
        );
        let plan = adapter
            .build_chat_request(&account, &body, &headers)
            .unwrap_or_else(|error| panic!("{}", error.message));
        let header = |name: &str| {
            plan.headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        assert_eq!(header("x-mavis-session-id"), "sess-42");
        assert_eq!(header("anthropic-version"), "2024-01-01");
    }

    #[test]
    fn build_chat_request_without_token_is_401() {
        let adapter = adapter();
        // ChatRequestPlan 没有 Debug，不能用 unwrap_err —— 按形态匹配取 error
        let error = match adapter.build_chat_request(
            &json!({ "auth": {} }),
            &json!({ "model": "MiniMax-M3", "messages": [] }),
            &HeaderMap::new(),
        ) {
            Ok(_) => panic!("缺 token 应失败"),
            Err(error) => error,
        };
        assert_eq!(error.status_code, 401);
    }
}
