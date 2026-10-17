//! Optional OpenCode2API bridge for the Docker sidecar.
//!
//! This is an integration with a SEPARATE GPL-3.0-or-later Go program. No code
//! from that program is vendored or linked into this Rust crate.
//!
//! Enabled only when both AGENT2API_OPENCODE2API_URL and
//! AGENT2API_OPENCODE2API_KEY are explicitly supplied. Uses the existing
//! custom-provider routing, account selection, SSE translation and reporting;
//! it never starts a second public Agent2API listener.

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::custom_providers;
use crate::server::logging;

const NAME: &str = "OpenCode2API";
const FALLBACK_MODEL: &str = "sota";

/// Register the sidecar as a normal custom provider/account, idempotently.
///
/// Called only after legacy-account migration has been ruled out. The upstream
/// server key is a *local proxy key*, not a Zen/Go credential. Zen/Go keys stay
/// in the independent upstream component and never enter Agent2API's DB.
pub fn bootstrap(store: &AccountStore) {
    let Ok(url) = std::env::var("AGENT2API_OPENCODE2API_URL") else {
        return;
    };
    let Ok(key) = std::env::var("AGENT2API_OPENCODE2API_KEY") else {
        logging::log("[OpenCode2API]", "环境变量缺少 AGENT2API_OPENCODE2API_KEY，跳过接入");
        return;
    };
    let key = key.trim();
    if key.is_empty() {
        logging::log("[OpenCode2API]", "网关 Key 为空，拒绝创建无鉴权连接");
        return;
    }
    let base_url = match custom_providers::normalize_base_url(&url) {
        Ok(url) => url,
        Err(error) => {
            logging::log("[OpenCode2API]", &format!("上游地址无效: {error}"));
            return;
        }
    };

    // Match both name and endpoint. Never take over an unrelated custom
    // provider just because it happens to use the same upstream URL.
    let existing = custom_providers::list().into_iter().find(|item| {
        item.get("name").and_then(Value::as_str) == Some(NAME)
            && item.get("baseUrl").and_then(Value::as_str) == Some(base_url.as_str())
    });
    let provider = match existing {
        Some(item) => item,
        None => match custom_providers::create(&json!({
            "name": NAME,
            "protocol": custom_providers::PROTOCOL_CHAT_COMPLETIONS,
            "baseUrl": base_url,
        })) {
            Ok(item) => item,
            Err(error) => {
                logging::log("[OpenCode2API]", &format!("注册提供商失败: {error}"));
                return;
            }
        },
    };
    let Some(provider_id) = provider.get("id").and_then(Value::as_str) else {
        logging::log("[OpenCode2API]", "提供商记录缺少 id");
        return;
    };

    // The account-store method merges a repeated identical key instead of
    // adding duplicate accounts after every application restart.
    if let Err(error) = store.add_custom_account(
        provider_id,
        &json!({"apiKey": key}),
        Some(NAME),
    ) {
        logging::log("[OpenCode2API]", &format!("登记账号失败: {error:?}"));
        return;
    }

    // Custom providers intentionally have no built-in catalogue. Seed only a
    // provisional alias; the task below replaces it with the actual remote
    // catalogue. Never overwrite user-curated models or mappings.
    if !custom_providers::models_of(provider_id).is_empty() {
        return;
    }
    if let Err(error) = custom_providers::set_models(
        provider_id,
        json!([{"id": FALLBACK_MODEL, "enabled": true}]),
        json!([]),
    ) {
        logging::log("[OpenCode2API]", &format!("初始化模型失败: {error}"));
        return;
    }
    let provider_id = provider_id.to_string();
    let store = store.clone();
    crate::spawn_task(async move {
        let models = match custom_providers::fetch_upstream_models(&provider_id, &store).await {
            Ok(models) if !models.is_empty() => models,
            Ok(_) => {
                logging::log("[OpenCode2API]", "上游返回空模型列表，保留 sota 兜底");
                return;
            }
            Err(error) => {
                logging::log("[OpenCode2API]", &format!("自动获取模型失败（可在模型页重试）: {error}"));
                return;
            }
        };
        // A user may have edited the list while HTTP discovery was in flight.
        // Never destroy that work. Compare the exact provisional shape.
        let current = custom_providers::models_of(&provider_id);
        if current.len() != 1
            || current[0].get("id").and_then(Value::as_str) != Some(FALLBACK_MODEL)
            || !custom_providers::mappings_of(&provider_id).is_empty()
        {
            return;
        }
        let entries = models.into_iter().map(|id| json!({"id": id, "enabled": true})).collect();
        if let Err(error) = custom_providers::set_models(&provider_id, Value::Array(entries), json!([])) {
            logging::log("[OpenCode2API]", &format!("保存模型目录失败: {error}"));
        }
    });
}
