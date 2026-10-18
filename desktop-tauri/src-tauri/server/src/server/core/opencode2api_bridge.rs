//! Optional OpenCode2API bridge for the Docker sidecar.
//!
//! This is an integration with a SEPARATE GPL-3.0-or-later Go program. No code
//! from that program is vendored or linked into this Rust crate.
//!
//! Enabled only when both AGENT2API_OPENCODE2API_URL and
//! AGENT2API_OPENCODE2API_KEY are explicitly supplied. Uses the existing
//! custom-provider routing, account selection, SSE translation and reporting;
//! it never starts a second public Agent2API listener.
//!
//! The sidecar is optional and may still be starting (or unhealthy) when the
//! gateway boots, so model discovery runs in the background with backoff and
//! fills the catalogue once the sidecar answers.

use std::time::Duration;

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::custom_providers;
use crate::server::logging;

/// Provider name. MUST stay identical to the `name` of the `opencode2api`
/// card in `desktop-tauri/ui/preset-providers.js`: the UI matches presets by
/// this literal (icon, "Preset API" section, and "already created → add
/// account" instead of creating a second, broken 127.0.0.1 provider).
pub const NAME: &str = "OpenCode2API";
/// Provisional alias written by earlier revisions of this bridge. A catalogue
/// that is exactly this entry is still treated as "not yet discovered".
const LEGACY_FALLBACK_MODEL: &str = "sota";
/// Backoff between model-discovery attempts (seconds). The last value repeats
/// until `DISCOVERY_ATTEMPTS` is reached (≈ 30 minutes in total).
const DISCOVERY_BACKOFF_SECS: &[u64] = &[2, 5, 10, 20, 30, 60, 120];
const DISCOVERY_ATTEMPTS: usize = 20;

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
    let existing = find_provider(&custom_providers::list(), &base_url);
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

    if !sync_account(store, provider_id, key) {
        return;
    }

    // Custom providers intentionally have no built-in catalogue. Discover it
    // from the sidecar in the background; never overwrite a user-curated list.
    if is_provisional_catalogue(
        &custom_providers::models_of(provider_id),
        custom_providers::mappings_of(provider_id).is_empty(),
    ) {
        spawn_model_discovery(provider_id.to_string(), store.clone());
    }
}

/// Make sure exactly one enabled account of this provider carries `key`.
///
/// Account ids are derived from the key, so a rotated server key would
/// otherwise create a second account while the old one stays in the routing
/// pool and keeps answering 401. Returns false when nothing usable is left.
fn sync_account(store: &AccountStore, provider_id: &str, key: &str) -> bool {
    let accounts = store.custom_account_keys(provider_id);
    let plan = plan_account(&accounts, key);
    match &plan.action {
        AccountAction::Keep(_) => {}
        AccountAction::Rotate(id) => {
            // Same account record, new credential: keeps the user's name,
            // priority, proxy and enabled state.
            if let Err(error) = store.update_custom_credentials(id, &json!({"apiKey": key})) {
                logging::log("[OpenCode2API]", &format!("更新网关 Key 失败: {error:?}"));
                return false;
            }
            logging::log("[OpenCode2API]", "检测到网关 Key 已轮换，已更新既有账号的凭证");
        }
        AccountAction::Create => {
            // Name only on creation: a later restart must not overwrite a
            // name the user gave the account in the UI.
            if let Err(error) = store.add_custom_account(provider_id, &json!({"apiKey": key}), Some(NAME)) {
                logging::log("[OpenCode2API]", &format!("登记账号失败: {error:?}"));
                return false;
            }
        }
    }
    for id in &plan.disable {
        match store.update_account(id, &json!({"enabled": false})) {
            Ok(_) => logging::log("[OpenCode2API]", &format!("已停用使用旧网关 Key 的账号 {id}")),
            Err(error) => logging::log("[OpenCode2API]", &format!("停用旧 Key 账号 {id} 失败: {error:?}")),
        }
    }
    true
}

fn spawn_model_discovery(provider_id: String, store: AccountStore) {
    crate::spawn_task(async move {
        for attempt in 0..DISCOVERY_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(backoff_secs(attempt - 1))).await;
            }
            // The user may have curated the list (or deleted the provider)
            // in the meantime. Stop quietly; never destroy that work.
            if custom_providers::get(&provider_id).is_none()
                || !is_provisional_catalogue(
                    &custom_providers::models_of(&provider_id),
                    custom_providers::mappings_of(&provider_id).is_empty(),
                )
            {
                return;
            }
            let models = match custom_providers::fetch_upstream_models(&provider_id, &store).await {
                Ok(models) if !models.is_empty() => models,
                Ok(_) => {
                    if attempt == 0 {
                        logging::log("[OpenCode2API]", "sidecar 暂无可用模型，稍后自动重试");
                    }
                    continue;
                }
                Err(error) => {
                    if attempt == 0 {
                        logging::log(
                            "[OpenCode2API]",
                            &format!("sidecar 尚未就绪（{error}），将在后台退避重试"),
                        );
                    }
                    continue;
                }
            };
            // Re-check right before writing: HTTP discovery took a while.
            if !is_provisional_catalogue(
                &custom_providers::models_of(&provider_id),
                custom_providers::mappings_of(&provider_id).is_empty(),
            ) {
                return;
            }
            let count = models.len();
            let entries = models.into_iter().map(|id| json!({"id": id, "enabled": true})).collect();
            match custom_providers::set_models(&provider_id, Value::Array(entries), json!([])) {
                Ok(_) => logging::log("[OpenCode2API]", &format!("已从 sidecar 获取 {count} 个模型")),
                Err(error) => logging::log("[OpenCode2API]", &format!("保存模型目录失败: {error}")),
            }
            return;
        }
        logging::log(
            "[OpenCode2API]",
            "多次重试后仍无法从 sidecar 获取模型；请检查 `docker compose logs opencode2api`，之后可在模型页手动「获取模型」",
        );
    });
}

// ─── Pure decision helpers (unit-tested below) ──────────────────────────

fn find_provider(list: &[Value], base_url: &str) -> Option<Value> {
    list.iter()
        .find(|item| {
            item.get("name").and_then(Value::as_str) == Some(NAME)
                && item.get("baseUrl").and_then(Value::as_str) == Some(base_url)
        })
        .cloned()
}

/// A catalogue the bridge may (re)fill: empty, or the legacy provisional
/// `[sota]` entry with no mappings. Anything else was curated by the user or
/// already discovered.
fn is_provisional_catalogue(models: &[Value], mappings_empty: bool) -> bool {
    if !mappings_empty {
        return false;
    }
    match models {
        [] => true,
        [only] => only.get("id").and_then(Value::as_str) == Some(LEGACY_FALLBACK_MODEL),
        _ => false,
    }
}

fn backoff_secs(retry: usize) -> u64 {
    let last = DISCOVERY_BACKOFF_SECS.len() - 1;
    DISCOVERY_BACKOFF_SECS[retry.min(last)]
}

#[derive(Debug, PartialEq, Eq)]
enum AccountAction {
    /// Current key already registered under this account id.
    Keep(String),
    /// No account has the current key; move it onto this existing account.
    Rotate(String),
    /// Nothing to reuse; create a new account.
    Create,
}

#[derive(Debug, PartialEq, Eq)]
struct AccountPlan {
    action: AccountAction,
    /// Enabled accounts still holding an outdated key (they would only 401).
    disable: Vec<String>,
}

/// `accounts` = `(id, apiKey, enabled)` in priority order.
fn plan_account(accounts: &[(String, String, bool)], key: &str) -> AccountPlan {
    let keyed: Vec<&(String, String, bool)> =
        accounts.iter().filter(|(_, api_key, _)| !api_key.is_empty()).collect();
    let action = if let Some((id, _, _)) = keyed.iter().find(|(_, api_key, _)| api_key == key) {
        AccountAction::Keep(id.clone())
    } else if let Some((id, _, _)) = keyed.first() {
        AccountAction::Rotate(id.clone())
    } else {
        AccountAction::Create
    };
    let chosen = match &action {
        AccountAction::Keep(id) | AccountAction::Rotate(id) => Some(id.as_str()),
        AccountAction::Create => None,
    };
    let disable = keyed
        .iter()
        .filter(|(id, api_key, enabled)| *enabled && api_key != key && Some(id.as_str()) != chosen)
        .map(|(id, _, _)| id.clone())
        .collect();
    AccountPlan { action, disable }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acc(id: &str, key: &str, enabled: bool) -> (String, String, bool) {
        (id.to_string(), key.to_string(), enabled)
    }

    #[test]
    fn first_start_creates_account() {
        let plan = plan_account(&[], "k1");
        assert_eq!(plan, AccountPlan { action: AccountAction::Create, disable: vec![] });
    }

    #[test]
    fn restart_with_same_key_is_a_no_op() {
        let plan = plan_account(&[acc("a", "k1", true)], "k1");
        assert_eq!(plan, AccountPlan { action: AccountAction::Keep("a".into()), disable: vec![] });
    }

    #[test]
    fn rotated_key_reuses_the_existing_account() {
        let plan = plan_account(&[acc("a", "old", true)], "new");
        assert_eq!(plan, AccountPlan { action: AccountAction::Rotate("a".into()), disable: vec![] });
    }

    #[test]
    fn stale_enabled_keys_are_disabled_but_disabled_ones_left_alone() {
        let accounts = [acc("a", "new", true), acc("b", "old", true), acc("c", "older", false)];
        let plan = plan_account(&accounts, "new");
        assert_eq!(plan.action, AccountAction::Keep("a".into()));
        assert_eq!(plan.disable, vec!["b".to_string()]);
    }

    #[test]
    fn rotation_with_several_stale_accounts_keeps_the_first_and_disables_the_rest() {
        let plan = plan_account(&[acc("a", "o1", true), acc("b", "o2", true)], "new");
        assert_eq!(plan.action, AccountAction::Rotate("a".into()));
        assert_eq!(plan.disable, vec!["b".to_string()]);
    }

    #[test]
    fn keyless_accounts_are_never_touched() {
        let plan = plan_account(&[acc("x", "", true)], "k1");
        assert_eq!(plan, AccountPlan { action: AccountAction::Create, disable: vec![] });
    }

    #[test]
    fn provider_matching_needs_both_name_and_url() {
        let url = "http://opencode2api:8080/v1";
        let list = vec![
            json!({"id": "custom-1", "name": "Other", "baseUrl": url}),
            json!({"id": "custom-2", "name": NAME, "baseUrl": "http://127.0.0.1:8080/v1"}),
            json!({"id": "custom-3", "name": NAME, "baseUrl": url}),
        ];
        let hit = find_provider(&list, url).unwrap();
        assert_eq!(hit["id"], "custom-3");
        assert!(find_provider(&list[..2], url).is_none());
    }

    #[test]
    fn only_empty_or_legacy_catalogue_is_provisional() {
        assert!(is_provisional_catalogue(&[], true));
        assert!(is_provisional_catalogue(&[json!({"id": "sota", "enabled": true})], true));
        assert!(!is_provisional_catalogue(&[json!({"id": "sota"})], false));
        assert!(!is_provisional_catalogue(&[json!({"id": "glm-5.3"})], true));
        assert!(!is_provisional_catalogue(&[json!({"id": "sota"}), json!({"id": "x"})], true));
        assert!(!is_provisional_catalogue(&[], false));
    }

    #[test]
    fn backoff_grows_then_caps() {
        assert_eq!(backoff_secs(0), 2);
        assert_eq!(backoff_secs(6), 120);
        assert_eq!(backoff_secs(50), 120);
        let total: u64 = (0..DISCOVERY_ATTEMPTS - 1).map(backoff_secs).sum();
        assert!(total >= 20 * 60 && total <= 40 * 60, "total backoff {total}s");
    }
}
