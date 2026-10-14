//! TeleAgent 模型目录：`GET /provider` 远程目录 + 静态兜底。
//!
//! ── 移植自 ────────────────────────────────────────────────────
//! `teleagent-openai-proxy/openai_proxy.py` 的 `_handle_api_models()` /
//! `_handle_models()`（MIT，原作者 lizhun781021）：
//!   - `GET /provider` → `data.all[]`，**只收 `source == "config"` 的
//!     provider**（有密钥的已配置项；内置未配置的不展示）；
//!   - 模型 id 形如 `{providerID}/{modelID}`（如 `NewApi/chat-pro`）；
//!   - 拉不到时回落默认两个：`NewApi/chat-pro`、`NewApi/chat-flash`
//!     （原实现 `_handle_models` 的兜底）。
//!
//! ── 与 kuku 的 `models.rs` 同结构 ────────────────────────────
//! 远程优先 + 静态兜底 + 10 分钟 TTL（`force = true` 跳过）；
//! `resolve_model` 把客户端请求的模型名归一成 `(provider_id, model_id)`。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{Value, json};

use crate::server::errors::GatewayError;

use super::protocol::signed_json_with_key;
use super::{DEFAULT_MODEL_ID, DEFAULT_PROVIDER_ID};

/// 远程目录缓存有效期（10 分钟，与 kuku / raccoon 同值）
const CATALOG_CACHE_TTL_MS: i64 = 10 * 60_000;
/// 目录请求超时（原实现 `_handle_api_models` 10 秒）
const CATALOG_TIMEOUT_MS: u64 = 10_000;

/// 静态兜底清单（原实现 `_handle_models` 拉不到时的默认值）
const FALLBACK_MODELS: [(&str, &str); 2] = [
    (DEFAULT_PROVIDER_ID, DEFAULT_MODEL_ID),
    (DEFAULT_PROVIDER_ID, "chat-flash"),
];

struct RemoteCatalog {
    models: Vec<Value>,
    fetched_at_ms: i64,
}

/// 进程级远程目录缓存（kuku 同款：不落盘，重启回落静态兜底）
static REMOTE: Mutex<Option<RemoteCatalog>> = Mutex::new(None);

fn remote_guard() -> std::sync::MutexGuard<'static, Option<RemoteCatalog>> {
    match REMOTE.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// 模型清单（远程优先，未拉到 / 未刷过用静态兜底）。
pub fn list() -> Vec<Value> {
    let guard = remote_guard();
    if let Some(catalog) = guard.as_ref() {
        if !catalog.models.is_empty() {
            return catalog.models.clone();
        }
    }
    drop(guard);
    fallback_list()
}

/// 静态兜底条目。
fn fallback_list() -> Vec<Value> {
    FALLBACK_MODELS
        .iter()
        .map(|(provider_id, model_id)| listing_entry(provider_id, model_id, model_id))
        .collect()
}

/// 一条清单条目（聚合层 `models::list_item` 的口径：id + name）。
fn listing_entry(provider_id: &str, model_id: &str, name: &str) -> Value {
    json!({
        "id": format!("{provider_id}/{model_id}"),
        "name": if name.is_empty() { model_id.to_string() } else { name.to_string() },
    })
}

/// 远程目录是否拉到过（`catalog::refresh_meta` 的「来源」列判据）。
pub fn remote_refreshed() -> bool {
    let guard = remote_guard();
    guard
        .as_ref()
        .map(|catalog| !catalog.models.is_empty())
        .unwrap_or(false)
}

/// 最后一次远程刷新的时刻（毫秒；没刷过给 0）。
pub fn last_refreshed_at() -> i64 {
    let guard = remote_guard();
    guard.as_ref().map(|catalog| catalog.fetched_at_ms).unwrap_or(0)
}

/// 刷新远程目录（`force = true` 跳过 TTL；失败不返回错误，保留现有清单）。
///
/// 模型目录是全局的：`store` / `account_id` 忽略（签名 key 即凭证，
/// 见 `auth.rs`）。`refresh_uses_account() = false` 与之配对。
pub async fn refresh(
    store: &crate::server::core::account_store::AccountStore,
    account_id: &str,
    force: bool,
) -> crate::server::core::providers::adapter::ModelRefreshOutcome {
    // TTL 早退（只有自动路径会走到）
    if !force {
        let now = crate::server::logging::now_ms();
        let guard = remote_guard();
        if let Some(catalog) = guard.as_ref() {
            if now - catalog.fetched_at_ms < CATALOG_CACHE_TTL_MS {
                return crate::server::core::providers::adapter::ModelRefreshOutcome::unchanged();
            }
        }
        drop(guard);
    }
    let key = match crate::server::core::account_store::teleagent_accounts::session_key_for(store, account_id) {
        Ok(key) => key,
        Err(error) => return crate::server::core::providers::adapter::ModelRefreshOutcome::failed(error.message),
    };
    match fetch_remote(&key).await {
        Ok(models) => {
            let now = crate::server::logging::now_ms();
            let mut guard = remote_guard();
            let catalog = guard.get_or_insert_with(|| RemoteCatalog {
                models: Vec::new(),
                fetched_at_ms: 0,
            });
            catalog.models = models.clone();
            catalog.fetched_at_ms = now;
            drop(guard);
            crate::server::logging::log(
                "[Models]",
                &format!("✅ TeleAgent 模型目录已更新（{} 个）", models.len()),
            );
            crate::server::core::providers::adapter::ModelRefreshOutcome::refreshed(models.len())
        }
        Err(error) => {
            crate::server::logging::verbose(
                "[Models]",
                &format!("TeleAgent 模型目录刷新失败：{}", error.message),
            );
            crate::server::core::providers::adapter::ModelRefreshOutcome::failed(error.message)
        }
    }
}

/// 真打一次上游目录接口（`GET /provider`，原实现 `_handle_api_models` 口径）。
async fn fetch_remote(key: &str) -> Result<Vec<Value>, GatewayError> {
    let value = signed_json_with_key("GET", "/provider", None, CATALOG_TIMEOUT_MS, key).await?;
    let mut seen: HashMap<String, bool> = HashMap::new();
    let mut models: Vec<Value> = Vec::new();
    let all = value.get("all").and_then(Value::as_array);
    let empty: Vec<Value> = Vec::new();
    for provider in all.unwrap_or(&empty) {
        // 只收已配置成功的 provider（`source == "config"`，原实现口径）
        if provider.get("source").and_then(Value::as_str) != Some("config") {
            continue;
        }
        let provider_id = provider.get("id").and_then(Value::as_str).unwrap_or("");
        if provider_id.is_empty() {
            continue;
        }
        let empty_map = serde_json::Map::new();
        let provider_models = provider
            .get("models")
            .and_then(Value::as_object)
            .unwrap_or(&empty_map);
        for (model_id, info) in provider_models {
            let name = info
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(model_id);
            let full_id = format!("{provider_id}/{model_id}");
            if seen.insert(full_id.clone(), true).is_none() {
                models.push(listing_entry(provider_id, model_id, name));
            }
        }
    }
    if models.is_empty() {
        // 认不出结构 / 一个可用 provider 都没有 → 静态兜底（原实现同款）
        models = fallback_list();
    }
    Ok(models)
}

/// 把客户端请求的模型名归一成 `(provider_id, model_id)`。
///
/// 接受 `NewApi/chat-pro` / `teleagent/NewApi/chat-pro` / 裸名；
/// 未知模型显式回退默认（上游对未知名会报错，显式回退更清晰，
/// 与 kuku 的 `resolve_model` 同一取舍）。
pub fn resolve_model(name: &str) -> Result<(String, String), GatewayError> {
    let mut name = name.trim().to_string();
    if let Some(rest) = name.strip_prefix("teleagent/") {
        name = rest.trim().to_string();
    }
    if name.is_empty() {
        return Ok((DEFAULT_PROVIDER_ID.to_string(), DEFAULT_MODEL_ID.to_string()));
    }
    let known = {
        let guard = remote_guard();
        let remote_known = guard
            .as_ref()
            .map(|catalog| {
                catalog
                    .models
                    .iter()
                    .any(|m| m.get("id").and_then(Value::as_str) == Some(name.as_str()))
            })
            .unwrap_or(false);
        drop(guard);
        remote_known
            || FALLBACK_MODELS
                .iter()
                .any(|(provider_id, model_id)| format!("{provider_id}/{model_id}") == name)
    };
    if !known {
        return Err(GatewayError::with_status(400, format!("TeleAgent 未知模型：{name}，请刷新模型目录或选择可用模型")));
    }
    Ok(match name.split_once('/') {
        Some((provider_id, model_id)) if !provider_id.is_empty() && !model_id.is_empty() => {
            (provider_id.to_string(), model_id.to_string())
        }
        _ => (DEFAULT_PROVIDER_ID.to_string(), name),
    })
}
