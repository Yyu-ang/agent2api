//! Tabbit 模型目录：静态兜底 + 远程刷新。
//!
//! ── 上游长什么样（tabbit2api 实测）───────────────────────────
//! `GET {base}/proxy/v1/model_config/models?a=0&scene=chat`（带 HMAC 签名，
//! 空 body 签名）→ `{models: [...]}`。`selected_model` 必须传模型列表里的
//! `display_name`（发 slug 会被服务端自动切换模型），因此目录条目保留
//! `display_name` 与 `model_access_type`（免费 `free_unlimited` 等）。
//!
//! ── 模型名的板块前缀 ────────────────────────────────────────
//! 参考实现用 `cn/glm-5.3` / `intl/gpt-6-luna` 区分两板块 14 个同名模型。
//! 拆家后地区是 provider 身份：本家目录只收**本地区**的模型，条目 id 仍保留
//! `cn/` / `intl/` 前缀形态（客户端按参考习惯点名时能直接命中）；
//! `resolve_model` 同时接受裸名（`glm-5.3` → 本地区的同名模型）与
//! `默认`/`default` 别名（→ 本地区第一个 `free_unlimited` 模型）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::{Value, json};

use crate::server::core::account_store::AccountStore;

use super::credentials;
use super::region::Region;

/// 远程目录缓存有效期（10 分钟，与 kuku / raccoon 同值）
const CATALOG_CACHE_TTL_MS: i64 = 10 * 60_000;

/// 目录请求超时
const CATALOG_TIMEOUT_MS: u64 = 15_000;

/// 静态兜底清单（tabbit2api README：国内 23 + 国际 33；这里只列每地区有把握的
/// 一批，远程刷新会覆盖）。
const FALLBACK_CN: [&str; 8] = [
    "默认",
    "GLM-5.3",
    "Kimi-K3",
    "DeepSeek-V4-Flash",
    "Qwen3-Max",
    "Doubao-Seed-1.8",
    "Hunyuan-Turbo",
    "MiniMax-M2.1",
];

const FALLBACK_INTL: [&str; 8] = [
    "默认",
    "GPT-6-Luna",
    "Claude-Opus-5.5",
    "Gemini-3.5-Pro",
    "GLM-5.3",
    "Kimi-K3",
    "DeepSeek-V4-Flash",
    "Grok-4.1",
];

struct RemoteCatalog {
    models: Vec<Value>,
    fetched_at_ms: i64,
}

/// 进程级远程目录缓存（按地区各一格；不落盘，重启后回落静态兜底）
static REMOTE: Mutex<Option<HashMap<String, RemoteCatalog>>> = Mutex::new(None);

fn remote_guard() -> std::sync::MutexGuard<'static, Option<HashMap<String, RemoteCatalog>>> {
    match REMOTE.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn region_key(region: Region) -> &'static str {
    region.provider_id()
}

/// 模型清单（远程优先，未拉到 / 未刷过用静态兜底）。
pub fn list(region: Region) -> Vec<Value> {
    let guard = remote_guard();
    if let Some(catalog) = guard.as_ref().and_then(|map| map.get(region_key(region))) {
        if !catalog.models.is_empty() {
            return catalog.models.clone();
        }
    }
    drop(guard);
    fallback(region).iter().map(|id| listing_entry(region, id)).collect()
}

/// 一条清单条目（聚合层 `models::list_item` 的口径：id + name + 透传字段）。
///
/// id 取 `cn/<display_name>` 形态（与参考实现的对外模型 ID 一致，客户端按
/// 参考习惯点名可直接命中）；`display_name` 原样保留（转发时上游要它）。
fn listing_entry(region: Region, display_name: &str) -> Value {
    let prefix = match region {
        Region::Cn => "cn",
        Region::Intl => "intl",
    };
    json!({
        "id": format!("{prefix}/{display_name}"),
        "name": display_name,
        "display_name": display_name,
    })
}

fn fallback(region: Region) -> &'static [&'static str] {
    match region {
        Region::Cn => &FALLBACK_CN,
        Region::Intl => &FALLBACK_INTL,
    }
}

/// 把客户端给的模型名解析成上游 `display_name`。
///
/// 接受：`cn/GLM-5.3`（前缀形态，原样取后半）/ `GLM-5.3`（裸名，本地区精确
/// 匹配）/ `默认` / `default`（→ 本地区第一个 `free_unlimited` 模型）。
/// 找不到 → 返回原名（上游对未知模型的行为是「自动切换」，由上游决定；
/// 不在这里静默改名，见模块头）。
pub fn resolve_model(region: Region, client_model: &str) -> String {
    let name = client_model.trim();
    // 剥板块前缀（`cn/` / `intl/`，大小写不敏感；参考 `_PREFIX_ALIASES` 的子集）
    let bare = match name.split_once('/') {
        Some((head, tail)) => {
            let lower = head.trim().to_ascii_lowercase();
            if matches!(lower.as_str(), "cn" | "china" | "domestic" | "intl" | "international" | "overseas" | "global")
                && !tail.trim().is_empty()
            {
                tail.trim()
            } else {
                name
            }
        }
        None => name,
    };
    // 默认别名 → 本地区第一个 free_unlimited
    let lower = bare.to_ascii_lowercase();
    if matches!(lower.as_str(), "默认" | "default" | "best" | "free") {
        if let Some(display) = first_free_unlimited(region) {
            return display;
        }
        return bare.to_string();
    }
    // 本地区精确匹配（display_name 或 id 后半）
    for item in list(region) {
        let display = item.get("display_name").and_then(Value::as_str).unwrap_or("");
        if display.eq_ignore_ascii_case(bare) {
            return display.to_string();
        }
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            if let Some((_, tail)) = id.split_once('/') {
                if tail.eq_ignore_ascii_case(bare) {
                    return display.to_string();
                }
            }
        }
    }
    bare.to_string()
}

fn first_free_unlimited(region: Region) -> Option<String> {
    for item in list(region) {
        if item
            .get("model_access_type")
            .and_then(Value::as_str)
            .map(|access| access == "free_unlimited")
            .unwrap_or(false)
        {
            if let Some(display) = item.get("display_name").and_then(Value::as_str) {
                return Some(display.to_string());
            }
        }
    }
    // 兜底清单没有 access_type 字段时，取第一条
    list(region)
        .into_iter()
        .next()
        .and_then(|item| {
            item.get("display_name")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

/// 远程目录是否拉到过。
pub fn remote_refreshed(region: Region) -> bool {
    let guard = remote_guard();
    guard
        .as_ref()
        .and_then(|map| map.get(region_key(region)))
        .map(|catalog| !catalog.models.is_empty())
        .unwrap_or(false)
}

/// 最后一次远程刷新的时刻（毫秒；没刷过给 0）。
pub fn last_refreshed_at(region: Region) -> i64 {
    let guard = remote_guard();
    guard
        .as_ref()
        .and_then(|map| map.get(region_key(region)))
        .map(|catalog| catalog.fetched_at_ms)
        .unwrap_or(0)
}

/// 刷新远程目录（`force = true` 跳过 TTL；失败不返回错误，保留现有清单）。
pub async fn refresh(
    region: Region,
    store: &AccountStore,
    account_id: &str,
    force: bool,
) -> crate::server::core::providers::adapter::ModelRefreshOutcome {
    use crate::server::core::providers::adapter::ModelRefreshOutcome;
    let credentials = match credentials::snapshot_for(region, store, account_id) {
        Ok(credentials) => credentials,
        Err(error) => {
            crate::server::logging::verbose(
                "[Models]",
                &format!("Tabbit（{}）模型目录刷新跳过：{}", region.label(), error.message),
            );
            return ModelRefreshOutcome::unchanged();
        }
    };
    let account = store.tabbit_account_record(region.provider_id(), account_id);
    let resolution = crate::server::core::proxies::resolve_account_proxy(
        account.as_ref().and_then(|record| record.get("proxy")),
    );
    let proxy = resolution.as_ref().and_then(|item| item.resolved());
    if !force {
        let now = crate::server::logging::now_ms();
        let guard = remote_guard();
        if let Some(catalog) = guard.as_ref().and_then(|map| map.get(region_key(region))) {
            if now - catalog.fetched_at_ms < CATALOG_CACHE_TTL_MS {
                return ModelRefreshOutcome::unchanged();
            }
        }
        drop(guard);
    }
    match fetch_remote(region, &credentials, proxy).await {
        Ok(models) => {
            let now = crate::server::logging::now_ms();
            let mut guard = remote_guard();
            let map = guard.get_or_insert_with(HashMap::new);
            let catalog = map.entry(region_key(region).to_string()).or_insert(RemoteCatalog {
                models: Vec::new(),
                fetched_at_ms: 0,
            });
            catalog.models = models.clone();
            catalog.fetched_at_ms = now;
            drop(guard);
            crate::server::logging::log(
                "[Models]",
                &format!("✅ Tabbit（{}）模型目录已更新（{} 个）", region.label(), models.len()),
            );
            ModelRefreshOutcome::refreshed(models.len())
        }
        Err(error) => {
            crate::server::logging::verbose(
                "[Models]",
                &format!("Tabbit（{}）模型目录刷新失败：{}", region.label(), error.message),
            );
            ModelRefreshOutcome::failed(error.message)
        }
    }
}

/// 拉一次远程目录（`GET /proxy/v1/model_config/models?a=0&scene=chat`）。
async fn fetch_remote(
    region: Region,
    credentials: &super::credentials::TabbitCredentials,
    proxy: Option<&crate::server::core::proxies::ResolvedProxy>,
) -> Result<Vec<Value>, crate::server::errors::GatewayError> {
    let sign_key =
        super::protocol::ensure_sign_key(region, credentials, proxy, false).await;
    let url = format!(
        "{}/proxy/v1/model_config/models?a=0&scene=chat",
        region.base_url()
    );
    // 空 body 签名（参考实现 `_signature("")`）
    let mut headers = super::protocol::base_headers(region, credentials, "/newtab");
    headers.push(("Accept".to_string(), "application/json".to_string()));
    headers.extend(super::protocol::signature_headers(&sign_key, ""));
    let mut value =
        super::http::get_json_value(&url, &headers, proxy, Some(CATALOG_TIMEOUT_MS)).await;
    // 499 → 刷新 key 重试一次（参考实现同逻辑）
    if let Err(error) = &value {
        if error.status_code == 499 {
            let fresh =
                super::protocol::ensure_sign_key(region, credentials, proxy, true).await;
            let mut retry_headers =
                super::protocol::base_headers(region, credentials, "/newtab");
            retry_headers.push(("Accept".to_string(), "application/json".to_string()));
            retry_headers.extend(super::protocol::signature_headers(&fresh, ""));
            value = super::http::get_json_value(&url, &retry_headers, proxy, Some(CATALOG_TIMEOUT_MS))
                .await;
        }
    }
    let value = value?;
    let models = value
        .get("models")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(models.len());
    for model in models {
        let display = model.get("display_name").and_then(Value::as_str).unwrap_or("");
        if display.trim().is_empty() {
            continue;
        }
        let mut entry = listing_entry(region, display.trim());
        // 对外保留能力、会员权益和倍率，供统一模型目录和路由层使用。
        for key in [
            "model_access_type", "supports_images", "supports_tools",
            "support_thinking", "display_multiplier",
        ] {
            if let Some(value) = model.get(key) { entry[key] = value.clone(); }
        }
        out.push(entry);
    }
    Ok(out)
}
