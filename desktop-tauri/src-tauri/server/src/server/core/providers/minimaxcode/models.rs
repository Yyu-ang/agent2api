//! MiniMax Code 模型目录：**静态兜底清单 + 上游实时清单**（两级，与小浣熊同款）。
//!
//! ── 两级结构 ────────────────────────────────────────────────
//!   1. **静态兜底** [`fallback_models`]：插件自带的 4 个模型（M3.1-Flash-
//!      Preview / M3 / M2.7 / M2.7-highspeed）。没有账号、刷新还没跑、
//!      上游挂了的任何时刻都有清单可用，条目形状与实时解析**同一个
//!      归一函数出口**，前端无从区分来源；
//!   2. **实时清单**：`GET {llm}/mavis/api/v1/models?region=<cn|en>&buildEnv=prod`
//!      （Bearer；401/403 时**无鉴权重试一次**——该接口匿名可读，插件
//!      `liveModels` 同款），解析 `providers[]` 里 `providerId == "minimax"`
//!      的 `config.models`，顺序按 `config.model_order` 优先、其余按对象
//!      键序补尾（插件 `parseModels` 逐条对齐）。
//!
//! ── 归一条目形状 ────────────────────────────────────────────
//! 走全仓目录条目形状（`core::models::shape::list_item` 消费）：
//! `id` / `name` / `maxInputTokens`（context）/ `maxOutputTokens`（output）/
//! `supportsImages` / `supportsReasoning` / `supportsToolCall: true`，
//! 另带 `efforts`（思考强度档位，`model_meta` 的 LEVEL_KEYS 会读）与
//! `thinkingMode`（插件的思考形态标注，目录外不消费、留作排障）。
//! 思考形态判定（插件逐条）：`thinking_config.mode` 为 `switchable` →
//! 可开关；`forced_off` → 关；`forced_on` → 强制开；其余看 `reasoning`
//! 字段（false → 关，其余 → 开）。`supportsReasoning` = 思考形态 ≠ 关。
//!
//! ── 进程状态与落盘缓存 ──────────────────────────────────────
//! 每个地区一格进程状态（`OnceLock<RwLock>`），首次读时从
//! `catalog_cache`（scope `minimaxCode` / `minimaxCodeGlobal`）恢复；
//! 刷新成功先落盘再写内存（与小浣熊同款次序：进程崩在中间也不会出现
//! 「内存有、磁盘没有」的倒挂）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：绝不 unwrap/expect/panic；锁一律「毒化当空
//! 处理」。

use std::sync::{OnceLock, RwLock};

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::auth_http::{self, ApiResponse};
use crate::server::core::providers::adapter::ModelRefreshOutcome;
use crate::server::core::providers::catalog_cache;
use crate::server::logging;

use super::refresh;
use super::region::Region;

/// 目录 TTL：手点「获取模型」之外，10 分钟内不重复打上游（与小浣熊同档）
const CATALOG_TTL_MS: i64 = 10 * 60 * 1000;
/// 拉实时清单的超时（插件 `liveModels` 的 10 秒同档）
const FETCH_TIMEOUT_MS: u64 = 10_000;

/// 一个地区的目录进程状态
#[derive(Clone, Default)]
struct CatalogState {
    /// 已归一的条目（空 = 还没有实时清单，用静态兜底）
    models: Vec<Value>,
    /// 最近一次成功刷新的时刻（毫秒）
    fetched_at: i64,
}

fn state_cell(region: Region) -> &'static RwLock<CatalogState> {
    static CN: OnceLock<RwLock<CatalogState>> = OnceLock::new();
    static GLOBAL: OnceLock<RwLock<CatalogState>> = OnceLock::new();
    let cell = match region {
        Region::Cn => CN.get_or_init(|| RwLock::new(CatalogState::default())),
        Region::Global => GLOBAL.get_or_init(|| RwLock::new(CatalogState::default())),
    };
    cell
}

/// 读当前状态（首次读时从落盘缓存恢复；毒化锁当空处理）
fn current_state(region: Region) -> CatalogState {
    let cell = state_cell(region);
    {
        let guard = match cell.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !guard.models.is_empty() {
            return guard.clone();
        }
    }
    // 进程状态为空：试落盘缓存（上次成功刷新的清单）。恢复成功就回填。
    if let Some(cached) = catalog_cache::load(scope_of(region)) {
        let restored = CatalogState {
            models: cached.models,
            fetched_at: cached.fetched_at,
        };
        if let Ok(mut guard) = cell.write() {
            if guard.models.is_empty() {
                *guard = restored.clone();
            }
            return guard.clone();
        }
        return restored;
    }
    CatalogState::default()
}

/// 地区的 catalog_cache scope
fn scope_of(region: Region) -> &'static str {
    match region {
        Region::Cn => catalog_cache::SCOPE_MINIMAX_CODE,
        Region::Global => catalog_cache::SCOPE_MINIMAX_CODE_GLOBAL,
    }
}

/// 适配器 `list_models` 的数据源：实时清单优先（进程内存 → 落盘缓存），
/// 都没有才用静态兜底。
pub fn list(region: Region) -> Vec<Value> {
    let state = current_state(region);
    if state.models.is_empty() {
        fallback_models()
    } else {
        state.models
    }
}

/// 是否已有远程来源的清单（`catalog::refresh_meta` 读它画「来源」列）
pub fn remote_refreshed(region: Region) -> bool {
    !current_state(region).models.is_empty()
}

/// 最近一次成功刷新的时刻（毫秒；没刷过为 0）
pub fn last_refreshed_at(region: Region) -> i64 {
    current_state(region).fetched_at
}

/// 静态兜底清单（插件 `MODELS`，顺序同插件）。
pub fn fallback_models() -> Vec<Value> {
    vec![
        spec(
            "MiniMax-M3.1-Flash-Preview",
            Some("M3.1-Flash-Preview"),
            512_000,
            128_000,
            true,
            "forced_on",
            &["low", "medium", "high", "xhigh", "max"],
        ),
        spec(
            "MiniMax-M3",
            None,
            512_000,
            128_000,
            true,
            "switchable",
            &[],
        ),
        spec("MiniMax-M2.7", None, 200_000, 128_000, false, "on", &[]),
        spec(
            "MiniMax-M2.7-highspeed",
            None,
            200_000,
            128_000,
            false,
            "on",
            &[],
        ),
    ]
}

/// 一条兜底规格 → 归一条目（与 `parse_models` 共用同一出口形状）
fn spec(
    id: &str,
    name: Option<&str>,
    context: i64,
    output: i64,
    images: bool,
    thinking_mode: &str,
    efforts: &[&str],
) -> Value {
    entry(
        id,
        name.unwrap_or(id),
        context,
        output,
        images,
        thinking_mode,
        efforts.iter().map(|effort| effort.to_string()).collect(),
    )
}

/// 归一条目的唯一出口（兜底与实时解析共用，保证形状一致）
fn entry(
    id: &str,
    name: &str,
    context: i64,
    output: i64,
    images: bool,
    thinking_mode: &str,
    efforts: Vec<String>,
) -> Value {
    json!({
        "id": id,
        "name": name,
        "maxInputTokens": context,
        "maxOutputTokens": output,
        "supportsImages": images,
        "supportsReasoning": thinking_mode != "off",
        "supportsToolCall": true,
        "efforts": efforts,
        "thinkingMode": thinking_mode,
    })
}

/// 解析上游 `/mavis/api/v1/models` 响应（插件 `parseModels` 逐条对齐，纯函数）。
///
/// 结构：`providers[]` 里找 `providerId == "minimax"`，取它的
/// `config.models`（对象，键 = 模型 id）与 `config.model_order`（顺序表）。
/// 找不到该 provider / models 不是对象 → 空 Vec（调用方按「目录为空」
/// 失败处理，不拿空清单覆盖旧清单）。
pub fn parse_models(body: &Value) -> Vec<Value> {
    let Some(providers) = body.get("providers").and_then(Value::as_array) else {
        return Vec::new();
    };
    let Some(provider) = providers
        .iter()
        .find(|item| item.get("providerId").and_then(Value::as_str) == Some("minimax"))
    else {
        return Vec::new();
    };
    let config = provider.get("config").cloned().unwrap_or(Value::Null);
    let Some(models) = config.get("models").and_then(Value::as_object) else {
        return Vec::new();
    };
    // 顺序：model_order 里仍存在的 id 在前，其余按对象键序补尾
    let mut ids: Vec<String> = Vec::new();
    if let Some(order) = config.get("model_order").and_then(Value::as_array) {
        for item in order {
            if let Some(id) = item.as_str() {
                if models.contains_key(id) && !ids.iter().any(|seen| seen == id) {
                    ids.push(id.to_string());
                }
            }
        }
    }
    for id in models.keys() {
        if !ids.contains(id) {
            ids.push(id.clone());
        }
    }
    let mut out = Vec::new();
    for id in ids {
        let Some(definition) = models.get(&id) else {
            continue;
        };
        let Some(parsed) = parse_model_entry(&id, definition) else {
            continue;
        };
        out.push(parsed);
    }
    out
}

/// 一条模型定义的归一（`definition` 不是对象 / id 为空 → None 跳过）
fn parse_model_entry(id: &str, definition: &Value) -> Option<Value> {
    if id.trim().is_empty() || !definition.is_object() {
        return None;
    }
    let name = definition
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(id);
    let context = number_at(definition, &["limit", "context"]).unwrap_or(0.0) as i64;
    let output = number_at(definition, &["limit", "output"]).unwrap_or(0.0) as i64;
    let images = definition
        .get("modalities")
        .and_then(|modalities| modalities.get("input"))
        .and_then(Value::as_array)
        .is_some_and(|inputs| inputs.iter().any(|item| item.as_str() == Some("image")))
        || definition.get("attachment").and_then(Value::as_bool) == Some(true);
    let mode = definition
        .get("thinking_config")
        .and_then(|config| config.get("mode"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let thinking_mode = match mode {
        "switchable" => "switchable",
        "forced_off" => "off",
        "forced_on" => "forced_on",
        _ => {
            if definition.get("reasoning").and_then(Value::as_bool) == Some(false) {
                "off"
            } else {
                "on"
            }
        }
    };
    // efforts 来源有四处候选键（插件逐条）：thinking.effortOptions /
    // thinking.effort_options / effort_options / effortOptions；过滤空串
    // 与 default / none（那是「不指定」的占位档，不是真档位）
    let mut efforts: Vec<String> = Vec::new();
    for candidate in [
        definition
            .get("thinking")
            .and_then(|thinking| thinking.get("effortOptions")),
        definition
            .get("thinking")
            .and_then(|thinking| thinking.get("effort_options")),
        definition.get("effort_options"),
        definition.get("effortOptions"),
    ] {
        if let Some(list) = candidate.and_then(Value::as_array) {
            for item in list {
                if let Some(effort) = item.as_str() {
                    let effort = effort.trim();
                    if !effort.is_empty()
                        && effort != "default"
                        && effort != "none"
                        && !efforts.iter().any(|seen| seen == effort)
                    {
                        efforts.push(effort.to_string());
                    }
                }
            }
            if !efforts.is_empty() {
                break;
            }
        }
    }
    Some(entry(
        id,
        name,
        context,
        output,
        images,
        thinking_mode,
        efforts,
    ))
}

/// 嵌套数字取值（插件 `num()`：有限数才算数，缺失给 None）
fn number_at(value: &Value, path: &[&str]) -> Option<f64> {
    let mut current = value;
    for key in path {
        current = current.get(key)?;
    }
    current.as_f64().filter(|number| number.is_finite())
}

/// 刷新目录（适配器 `refresh_models` 的实现）。
///
/// 流程：TTL 内（非强制）直接跳过 → 拿凭证（该地区任一账号的可用
/// token，拿不到就匿名）→ GET 实时清单（401/403 时无鉴权重试一次）→
/// 解析为空按失败处理（保留旧清单）→ 成功先落盘再写内存。
pub async fn refresh(
    store: &AccountStore,
    region: Region,
    account_id: &str,
    force: bool,
) -> ModelRefreshOutcome {
    let state = current_state(region);
    let now = crate::server::logging::now_ms();
    if !force && !state.models.is_empty() && now - state.fetched_at < CATALOG_TTL_MS {
        return ModelRefreshOutcome::unchanged();
    }
    // 凭证尽力拿：适配器 refresh_models 的语义里账号只是目录来源之一，
    // token 拿不到（没账号 / 续期暂态失败）不等于刷新失败 —— 该接口匿名
    // 可读（插件 liveModels 的 401/403 无鉴权重试就是为此）。
    let token = match refresh::ensure_fresh(store, region, account_id, false).await {
        Ok(credentials) => Some(credentials.access_token),
        Err(_) => match refresh::snapshot(store, region, account_id) {
            Ok(credentials) => Some(credentials.access_token),
            Err(_) => None,
        },
    };
    let url = format!(
        "{}/mavis/api/v1/models?region={}&buildEnv=prod",
        region.llm_base(),
        region.region_param()
    );
    let response = match fetch_catalog(&url, token.as_deref()).await {
        Ok(response) => response,
        Err(error) => return ModelRefreshOutcome::failed(error),
    };
    if !response.ok {
        return ModelRefreshOutcome::failed(format!(
            "MiniMax 模型目录接口返回 HTTP {}",
            response.status
        ));
    }
    let Some(payload) = response.payload else {
        return ModelRefreshOutcome::failed("MiniMax 模型目录响应不是 JSON");
    };
    let models = parse_models(&payload);
    if models.is_empty() {
        return ModelRefreshOutcome::failed("上游返回的模型目录为空");
    }
    let count = models.len();
    let fetched_at = crate::server::logging::now_ms();
    // 先落盘再写内存（与小浣熊同款次序）
    catalog_cache::save(scope_of(region), &models, fetched_at);
    if let Ok(mut guard) = state_cell(region).write() {
        guard.models = models;
        guard.fetched_at = fetched_at;
    }
    logging::log(
        "[MiniMaxCode]",
        &format!("{} 模型目录已刷新: {count} 个", region.site_name()),
    );
    ModelRefreshOutcome::refreshed(count)
}

/// 拉一次目录：带 Bearer 打；401/403 且带了 Bearer 时无鉴权重试一次
/// （插件 `liveModels` 同款）。
async fn fetch_catalog(url: &str, token: Option<&str>) -> Result<ApiResponse, String> {
    let with_auth: Vec<(String, String)> = match token {
        Some(token) if !token.trim().is_empty() => vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("Authorization".to_string(), format!("Bearer {token}")),
        ],
        _ => vec![("Accept".to_string(), "application/json".to_string())],
    };
    let first = send(url, &with_auth).await?;
    if (first.status == 401 || first.status == 403) && token.is_some_and(|t| !t.trim().is_empty()) {
        let anonymous = vec![("Accept".to_string(), "application/json".to_string())];
        return send(url, &anonymous).await;
    }
    Ok(first)
}

async fn send(url: &str, headers: &[(String, String)]) -> Result<ApiResponse, String> {
    auth_http::send_raw("GET", url, None, headers, None, Some(FETCH_TIMEOUT_MS))
        .await
        .map_err(|error| format!("MiniMax 模型目录请求失败: {}", egress_detail(&error)))
}

fn egress_detail(error: &reqwest::Error) -> String {
    crate::server::core::egress::describe_error_detail(error)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn fallback_has_four_models_in_plugin_order() {
        let models = fallback_models();
        let ids: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("id").and_then(Value::as_str))
            .collect();
        assert_eq!(
            ids,
            vec![
                "MiniMax-M3.1-Flash-Preview",
                "MiniMax-M3",
                "MiniMax-M2.7",
                "MiniMax-M2.7-highspeed"
            ]
        );
        assert_eq!(models[0]["maxInputTokens"], 512_000);
        assert_eq!(models[0]["supportsImages"], true);
        assert_eq!(models[0]["supportsReasoning"], true);
        assert_eq!(models[0]["efforts"][4], "max");
        assert_eq!(models[2]["maxInputTokens"], 200_000);
        assert_eq!(models[2]["supportsImages"], false);
    }

    #[test]
    fn parse_models_follows_model_order_and_reads_capabilities() {
        // 插件同形态：providers 里夹着别家、minimax 的 config.models + model_order
        let body = json!({
            "providers": [
                { "providerId": "other", "config": { "models": { "x": {} } } },
                {
                    "providerId": "minimax",
                    "config": {
                        "model_order": ["MiniMax-M3", "MiniMax-M3.1-Flash-Preview", "gone"],
                        "models": {
                            "MiniMax-M3.1-Flash-Preview": {
                                "name": "M3.1-Flash-Preview",
                                "limit": { "context": 512000, "output": 128000 },
                                "modalities": { "input": ["text", "image"] },
                                "thinking_config": { "mode": "forced_on" },
                                "thinking": { "effortOptions": ["low", "default", "none", "max"] },
                            },
                            "MiniMax-M3": {
                                "limit": { "context": 512000, "output": 128000 },
                                "attachment": true,
                                "thinking_config": { "mode": "switchable" },
                            },
                            "MiniMax-M2.7": {
                                "limit": { "context": 200000, "output": 128000 },
                                "reasoning": false,
                            },
                        },
                    },
                },
            ],
        });
        let models = parse_models(&body);
        let ids: Vec<&str> = models
            .iter()
            .filter_map(|model| model.get("id").and_then(Value::as_str))
            .collect();
        // model_order 优先（gone 不存在被跳过），其余按对象键序补尾
        assert_eq!(
            ids,
            vec!["MiniMax-M3", "MiniMax-M3.1-Flash-Preview", "MiniMax-M2.7"]
        );
        let flash = &models[1];
        assert_eq!(flash["name"], "M3.1-Flash-Preview");
        assert_eq!(flash["supportsImages"], true);
        assert_eq!(flash["thinkingMode"], "forced_on");
        // efforts 过滤 default / none
        assert_eq!(flash["efforts"], json!(["low", "max"]));
        // attachment=true 也算支持图片；switchable 算支持思考
        assert_eq!(models[0]["supportsImages"], true);
        assert_eq!(models[0]["supportsReasoning"], true);
        // reasoning=false → 思考关
        assert_eq!(models[2]["supportsReasoning"], false);
    }

    #[test]
    fn parse_models_without_minimax_provider_is_empty() {
        assert!(parse_models(&json!({ "providers": [] })).is_empty());
        assert!(parse_models(&json!({})).is_empty());
        assert!(parse_models(&json!({
            "providers": [{ "providerId": "minimax", "config": {} }]
        }))
        .is_empty());
    }
}
