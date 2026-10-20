//! Zed 的模型目录：`GET {CLOUD}/models` 的解析、进程内缓存（10 分钟
//! TTL）+ `catalog_cache` 落盘恢复、静态兜底 6 个，以及「每个模型归哪家
//! vendor」的记录（转发分派的依据）。
//!
//! ── 清单的归属是**账号**的，不是全局的 ────────────────────
//! 插件按账号缓存清单（组织不同、能调的模型不同）。本仓的目录设施是按
//! provider 一格（其余各家同形），因此这里保留的是**最近一次成功拉取**
//! 的那份；多账号组织不同时以最后刷新到的为准 —— 这是与仓库既有设施
//! 对齐的取舍，如实记下。vendor 归属表同理是全局一张（插件 `vendors`
//! Map 也是全局的，逐字同形）。
//!
//! ── 拉不到清单时怎么办 ────────────────────────────────────
//! 从未成功拉到过 → 用静态兜底 6 个（插件在登录前声明的那六个，逐字）；
//! 拉到过 → 保留旧清单（插件 `lists` 的既有条目在刷新失败时保留，同
//! 一处置）。兜底条目的 vendor 归属在进程首次读清单时一并登记。
//!
//! ── 条目形状 ──────────────────────────────────────────────
//! 对内保存上游形状（`ParsedModel`）；对目录出口映射成仓内清单形状
//! （`id` / `name` / `supportsToolCall` / `supportsImages` /
//! `supportsReasoning` / `maxInputTokens` / `maxOutputTokens` /
//! `reasoningLevels`）。`catalog_cache` 里存上游形状的 JSON（键名见
//! `parsed_to_json`），读回时走同一个解析器 —— 写入与读回只有一种形状。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::providers::adapter::ModelRefreshOutcome;
use crate::server::core::providers::catalog_cache;
use crate::server::logging;

use super::credentials::Credential;
use super::http;

/// 本家在 `catalog_cache` 里的 scope 名
pub const SCOPE: &str = catalog_cache::SCOPE_ZED;

/// 自动路径的缓存 TTL（10 分钟，与插件 `MODELS_TTL` 逐字、与其余各家同档）
const TTL_MS: i64 = 10 * 60 * 1000;

/// 四家 vendor（插件 `NPM` 的键集合）：不在这张表里的 provider 的模型
/// 写不出它的原生请求，如实剔除（插件 `parseModels` 同一条）。
const KNOWN_VENDORS: [&str; 4] = ["anthropic", "open_ai", "x_ai", "google"];

/// 一条目录记录（上游形状归一后）。
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedModel {
    pub id: String,
    pub display_name: String,
    pub provider: String,
    pub max_token_count: i64,
    pub max_output_tokens: i64,
    pub supports_images: bool,
    pub supports_thinking: bool,
    pub efforts: Vec<String>,
}

/// 静态兜底 6 个（插件 `MODELS` 逐字：id / 展示名 / 上下文 / 输出 /
/// 图片 / 思考；六个都支持图片与思考，effort 档位上游此处未给、留空）。
fn fallback_models() -> Vec<ParsedModel> {
    const SPECS: [(&str, &str, &str, i64, i64); 6] = [
        ("claude-sonnet-5", "Claude Sonnet 5", "anthropic", 1_000_000, 64_000),
        ("claude-opus-5-5", "Claude Opus 5.5", "anthropic", 1_000_000, 128_000),
        ("claude-haiku-4-5", "Claude Haiku 4.5", "anthropic", 200_000, 64_000),
        ("gpt-5.5", "GPT-5.5", "open_ai", 400_000, 128_000),
        ("gemini-3.5-flash", "Gemini 3.5 Flash", "google", 1_048_576, 65_536),
        ("grok-4.7", "Grok 4.7", "x_ai", 500_000, 64_000),
    ];
    SPECS
        .iter()
        .map(|(id, name, provider, context, output)| ParsedModel {
            id: (*id).to_string(),
            display_name: (*name).to_string(),
            provider: (*provider).to_string(),
            max_token_count: *context,
            max_output_tokens: *output,
            supports_images: true,
            supports_thinking: true,
            efforts: Vec::new(),
        })
        .collect()
}

struct CatalogState {
    models: Vec<ParsedModel>,
    fetched_at: i64,
}

fn state() -> &'static Mutex<Option<CatalogState>> {
    static STATE: OnceLock<Mutex<Option<CatalogState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

fn vendors() -> &'static Mutex<HashMap<String, String>> {
    static VENDORS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    VENDORS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn with_state<R>(f: impl FnOnce(&mut Option<CatalogState>) -> R) -> R {
    let mutex = state();
    let mut guard = mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if guard.is_none() {
        if let Some(cached) = catalog_cache::load(SCOPE) {
            let models: Vec<ParsedModel> = cached
                .models
                .iter()
                .filter_map(parsed_from_json)
                .collect();
            if !models.is_empty() {
                register_vendors(&models);
                *guard = Some(CatalogState {
                    models,
                    fetched_at: cached.fetched_at,
                });
            }
        }
    }
    f(&mut guard)
}

fn register_vendors(models: &[ParsedModel]) {
    let mut table = vendors()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for model in models {
        table.insert(model.id.clone(), model.provider.clone());
    }
}

/// 某模型归哪家 vendor：目录记录优先，没有记录时按名字猜（插件 `guess`
/// 逐字：claude* → anthropic、gemini* → google、grok* → x_ai、其余
/// open_ai）。
pub fn vendor_of(model: &str) -> String {
    // 兜底清单的归属也要在表里：进程还没拉过远程清单时转发就靠它
    let _ = list();
    if let Some(provider) = vendors()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(model)
        .cloned()
    {
        return provider;
    }
    let lowered = model.to_lowercase();
    if lowered.starts_with("claude") {
        "anthropic".to_string()
    } else if lowered.starts_with("gemini") {
        "google".to_string()
    } else if lowered.starts_with("grok") {
        "x_ai".to_string()
    } else {
        "open_ai".to_string()
    }
}

/// 当前清单（远程拉到了就用它，否则静态兜底），仓内清单形状。
pub fn list() -> Vec<Value> {
    let models = with_state(|slot| {
        slot.as_ref().map(|state| state.models.clone())
    });
    match models {
        Some(models) if !models.is_empty() => {
            models.iter().map(list_item).collect()
        }
        _ => {
            let fallback = fallback_models();
            register_vendors(&fallback);
            fallback.iter().map(list_item).collect()
        }
    }
}

/// 一条目录记录 → 仓内清单形状（出口只写有依据的键，与各家同一口径）。
fn list_item(model: &ParsedModel) -> Value {
    let mut item = json!({
        "id": model.id,
        "name": model.display_name,
        "supportsToolCall": true,
        "supportsImages": model.supports_images,
        "supportsReasoning": model.supports_thinking || !model.efforts.is_empty(),
    });
    if let Some(object) = item.as_object_mut() {
        if model.max_token_count > 0 {
            object.insert(
                "maxInputTokens".to_string(),
                Value::from(model.max_token_count),
            );
        }
        if model.max_output_tokens > 0 {
            object.insert(
                "maxOutputTokens".to_string(),
                Value::from(model.max_output_tokens),
            );
        }
        if !model.efforts.is_empty() {
            object.insert(
                "reasoningLevels".to_string(),
                Value::Array(
                    model
                        .efforts
                        .iter()
                        .map(|level| Value::String(level.clone()))
                        .collect(),
                ),
            );
        }
    }
    item
}

/// 这份清单是否来自远程拉取（`catalog::refresh_meta` 的「来源」判据）
pub fn remote_refreshed() -> bool {
    with_state(|slot| {
        slot.as_ref()
            .map(|state| !state.models.is_empty())
            .unwrap_or(false)
    })
}

/// 最近一次成功刷新的时刻（毫秒；没刷过为 0）
pub fn last_refreshed_at() -> i64 {
    with_state(|slot| slot.as_ref().map(|state| state.fetched_at).unwrap_or(0))
}

/// 解析 `GET /models` 的响应（插件 `parseModels` 逐字）：剔除没有 id、
/// 被 disabled、以及 provider 不在四家之内的条目；一条不剩是
/// `NothingListed` 错误（文案逐字），由调用方决定 repick 还是上报。
pub fn parse_models(text: &str) -> Result<Vec<ParsedModel>, String> {
    let parsed: Value = serde_json::from_str(text).unwrap_or(Value::Null);
    let mut out = Vec::new();
    if let Some(list) = parsed.get("models").and_then(Value::as_array) {
        for entry in list {
            let Some(id) = entry
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            else {
                continue;
            };
            if entry
                .get("is_disabled")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                continue;
            }
            let Some(provider) = entry
                .get("provider")
                .and_then(Value::as_str)
                .filter(|provider| KNOWN_VENDORS.contains(provider))
            else {
                continue;
            };
            let efforts = entry
                .get("supported_effort_levels")
                .and_then(Value::as_array)
                .map(|levels| {
                    levels
                        .iter()
                        .filter_map(|level| {
                            level
                                .get("value")
                                .and_then(Value::as_str)
                                .filter(|value| !value.is_empty())
                                .map(str::to_string)
                        })
                        .collect()
                })
                .unwrap_or_default();
            out.push(ParsedModel {
                id: id.to_string(),
                display_name: entry
                    .get("display_name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .unwrap_or(id)
                    .to_string(),
                provider: provider.to_string(),
                max_token_count: entry
                    .get("max_token_count")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
                max_output_tokens: entry
                    .get("max_output_tokens")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
                supports_images: entry
                    .get("supports_images")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                supports_thinking: entry
                    .get("supports_thinking")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                efforts,
            });
        }
    }
    if out.is_empty() {
        return Err(
            "Zed lists no models for this account — its plan may not include Zed's hosted models (see zed.dev/account)".to_string(),
        );
    }
    register_vendors(&out);
    Ok(out)
}

fn parsed_to_json(model: &ParsedModel) -> Value {
    json!({
        "id": model.id,
        "display_name": model.display_name,
        "provider": model.provider,
        "max_token_count": model.max_token_count,
        "max_output_tokens": model.max_output_tokens,
        "supports_images": model.supports_images,
        "supports_thinking": model.supports_thinking,
        "supported_effort_levels": model
            .efforts
            .iter()
            .map(|value| json!({ "value": value }))
            .collect::<Vec<_>>(),
    })
}

fn parsed_from_json(entry: &Value) -> Option<ParsedModel> {
    // 读回走与远程响应同一个解析器：把单条包成一份 /models 响应再解，
    // 形状只有一处定义（缓存里混进坏条目会被同一套规则剔掉）。
    let wrapped = json!({ "models": [entry] });
    parse_models(&wrapped.to_string()).ok()?.into_iter().next()
}

/// 真打一次 `GET /models`（插件 `fetchModels` 逐字）：令牌 stale 重铸
/// 一次；402 或「列不出可调模型」时 repick 一次组织再拉。成功返回解析
/// 后的清单；失败返回人读原因（登录过期单独成一种，调用方据此上报）。
pub enum FetchError {
    /// 铸造接口 401：登录过期
    SignInExpired(String),
    /// 其余失败（已塑形的人读文案）
    Other(String),
}

/// 一次拉取的失败分类（内部信号，不出本模块）：`Stale` 与 `Payment` /
/// `NothingListed` 是插件里会触发「重铸一次 / repick 一次」的三种情形，
/// 与「如实上报」的失败分开，免得靠文案前缀传信号。
enum FetchFail {
    Stale,
    Payment(String),
    NothingListed(String),
    Other(String),
}

pub async fn fetch_models(
    store: &AccountStore,
    account_id: &str,
    credential: &Credential,
) -> Result<Vec<ParsedModel>, FetchError> {
    let mut credential = credential.clone();
    let mut moved = false;
    let mut renewed = false;
    loop {
        // 铸造（renewed 之后的那一轮强制重铸，与插件 modelToken(t>0) 同形）
        let token = match super::tokens::model_token(&credential, renewed, None).await {
            Ok(token) => token,
            Err(error) => {
                if super::tokens::is_sign_in_expired(&error) {
                    return Err(FetchError::SignInExpired(error.message));
                }
                if error.status_code == 402 && !moved {
                    // 铸造被 402（组织没有可用套餐）：走 repick 再试一次
                    moved = true;
                    match http::repick(&credential, None).await {
                        Some(next) => {
                            save_rep_elected(store, account_id, &next);
                            credential = next;
                            continue;
                        }
                        None => return Err(FetchError::Other(error.message)),
                    }
                }
                return Err(FetchError::Other(error.message));
            }
        };
        let step = async {
            let raw = http::fetch_models_raw(&token, None)
                .await
                .map_err(|error| FetchFail::Other(error.message()))?;
            if !(200..300).contains(&raw.status) {
                let failure = http::HttpFailure {
                    status: raw.status,
                    body: raw.text.clone(),
                    expired_token: raw.expired_token,
                    outdated_token: raw.outdated_token,
                };
                if !renewed && failure.is_stale() {
                    return Err(FetchFail::Stale);
                }
                let (status, message) = super::protocol::refusal(raw.status, &raw.text);
                return Err(if status == 402 {
                    FetchFail::Payment(message)
                } else {
                    FetchFail::Other(message)
                });
            }
            match parse_models(&raw.text) {
                Ok(models) => Ok(models),
                Err(message) => Err(FetchFail::NothingListed(message)),
            }
        }
        .await;
        match step {
            Ok(models) => return Ok(models),
            Err(FetchFail::Stale) => {
                renewed = true;
            }
            Err(fail) => {
                // Stale 已被上面的分支接住，走不到这里；其余按「能不能
                // repick」分：Payment / NothingListed 可以，其余如实上报。
                let message = match fail {
                    FetchFail::Payment(message) => message,
                    FetchFail::NothingListed(message) => message,
                    FetchFail::Other(message) => {
                        return Err(FetchError::Other(message))
                    }
                    FetchFail::Stale => {
                        return Err(FetchError::Other(
                            "Zed：模型目录刷新失败，请重试".to_string(),
                        ))
                    }
                };
                // 402 / 列不出模型：repick 一次组织再拉（插件 fetchModels 逐字）
                if !moved {
                    moved = true;
                    if let Some(next) = http::repick(&credential, None).await {
                        save_rep_elected(store, account_id, &next);
                        credential = next;
                        continue;
                    }
                }
                return Err(FetchError::Other(message));
            }
        }
    }
}

/// repick 选出的新组织回写账号（account_id 为空 = 没有落盘的账号，跳过）。
fn save_rep_elected(store: &AccountStore, account_id: &str, next: &Credential) {
    if !account_id.is_empty() {
        let _ = store.update_zed_organization(account_id, &next.org, &next.plan);
    }
}

/// 刷新目录（适配器 `refresh_models` 的实现）：自动路径 TTL 内早退，
/// 手动路径真打。落地后同步写 `catalog_cache`。
pub async fn refresh(
    store: &AccountStore,
    account_id: &str,
    credential: &Credential,
    force: bool,
) -> ModelRefreshOutcome {
    let now = logging::now_ms();
    if !force {
        let fresh = with_state(|slot| {
            slot.as_ref()
                .map(|state| !state.models.is_empty() && now - state.fetched_at < TTL_MS)
                .unwrap_or(false)
        });
        if fresh {
            return ModelRefreshOutcome::unchanged();
        }
    }
    match fetch_models(store, account_id, credential).await {
        Ok(models) => {
            let count = models.len();
            let stored: Vec<Value> = models.iter().map(parsed_to_json).collect();
            with_state(|slot| {
                *slot = Some(CatalogState {
                    models,
                    fetched_at: now,
                });
            });
            catalog_cache::save(SCOPE, &stored, now);
            ModelRefreshOutcome::refreshed(count)
        }
        Err(FetchError::SignInExpired(message)) => ModelRefreshOutcome::failed(message),
        Err(FetchError::Other(message)) => ModelRefreshOutcome::failed(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_models_filters_and_maps() {
        let text = r#"{
            "models": [
                { "id": "claude-sonnet-5", "provider": "anthropic", "display_name": "Claude Sonnet 5",
                  "max_token_count": 1000000, "max_output_tokens": 64000,
                  "supports_images": true, "supports_thinking": true,
                  "supported_effort_levels": [{ "value": "low" }, { "value": "high" }, { "value": "" }] },
                { "id": "dead", "provider": "anthropic", "is_disabled": true },
                { "id": "mystery", "provider": "someone_else" },
                { "provider": "open_ai" },
                { "id": "gpt-5.5", "provider": "open_ai" }
            ]
        }"#;
        let models = parse_models(text).expect("应能解析");
        assert_eq!(models.len(), 2);
        let first = &models[0];
        assert_eq!(first.id, "claude-sonnet-5");
        assert_eq!(first.provider, "anthropic");
        assert_eq!(first.max_token_count, 1_000_000);
        assert_eq!(first.efforts, vec!["low".to_string(), "high".to_string()]);
        // 没有 display_name 时用 id 兜底
        assert_eq!(models[1].display_name, "gpt-5.5");
        // vendor 归属登记上了
        assert_eq!(vendor_of("claude-sonnet-5"), "anthropic");
    }

    #[test]
    fn parse_models_empty_is_nothing_listed() {
        let error = parse_models(r#"{"models": []}"#).expect_err("空清单应报错");
        assert!(error.starts_with("Zed lists no models"), "{error}");
        // 解不出的响应同样是 NothingListed（插件 parse 失败 → 空表）
        assert!(parse_models("not json").is_err());
    }

    #[test]
    fn fallback_list_matches_plugin_six() {
        let items = fallback_models();
        assert_eq!(items.len(), 6);
        let ids: Vec<&str> = items.iter().map(|model| model.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "claude-sonnet-5",
                "claude-opus-5-5",
                "claude-haiku-4-5",
                "gpt-5.5",
                "gemini-3.5-flash",
                "grok-4.7",
            ]
        );
        let gemini = &items[4];
        assert_eq!(gemini.provider, "google");
        assert_eq!(gemini.max_token_count, 1_048_576);
        assert_eq!(gemini.max_output_tokens, 65_536);
        assert!(items.iter().all(|model| model.supports_images && model.supports_thinking));
    }

    #[test]
    fn vendor_guess_for_unknown_ids() {
        assert_eq!(vendor_of("claude-future-1"), "anthropic");
        assert_eq!(vendor_of("gemini-future"), "google");
        assert_eq!(vendor_of("grok-future"), "x_ai");
        assert_eq!(vendor_of("gpt-future"), "open_ai");
    }
}
