//! Tabbit 凭证：解析 / 粘贴导入 / 从账号记录取快照。
//!
//! ── 凭证形态（tabbit2api 实测）──────────────────────────────
//! 服务端认证只需要 Cookie：`token`（HttpOnly JWT，**必填**）+ `user_id` /
//! `managed=tab_browser` / `NEXT_LOCALE=zh` / `SAPISID`（后四个恒附加，不在
//! 账号里单独存）。`user_id` 可从 JWT 的 `id` / `sub` 字段解析出来。
//!
//! ── 添加路径 ────────────────────────────────────────────────
//!   1. **粘贴 Cookie**：Cookie 头串 / Cookie 编辑器 JSON / `{token, user_id}`
//!      对象三种形态（与 KukuAI 的三种形态同款，解析逻辑同构）；
//!   2. **网页登录**：`login.rs`（打开官方登录页，壳侧读 `token` Cookie 交回）。
//!
//! ── 刷新 ────────────────────────────────────────────────────
//! 上游**没有**刷新接口：JWT 过期只能重新登录 / 重新粘贴（`supports_refresh =
//! false`，与 KukuAI / CatPaw 同一处境）。`ensure_access_token` 因此只做
//! 「从账号记录取 token」，不做网络。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use base64::Engine;
use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::errors::GatewayError;

use super::region::Region;

/// token 最小长度（JWT 实测远大于此；只拦明显残缺的值）
const MIN_TOKEN_LENGTH: usize = 16;

/// 一条 Tabbit 登录态。
#[derive(Clone, Debug)]
pub struct TabbitCredentials {
    /// JWT（`token` Cookie）
    pub token: String,
    /// 用户 id（JWT 解析或粘贴时给出）
    pub user_id: String,
    /// 设备 id（签名指纹用；账号级稳定，落盘）
    pub device_id: String,
    /// OAuth client_id（客户端硬编码，见 `super::CLIENT_ID`）
    pub client_id: String,
    /// 所属地区（账号归属的权威：provider 身份，不是账号字段）
    pub region: Region,
    /// 展示昵称（验证时 best-effort 取到，可空）
    pub nickname: String,
}

impl TabbitCredentials {
    /// 拼出请求用 Cookie 头（`token` / `user_id` / `managed` / `NEXT_LOCALE` /
    /// `SAPISID`，与参考实现 `_cookies()` 同口径）。
    pub fn cookie_header(&self) -> String {
        format!(
            "token={}; user_id={}; managed=tab_browser; NEXT_LOCALE=zh; SAPISID={}",
            self.token, self.user_id, self.user_id
        )
    }

    /// 脱敏尾段（展示用；与各家 `tokenTail` 同一口径）
    pub fn token_tail(&self) -> String {
        token_tail_of(&self.token)
    }
}

pub fn token_tail_of(token: &str) -> String {
    if token.len() <= 8 {
        return String::new();
    }
    token.chars().skip(token.len() - 8).collect()
}

/// 从 JWT 的 payload 里解析用户 id（`id` / `sub` 字段；参考 `_extract_user_id`）。
pub fn user_id_from_jwt(token: &str) -> String {
    let mut parts = token.split('.');
    let payload_b64 = match (parts.next(), parts.next()) {
        (Some(_), Some(payload)) => payload,
        _ => return String::new(),
    };
    let mut padded = payload_b64.to_string();
    while padded.len() % 4 != 0 {
        padded.push('=');
    }
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(padded)
        .unwrap_or_default();
    let value: Value = serde_json::from_slice(&decoded).unwrap_or(Value::Null);
    value
        .get("id")
        .or_else(|| value.get("sub"))
        .and_then(|id| match id {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

/// 从字符串里解析一对 `name=value`（取首个 `=` 后的全部）。
fn cookie_pair(text: &str) -> Option<(String, String)> {
    let text = text.trim();
    let (name, value) = text.split_once('=')?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() || value.is_empty() {
        return None;
    }
    Some((name.to_string(), value.to_string()))
}

/// 从 Cookie 头串提取 `token` + `user_id`。
fn parse_cookie_header(text: &str) -> (String, String) {
    let mut token = String::new();
    let mut user_id = String::new();
    for segment in text.split(';') {
        if let Some((name, value)) = cookie_pair(segment) {
            if name.eq_ignore_ascii_case("token") && token.is_empty() {
                token = value;
            } else if name.eq_ignore_ascii_case("user_id") && user_id.is_empty() {
                user_id = value;
            }
        }
    }
    (token, user_id)
}

/// 从粘贴内容解析凭证（三种形态，与 KukuAI 的三种形态同构）。
///
///   - Cookie 头串：`token=…; user_id=…; …`；
///   - Cookie 编辑器 JSON 数组：`[{"name":"token","value":"…"},…]`；
///   - JSON 对象：`{"token":"…","user_id":"…"}` / `{"cookie":"token=…;…"}`。
///
/// `region` 是用户在添加弹窗选的那一家（权威）；粘贴串里若带了可识别的域名
/// （`base_url` / `domain` 字段），按域名反推并**以域名为准**（参考
/// `region_for_base_url` 的「按登录域名自动归位」）。
pub fn credentials_from_paste(text: &str, region: Region) -> Result<TabbitCredentials, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("请粘贴 Tabbit 的登录 Cookie（token）".to_string());
    }
    let mut token = String::new();
    let mut user_id = String::new();
    let mut hinted_region: Option<Region> = None;

    // 先试 JSON 形态
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        match &value {
            Value::Array(items) => {
                for item in items {
                    let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                    let val = item.get("value").and_then(Value::as_str).unwrap_or("");
                    if name.eq_ignore_ascii_case("token") && token.is_empty() {
                        token = val.to_string();
                    } else if name.eq_ignore_ascii_case("user_id") && user_id.is_empty() {
                        user_id = val.to_string();
                    }
                }
            }
            Value::Object(map) => {
                if let Some(text) = map.get("token").and_then(Value::as_str) {
                    token = text.trim().to_string();
                }
                if let Some(text) = map.get("user_id").and_then(Value::as_str) {
                    user_id = text.trim().to_string();
                }
                if token.is_empty() {
                    if let Some(cookie) = map.get("cookie").and_then(Value::as_str) {
                        let (parsed_token, parsed_uid) = parse_cookie_header(cookie);
                        token = parsed_token;
                        if user_id.is_empty() {
                            user_id = parsed_uid;
                        }
                    }
                }
                for key in ["base_url", "baseUrl", "domain", "url"] {
                    if let Some(url) = map.get(key).and_then(Value::as_str) {
                        if let Some(detected) = Region::from_base_url(url) {
                            hinted_region = Some(detected);
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    // 回落：当成 Cookie 头串
    if token.is_empty() {
        let (parsed_token, parsed_uid) = parse_cookie_header(text);
        token = parsed_token;
        if user_id.is_empty() {
            user_id = parsed_uid;
        }
    }
    // 裸 token（不带 `token=` 前缀的 JWT 三段式）
    if token.is_empty() && text.split('.').count() == 3 && !text.contains(' ') && !text.contains(';')
    {
        token = text.to_string();
    }
    if token.len() < MIN_TOKEN_LENGTH {
        return Err("未从粘贴内容里识别出有效的 token（JWT 长度明显不足）".to_string());
    }
    if user_id.is_empty() {
        user_id = user_id_from_jwt(&token);
    }
    let region = hinted_region.unwrap_or(region);
    Ok(TabbitCredentials {
        token,
        user_id,
        device_id: new_device_id(),
        client_id: super::CLIENT_ID.to_string(),
        region,
        nickname: String::new(),
    })
}

/// 生成设备 id（uuid4 文本；账号级稳定，随凭证落盘后跨请求不变）。
pub fn new_device_id() -> String {
    super::protocol::public_random_uuid()
}

/// 用上游 `/api/v0/user/base-info` 验证凭证并补全身份（昵称/uid）。
///
/// 验证失败（凭证无效）→ 错误上抛，调用方按 400 处理，不落空记录
/// （与 KukuAI 的添加验证同口径）。
pub async fn enrich_identity(
    credentials: &TabbitCredentials,
    proxy: Option<&crate::server::core::proxies::ResolvedProxy>,
) -> Result<TabbitCredentials, GatewayError> {
    let url = format!("{}/api/v0/user/base-info", credentials.region.base_url());
    let headers = super::protocol::base_headers(credentials.region, credentials, "/");
    let mut enriched = credentials.clone();
    let value = super::http::get_json_value(&url, &headers, proxy, Some(15_000)).await?;
    let info = value.get("user_info").cloned().unwrap_or(Value::Null);
    if enriched.user_id.is_empty() {
        enriched.user_id = info
            .get("id")
            .or_else(|| info.get("user_id"))
            .and_then(|id| match id {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            })
            .unwrap_or_default();
    }
    enriched.nickname = info
        .get("display_name")
        .or_else(|| info.get("nickname"))
        .or_else(|| info.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok(enriched)
}

/// 从账号存储取某账号的凭证快照（**按地区隔离**：只认本地区的账号记录）。
///
/// `account_id` 为空 → 取本地区组内优先级最小的启用账号（与转发选路同一判据）；
/// 点名了别的地区的账号 → 视为无可用凭证（不跨地区取，见模块头）。
pub fn snapshot_for(
    region: Region,
    store: &AccountStore,
    account_id: &str,
) -> Result<TabbitCredentials, GatewayError> {
    let record = store
        .tabbit_account_record(region.provider_id(), account_id)
        .ok_or_else(|| {
            GatewayError::with_status(
                401,
                format!(
                    "Tabbit（{}）没有可用的登录凭证：请在账号页添加账号（粘贴 Cookie 或网页登录）",
                    region.label()
                ),
            )
        })?;
    let token = record
        .get("accessToken")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if token.len() < MIN_TOKEN_LENGTH {
        return Err(GatewayError::with_status(
            401,
            "Tabbit 账号缺少有效的 token：请重新添加该账号",
        ));
    }
    Ok(TabbitCredentials {
        token,
        user_id: record
            .get("uid")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        device_id: record
            .get("deviceId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(new_device_id),
        client_id: super::CLIENT_ID.to_string(),
        region,
        nickname: record
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    })
}
