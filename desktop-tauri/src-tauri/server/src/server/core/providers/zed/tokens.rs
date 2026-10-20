//! 模型令牌（`POST /client/llm_tokens` 换来的短期令牌）的内存缓存与
//! 单飞铸造（插件 `tokens` / `minting` 两张 Map 的对应物）。
//!
//! ── 为什么只住内存 ────────────────────────────────────────
//! Zed 铸的令牌活一个小时量级，且上游会明说它过期（401 /
//! `x-zed-expired-token` / `x-zed-outdated-token`）。落盘没有收益只有
//! 风险（把短期令牌写进账号库，将来还要负责清理），插件同样只放内存。
//!
//! ── 缓存键 ────────────────────────────────────────────────
//! `userId + " " + org + " " + access`（插件逐字）：令牌是按组织铸的，
//! 组织换了（repick）或令牌换了（重新登录）都必须铸新的 —— 键里带全
//! 三者，旧条目自然失效、不需要显式清理。
//!
//! ── 401 的翻译 ────────────────────────────────────────────
//! 铸造接口本身回 401 = 账号的登录被拒（插件 `SignInExpired`）：这是
//! 「登录过期、请重新登录」，与「模型令牌被拒」是两件事。翻译成
//! [`GatewayError`] 时打上 [`SIGN_IN_EXPIRED_CODE`] 标记，转发层据此
//! 给出 TokenExpired 语义；其余铸造失败（402 等）原样带状态回去，由
//! 调用方决定 repick 还是 refusal 塑形。

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::providers::refresh_flight::{Join, Table};
use crate::server::errors::GatewayError;

use super::credentials::Credential;
use super::http::{self, CloudError};

/// 「铸造接口回 401 = 登录过期」的机器可识别标记（`GatewayError::code`）。
pub const SIGN_IN_EXPIRED_CODE: &str = "zed_sign_in_expired";

fn cache() -> &'static Mutex<HashMap<String, String>> {
    static CACHE: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn flights() -> &'static Table<String> {
    static FLIGHTS: OnceLock<Table<String>> = OnceLock::new();
    FLIGHTS.get_or_init(Table::new)
}

fn key_of(credential: &Credential) -> String {
    format!(
        "{} {} {}",
        credential.user_id, credential.org, credential.access
    )
}

fn cached(key: &str) -> Option<String> {
    cache()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(key)
        .cloned()
}

fn store(key: &str, token: &str) {
    cache()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(key.to_string(), token.to_string());
}

fn evict(key: &str) {
    cache()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(key);
}

/// 取一枚可用的模型令牌。`renew = true` 表示上一枚被上游判 stale：
/// 绕过缓存重铸（插件 `modelToken(s, renew)` 逐字）。
///
/// 单飞：同键并发只铸一次（`refresh_flight::Table`，与 antigravity 的
/// 令牌刷新同一设施），等待者共用 leader 的结果 —— 失败也一样（上游正
/// 在拒绝时，一串并发各铸一次只会把拒绝放大）。
pub async fn model_token(
    credential: &Credential,
    renew: bool,
    proxy: Option<&ResolvedProxy>,
) -> Result<String, GatewayError> {
    let key = key_of(credential);
    if !renew {
        if let Some(token) = cached(&key) {
            return Ok(token);
        }
    }
    match flights().join(&key) {
        Join::Waiter(waiter) => waiter.wait().await,
        Join::Leader(guard) => {
            // 双检：排队期间可能已有别的 leader 铸好放进缓存了
            if !renew {
                if let Some(token) = cached(&key) {
                    let token_clone = token.clone();
                    guard.finish(Ok(token_clone));
                    return Ok(token);
                }
            }
            let result = mint(credential, proxy).await;
            match &result {
                Ok(token) => store(&key, token),
                Err(_) => evict(&key),
            }
            guard.finish(result.clone());
            result
        }
    }
}

/// 真铸一次，并把 401 翻译成「登录过期」。
async fn mint(
    credential: &Credential,
    proxy: Option<&ResolvedProxy>,
) -> Result<String, GatewayError> {
    match http::mint_model_token(credential, proxy).await {
        Ok(token) => Ok(token),
        Err(CloudError::Status(failure)) if failure.status == 401 => Err(GatewayError::with_status(
            401,
            format!(
                "{}：Zed 登录已过期，请重新登录",
                if credential.display.is_empty() {
                    "该账号"
                } else {
                    &credential.display
                }
            ),
        )
        .with_code(SIGN_IN_EXPIRED_CODE)),
        Err(error) => {
            let status = error.status().unwrap_or(502);
            Err(GatewayError::with_status(
                i32::from(status),
                format!("Zed：铸造模型令牌失败（{}）", error.message()),
            ))
        }
    }
}

/// 这个错误是不是「登录过期」（铸造 401）。
pub fn is_sign_in_expired(error: &GatewayError) -> bool {
    error.code.as_deref() == Some(SIGN_IN_EXPIRED_CODE)
}
