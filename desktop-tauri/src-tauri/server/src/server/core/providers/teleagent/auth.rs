//! TeleAgent 签名鉴权：session key 提取 + `local-v1` HMAC-SHA256 签名。
//!
//! ── 移植自 ────────────────────────────────────────────────────
//! `teleagent-openai-proxy/openai_proxy.py` 的 `get_session_key()` /
//! `sign_request()`（MIT，原作者 lizhun781021），逻辑逐字对应：
//!   1. 先读环境变量 `SUPER_AGENT_LOCAL_SESSION_KEY`；
//!   2. 没有则在 scheduler 进程的环境里找（`pgrep -f scheduler/index.cjs`
//!      → `ps eww <pid>` 里匹配 `SUPER_AGENT_LOCAL_SESSION_KEY=<值>`）；
//!   3. 60 秒缓存（`_cached_session_key` / `_cached_session_key_time`）。
//!
//! ── 待签串（与原实现逐字同构）─────────────────────────────────
//! ```text
//!   local-v1\n{METHOD}\n{path_with_query}\n{timestamp_ms}\n{nonce}
//! ```
//! `nonce` 是 24 位 hex（原实现 `uuid.uuid4().hex[:24]`）；签名是
//! HMAC-SHA256 后 base64url **去填充**；四个头
//! `X-SA-Sign-Version / X-SA-Timestamp / X-SA-Nonce / X-SA-Signature`。
//!
//! ── 平台差异（原实现同样如此，如实保留）───────────────────────
//! 进程环境提取依赖 `pgrep` / `ps`，只在 Unix 上可用；Windows 上只有
//! 环境变量一条路（见 [`session_key`] 的文档）。网关在 Windows 上用
//! TeleAgent 时，需要把 key 设进网关进程的环境变量。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await
//! （缓存锁只在同步段里拿放）。

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::server::errors::GatewayError;

use super::{SIGN_VERSION, SUPER_AGENT_HOST, SUPER_AGENT_PORT};

/// session key 的环境变量名（原实现常量）
const SESSION_KEY_ENV: &str = "SUPER_AGENT_LOCAL_SESSION_KEY";
/// scheduler 进程的匹配特征（原实现 `pgrep -f` 的参数）
const SCHEDULER_PATTERN: &str = "scheduler/index.cjs";
/// key 缓存有效期（原实现 60 秒）
const KEY_CACHE_TTL_MS: i64 = 60_000;

struct KeyCache {
    key: Option<String>,
    fetched_at_ms: i64,
}

/// 进程级 key 缓存（原实现的 `_cached_session_key`  globals）
static KEY_CACHE: Mutex<KeyCache> = Mutex::new(KeyCache {
    key: None,
    fetched_at_ms: 0,
});

fn cache_guard() -> std::sync::MutexGuard<'static, KeyCache> {
    match KEY_CACHE.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// 取 session key（缓存 60 秒；`force` 跳过缓存、真取一次）。
///
/// 顺序与原实现一致：环境变量 →（Unix）scheduler 进程环境。
/// 取不到返回 `None`（调用方转成「请确保 TeleAgent 桌面应用正在运行」的
/// 503/401，而不是在这里抛错 —— 缓存检查与转发都要调它）。
pub fn session_key(force: bool) -> Option<String> {
    if !force {
        let guard = cache_guard();
        if let Some(key) = guard.key.clone() {
            if now_ms() - guard.fetched_at_ms < KEY_CACHE_TTL_MS && !key.is_empty() {
                return Some(key);
            }
        }
        drop(guard);
    }
    let key = from_env().or_else(from_scheduler_process);
    let mut guard = cache_guard();
    guard.key = key.clone();
    guard.fetched_at_ms = now_ms();
    key
}

/// 从环境变量取（跨平台，原实现第一优先级）。
fn from_env() -> Option<String> {
    std::env::var(SESSION_KEY_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// 从 scheduler 进程的环境里取（原实现第二优先级，Unix only）。
///
/// `pgrep -f scheduler/index.cjs` 找 pid → `ps eww <pid>` 读环境 →
/// 正则 `SUPER_AGENT_LOCAL_SESSION_KEY=(\S+)`（这里手写扫描，不引入
/// regex 依赖：找 `KEY=` 前缀、取到空白为止，与 `\S+` 同语义）。
#[cfg(unix)]
fn from_scheduler_process() -> Option<String> {
    let output = std::process::Command::new("pgrep")
        .args(["-f", SCHEDULER_PATTERN])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for pid in stdout.split_whitespace() {
        if pid.is_empty() || !pid.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }
        let env_output = std::process::Command::new("ps")
            .args(["eww", pid])
            .output()
            .ok()?;
        if !env_output.status.success() {
            continue;
        }
        let env_text = String::from_utf8_lossy(&env_output.stdout);
        let needle = format!("{SESSION_KEY_ENV}=");
        for segment in env_text.split_whitespace() {
            if let Some(value) = segment.strip_prefix(needle.as_str()) {
                let value = value.trim();
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

/// Windows：没有 `pgrep`/`ps`，读其他进程环境需要 PEB 解析 —— 原实现
/// 本来也不支持，这里如实只走环境变量（见模块头「平台差异」）。
#[cfg(not(unix))]
fn from_scheduler_process() -> Option<String> {
    None
}

/// 生成一次签名的四个头（原实现 `sign_request`）。
///
/// `path_with_query` 是待签路径（含 query；本家各接口目前都不带 query，
/// 与原实现调用点一致）。key 取不到时返回 503（原实现抛 RuntimeError，
/// 这里转成网关错误：文案与原实现一致）。
pub fn sign_headers(method: &str, path_with_query: &str) -> Result<Vec<(String, String)>, GatewayError> {
    let key = session_key(false).ok_or_else(|| {
        GatewayError::with_status(
            503,
            "无法获取 SUPER_AGENT_LOCAL_SESSION_KEY，请确保 TeleAgent 桌面应用正在运行",
        )
    })?;
    sign_headers_with_key(method, path_with_query, &key)
}

/// 用显式 key 生成签名头（核心实现；上者是「自动提取 key」的便捷包装）。
///
/// 添加账号时探活用户粘贴的 key、转发时用账号里存的 key 都走这里 ——
/// 不读 key 缓存、不触发刷新：调用方拿到的 key 是哪一把，签的就是哪一把。
pub fn sign_headers_with_key(
    method: &str,
    path_with_query: &str,
    key: &str,
) -> Result<Vec<(String, String)>, GatewayError> {
    if key.trim().is_empty() {
        return Err(GatewayError::with_status(400, "TeleAgent session key 为空，无法签名"));
    }
    let timestamp = now_ms().to_string();
    let nonce = random_hex(24);
    // 待签串：5 段 `\n` 连接（与原实现 `"\\n".join([...])` 逐字同构）
    let payload = format!(
        "{SIGN_VERSION}\n{}\n{path_with_query}\n{timestamp}\n{nonce}",
        method.to_ascii_uppercase()
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).map_err(|_| {
        GatewayError::with_status(503, "TeleAgent 签名初始化失败（session key 非法）")
    })?;
    mac.update(payload.as_bytes());
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(vec![
        ("X-SA-Sign-Version".to_string(), SIGN_VERSION.to_string()),
        ("X-SA-Timestamp".to_string(), timestamp),
        ("X-SA-Nonce".to_string(), nonce),
        ("X-SA-Signature".to_string(), signature),
    ])
}

/// super-agent 的基址（原实现 `SUPER_AGENT_BASE`）
pub fn base_url() -> String {
    format!("{SUPER_AGENT_HOST}:{SUPER_AGENT_PORT}")
}

/// super-agent 的完整基址（含 scheme）
pub fn base_http_url() -> String {
    format!("http://{}", base_url())
}

/// 随机十六进制串（nonce 用；`getrandom` 与 kuku 同源）。
fn random_hex(length: usize) -> String {
    let mut out = String::with_capacity(length);
    while out.len() < length {
        let mut bytes = [0u8; 16];
        if getrandom::getrandom(&mut bytes).is_err() {
            break;
        }
        for byte in bytes {
            out.push_str(&format!("{byte:02x}"));
            if out.len() >= length {
                break;
            }
        }
    }
    // 极端兜底：随机源不可用时用时间戳补齐（保证签名 nonce 不为空；
    // 安全性不依赖 nonce 保密，只要求不重复）
    while out.len() < length {
        out.push_str(&format!("{:x}", now_ms()));
    }
    out.truncate(length);
    out
}
