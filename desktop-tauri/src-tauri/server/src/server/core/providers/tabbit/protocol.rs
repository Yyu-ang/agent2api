//! Tabbit 上游协议：HMAC-SHA256 签名 + 会话 + SSE。
//!
//! ── 协议事实（tabbit2api 2026-10 实测，本模块照抄）────────────
//!   - 认证 Cookie：`token`（HttpOnly JWT）为主，辅 `user_id` / `managed` /
//!     `NEXT_LOCALE` / `SAPISID`；
//!   - 签名 key：`GET {base}/chat/sign-key` 动态获取（各域名独立），TTL 1800 秒，
//!     服务端回 499 时刷新 key 重试一次；
//!   - 签名：`ts`（毫秒）+ `nonce_uuid`（uuid4）+ `digest = sha256hex(body)`，
//!     `x-nonce = HMAC-SHA256(key, "{ts}.{nonce_uuid}.{digest}")`（hex），
//!     `x-signature = nonce_uuid`，`x-timestamp = ts`；
//!   - 版本头：`x-req-ctx = base64("1.6.20(10106020)")`（缺失或格式不符判 493
//!     「浏览器版本过低」）；
//!   - 自证头 `unique-uuid`：**国际版必需**，缺失聊天直接 492。32 位 hex，
//!     第 5 位（0-indexed）是「默认浏览器」标记位，位置 [2,7,11,14,18,21,25,28]
//!     编码时间 hex。**如实填写**：只在真的默认浏览器时置标记位（参考实现同
//!     口径，不伪造 Pro 权益）；
//!   - 建会话：`POST /panel/session`（空 body，**不带签名头**）；
//!   - 对话：`POST /api/v1/chat/completion`（SSE），body 见 `chat_body`；
//!     `selected_model` 必须传模型列表里的 `display_name`（发 slug 会被服务端
//!     自动切换模型）；
//!   - 模型目录：`GET /proxy/v1/model_config/models?a=0&scene=chat`（带签名，
//!     空 body 签名）；
//!   - 额度：`GET /api/commerce/quota/v1/usage?user_id=<uid>`；
//!   - 用户信息：`GET /api/v0/user/base-info`。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use std::collections::HashMap;
use std::sync::Mutex;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::server::errors::GatewayError;

use super::credentials::TabbitCredentials;
use super::region::Region;

/// 出站 User-Agent（tabbit2api 实测值；上游对 UA 敏感）
pub const USER_AGENT: &str = concat!(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ",
    "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
);

/// sec-ch-ua（Tabbit Browser 指纹，实测值）
const SEC_CH_UA: &str = "\"Tabbit Browser\";v=\"149\", \"Chromium\";v=\"149\", \"Not)A;Brand\";v=\"24\"";

/// x-req-ctx（base64("1.6.20(10106020)")，实测通过值）
pub const REQ_CTX: &str = "MS42LjIwKDEwMTA2MDIwKQ==";

/// 签名 key 有效期（秒，参考实现 1800）
const SIGN_KEY_TTL_SECS: i64 = 1800;

/// 签名 key 兜底（`GET /chat/sign-key` 失败时用；参考实现 `FALLBACK_SIGN_KEY`）。
/// 注意：兜底 key 只是让请求能发出去，服务端大概率回 499 → 触发刷新重试。
const FALLBACK_SIGN_KEY: &str = "f8d0e6a73f8d4b1a9c3d2e1f9a4b7c6d";

/// unique-uuid 里的时间编码位
const TS_POSITIONS: [usize; 8] = [2, 7, 11, 14, 18, 21, 25, 28];

/// 签名 key 槽（按 base_url 分域名缓存，各域名独立）。
struct SignKeySlot {
    key: String,
    fetched_at_ms: i64,
}

static SIGN_KEYS: Mutex<Option<HashMap<String, SignKeySlot>>> = Mutex::new(None);

fn sign_key_guard() -> std::sync::MutexGuard<'static, Option<HashMap<String, SignKeySlot>>> {
    match SIGN_KEYS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// 取（必要时刷新）某域名的签名 key。
///
/// `force = true` 跳过 TTL 直接刷新（收到 499 时用）。
pub async fn ensure_sign_key(
    region: Region,
    credentials: &TabbitCredentials,
    proxy: Option<&crate::server::core::proxies::ResolvedProxy>,
    force: bool,
) -> String {
    let base_url = region.base_url();
    let now = crate::server::logging::now_ms();
    if !force {
        let guard = sign_key_guard();
        if let Some(slot) = guard.as_ref().and_then(|map| map.get(base_url)) {
            if now - slot.fetched_at_ms < SIGN_KEY_TTL_SECS * 1000 && !slot.key.is_empty() {
                return slot.key.clone();
            }
        }
    }
    let key = refresh_sign_key(region, credentials, proxy).await.unwrap_or_default();
    if !key.is_empty() {
        let mut guard = sign_key_guard();
        let map = guard.get_or_insert_with(HashMap::new);
        map.insert(
            base_url.to_string(),
            SignKeySlot { key: key.clone(), fetched_at_ms: now },
        );
        return key;
    }
    // 刷新失败：有旧值用旧值，没有用兜底（服务端回 499 再由调用方处理）
    let guard = sign_key_guard();
    guard
        .as_ref()
        .and_then(|map| map.get(base_url))
        .map(|slot| slot.key.clone())
        .filter(|key| !key.is_empty())
        .unwrap_or_else(|| FALLBACK_SIGN_KEY.to_string())
}

async fn refresh_sign_key(
    region: Region,
    credentials: &TabbitCredentials,
    proxy: Option<&crate::server::core::proxies::ResolvedProxy>,
) -> Option<String> {
    let url = format!("{}/chat/sign-key", region.base_url());
    let headers = base_headers(region, credentials, "/");
    let response = super::http::get_raw(&url, &headers, proxy, Some(15_000)).await.ok()?;
    if response.status != 200 {
        return None;
    }
    let text = response.text.trim().to_string();
    if text.is_empty() {
        return None;
    }
    crate::server::logging::verbose(
        "[Tabbit]",
        &format!("签名 key 已刷新（{}…）", text.chars().take(8).collect::<String>()),
    );
    Some(text)
}

/// HMAC-SHA256 签名头（`x-timestamp` / `x-signature` / `x-nonce`）。
///
/// 待签串：`"{ts}.{nonce_uuid}.{sha256hex(body)}"`，key 为当前签名 key。
/// `body` 传**实际发出去的字节串**（与发送侧同一序列化口径，否则 MD5 对不上）。
pub fn signature_headers(sign_key: &str, body: &str) -> Vec<(String, String)> {
    let ts = crate::server::logging::now_ms().to_string();
    let nonce_uuid = random_uuid();
    let digest = {
        let mut hasher = Sha256::new();
        hasher.update(body.as_bytes());
        hex_of(&hasher.finalize())
    };
    let msg = format!("{ts}.{nonce_uuid}.{digest}");
    let nonce = hmac_hex(sign_key.as_bytes(), msg.as_bytes());
    vec![
        ("x-timestamp".to_string(), ts),
        ("x-signature".to_string(), nonce_uuid),
        ("x-nonce".to_string(), nonce),
    ]
}

fn hmac_hex(key: &[u8], msg: &[u8]) -> String {
    // 空 key 时 Hmac::new_from_slice 会失败：用确定性回退 key（签名必然被
    // 服务端拒绝并触发 499 刷新链路，不 panic）
    let effective: &[u8] = if key.is_empty() { b"tabbit" } else { key };
    let mut mac = match Hmac::<Sha256>::new_from_slice(effective) {
        Ok(mac) => mac,
        Err(_) => return String::new(),
    };
    mac.update(msg);
    hex_of(&mac.finalize().into_bytes())
}

fn hex_of(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// 基础请求头（浏览器指纹 + referer/origin）。
pub fn base_headers(
    region: Region,
    credentials: &TabbitCredentials,
    referer_path: &str,
) -> Vec<(String, String)> {
    let base_url = region.base_url();
    vec![
        ("User-Agent".to_string(), USER_AGENT.to_string()),
        ("sec-ch-ua".to_string(), SEC_CH_UA.to_string()),
        ("sec-ch-ua-platform".to_string(), "\"Windows\"".to_string()),
        ("sec-ch-ua-mobile".to_string(), "?0".to_string()),
        (
            "x-chrome-id-consistency-request".to_string(),
            format!(
                "version=1,client_id={},device_id={},sync_account_id={},signin_mode=all_accounts,signout_mode=show_confirmation",
                credentials.client_id, credentials.device_id, credentials.user_id
            ),
        ),
        ("referer".to_string(), format!("{base_url}{referer_path}")),
        ("Origin".to_string(), base_url.to_string()),
        ("Cookie".to_string(), credentials.cookie_header()),
    ]
}

/// 对话请求的完整头（含签名 + x-req-ctx + unique-uuid）。
pub fn chat_headers(
    region: Region,
    credentials: &TabbitCredentials,
    session_id: &str,
    body: &str,
    sign_key: &str,
) -> Vec<(String, String)> {
    let mut headers = base_headers(region, credentials, &format!("/session/{session_id}"));
    headers.push(("Accept".to_string(), "text/event-stream".to_string()));
    headers.push(("Content-Type".to_string(), "application/json".to_string()));
    headers.push(("Cache-Control".to_string(), "no-cache".to_string()));
    headers.push(("trace-id".to_string(), random_uuid()));
    headers.push(("x-req-ctx".to_string(), REQ_CTX.to_string()));
    // 国际版必需：缺失聊天直接 492（参考实现）；国内版也带上（无害）
    headers.push(("unique-uuid".to_string(), make_unique_uuid(false)));
    headers.extend(signature_headers(sign_key, body));
    headers
}

/// unique-uuid：32 位 hex（UUID 形式），第 5 位（0-indexed）是默认浏览器标记位，
/// 位置 [2,7,11,14,18,21,25,28] 编码当前时间 hex。
///
/// **如实填写**：`is_default_browser` 只在真的默认浏览器时为 true（参考实现同
/// 口径，不伪造 Pro 权益；标记位 0 已足以正常调用）。
pub fn make_unique_uuid(is_default_browser: bool) -> String {
    let now_secs = crate::server::logging::now_ms() / 1000;
    let ts_hex = format!("{:08x}", now_secs as u32);
    let ts_chars: Vec<char> = ts_hex.chars().collect();
    let entropy = random_bytes(32);
    let hex = b"0123456789abcdef";
    let non_default = b"023456789abcdef";
    let mut out = String::with_capacity(36);
    for i in 0..32 {
        let random = entropy.get(i).copied().unwrap_or(0) as usize;
        let ch = if i == 5 {
            if is_default_browser { '1' } else { non_default[random % non_default.len()] as char }
        } else if let Some(pos) = TS_POSITIONS.iter().position(|&p| p == i) {
            ts_chars.get(pos).copied().unwrap_or('0')
        } else {
            hex[random % hex.len()] as char
        };
        out.push(ch);
        if matches!(i, 7 | 11 | 15 | 19) { out.push('-'); }
    }
    out
}

/// 对话请求体（`POST /api/v1/chat/completion`）。
///
/// `model` 必须是上游 `display_name`（发 slug 会被服务端自动切换模型，见
/// `models::resolve_model`）。`content` 是本轮要发送的文本（增量/全量由调用方
/// 按参考实现的增量策略决定 —— 本版先全量，见 `chat.rs` 模块头）。
pub fn chat_body(session_id: &str, content: &str, model: &str) -> String {
    let html = format!("<p>{}</p>", escape_html(content));
    let payload = serde_json::json!({
        "chat_session_id": session_id,
        "message_id": null,
        "content": content,
        "selected_model": model,
        "parallel_group_id": null,
        "task_name": "chat",
        "agent_mode": false,
        "metadatas": { "html_content": html },
        "references": [],
        "entity": { "key": "d41d8cd98f00b204e9800998ecf8427e", "extras": { "type": "tab", "url": "" } },
        "client_turn_id": random_uuid(),
        "stream_mode": "sse",
        "force_execute": false,
    });
    payload.to_string()
}

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// uuid4 文本（跨模块共用：设备 id、登录 state 用；`pub(crate)` 避免重复实现）。
pub(crate) fn public_random_uuid() -> String {
    random_uuid()
}

/// 随机十六进制串（登录 state 用）。
pub(crate) fn public_random_hex(length: usize) -> String {
    let bytes = random_bytes(length.div_ceil(2));
    let mut out = String::with_capacity(length);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
        if out.len() >= length {
            break;
        }
    }
    out.truncate(length);
    out
}

fn random_uuid() -> String {
    // uuid4 文本形态（8-4-4-4-12），用系统随机源
    let bytes = random_bytes(16);
    let mut b = [0u8; 16];
    b.copy_from_slice(&bytes);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
    )
}

fn random_bytes(length: usize) -> Vec<u8> {
    // getrandom 0.2（与 qoder::machine 同款调用）
    let mut buf = vec![0u8; length];
    if getrandom::getrandom(&mut buf).is_err() {
        // 随机源不可用时退化为时间混合（签名 nonce 的唯一性降级，不 panic）
        let now = crate::server::logging::now_ms() as u64;
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = ((now >> ((i % 8) * 8)) ^ (i as u64 * 0x9E3779B97F4A7C15)) as u8;
        }
    }
    buf
}

/// 把上游 492 这类「文案与真实原因无关」的错误翻译成可读提示（参考
/// `service._explain_error`）。
///
/// 上游超长（约 2 万字符）/ 无权限（Premium）/ 版本过低都回 492，且文案是
/// 「欢迎使用 Tabbit 浏览器…」这类营销语 —— 不翻译会让用户误判为账号问题。
pub fn explain_error(raw: &str, code: Option<i64>, req_chars: usize) -> String {
    let text = raw.trim();
    let looks_like_marketing = text.contains("欢迎使用 Tabbit");
    if code == Some(492) || looks_like_marketing {
        if req_chars > 18_000 {
            return format!(
                "请求内容过长（约 {req_chars} 字符）被上游拒绝：实测 Tabbit 上游单次上限约 2 万字符，\
                 请缩短本轮输入或拆分多轮发送"
            );
        }
        return "上游拒绝了本次请求（492）：可能是该模型为 Premium（当前账号等级不足）、\
                内容过长，或国际版缺 unique-uuid 自证头；上游原文与真实原因无关，\
                请按上述三项排查"
            .to_string();
    }
    if text.is_empty() {
        return "上游返回错误（空响应体）".to_string();
    }
    text.chars().take(300).collect()
}

/// 409「会话处理中」的判定（参考 `_is_busy_error`）：未产出内容时等待重试。
pub fn is_busy_error(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("409")
        || lower.contains("会话处理中")
        || lower.contains("session is busy")
        || lower.contains("processing")
}

/// 建会话失败时的网关错误翻译（调用方已判过 HTTP 状态，这里只做文案）。
pub fn gateway_error(what: &str, status: u16, head: &str) -> GatewayError {
    if status == 401 || status == 403 {
        return GatewayError::with_status(
            401,
            format!("Tabbit 登录态已失效，请重新登录或重新添加该账号（{what}，HTTP {status}）"),
        );
    }
    GatewayError::with_status(
        status as i32,
        format!("Tabbit {what}失败（HTTP {status}）：{head}"),
    )
}
