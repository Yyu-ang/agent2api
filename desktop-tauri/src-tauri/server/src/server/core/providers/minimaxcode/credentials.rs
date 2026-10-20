//! MiniMax Code 凭证：OAuth 常量、PKCE、JWT 声明、令牌响应解析、账号记录解析，
//! 以及登录 / 刷新共用的 form POST。
//!
//! ── 协议（来源：社区插件 `@magpie-community/opencode-minimax-auth` v0.1.2）──
//! OAuth 是**设备流 + PKCE S256**：`client_id=mcode-public`、
//! `scope=agent.default`、`audience=agent-backend`，端点都在账号站
//! （`{account}/oauth2/device/code` 与 `{account}/oauth2/token`）。
//! 令牌响应是标准 OAuth 形态（`access_token` / `refresh_token` /
//! `expires_in` 秒）；access token 是 JWT，`sub` 与 `account_id` 声明是
//! 账号标识的来源（插件 `token()` 同口径）。
//!
//! ── refresh token 是**单次使用**的（本家最重要的纪律）──────────
//! 插件原话：MiniMax 花掉一个 refresh token 就给一个新的，旧的再问一次
//! 就是 `invalid_grant`、账号会被判登出。因此同一枚 refresh **绝不能发
//! 两次**：刷新走 `refresh.rs` 的单飞（仓库共用 `refresh_flight::Table`），
//! 成功后立即比较-再写回新的一对。用 `Copy` 的话说——本文件只管「凭证长
//! 什么样」，「怎么刷」在 `refresh.rs`。
//!
//! ── 真登出的唯一判据 ────────────────────────────────────────
//! 只有 **HTTP 400 + `error=invalid_grant`** 才算 refresh 被作废
//! （插件 `signedOut`）；5xx / 超时 / 其它错误都是路过，登录态保留。
//! 判据在 [`is_signed_out`]，刷新链据此决定给 401 终态还是 502 暂态。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件绝不 unwrap/expect/panic，取值走 Option 链。

use std::time::Duration;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::server::core::egress;
use crate::server::errors::GatewayError;

use super::region::Region;

/// OAuth client_id（mcode 公开客户端，插件 `CLIENT_ID`）
pub const CLIENT_ID: &str = "mcode-public";
/// OAuth scope（插件 `SCOPE`）
pub const SCOPE: &str = "agent.default";
/// OAuth audience（插件 `AUDIENCE`）
pub const AUDIENCE: &str = "agent-backend";
/// 设备流 grant_type（RFC 8628，插件 `DEVICE_GRANT`）
pub const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// 临期窗口：距过期不足 2 分钟就主动刷新（插件 `EARLY_MS`）。
pub const PROACTIVE_REFRESH_MARGIN_MS: i64 = 2 * 60 * 1000;

/// token 长度上限（与账号存储的 MAX_TOKEN_LENGTH 同量级）
pub const MAX_TOKEN_LENGTH: usize = 8192;

/// OAuth / 登录 / 刷新请求的超时（插件 postForm 的 30 秒同档）
pub const REQUEST_TIMEOUT_MS: u64 = 30_000;

/// 一份 MiniMax Code 凭证（账号记录解析后的统一形态）。
#[derive(Clone, Debug, PartialEq)]
pub struct MinimaxCredentials {
    /// 账号记录 id
    pub id: String,
    /// 地区（由记录的 provider 决定，不是记录里的字段）
    pub region: Region,
    /// access token（JWT，原样发 `Authorization: Bearer`）
    pub access_token: String,
    /// refresh token（可能为空 —— 用户只粘贴 access token 时）
    pub refresh_token: String,
    /// 过期时间（毫秒时间戳；None = 无从判断）
    pub expires_at: Option<f64>,
    /// 账号标识（JWT `account_id` 优先，其次 `sub`；签名账号 API 的 user_id 也用它）
    pub uid: String,
    /// 邮箱（JWT 声明里有才有；v1 不调 identity 接口补，见模块头缺口）
    pub email: String,
    /// 展示名（记录里的备注名 / displayName，解析时不做推导，推导在账号层）
    pub name: String,
}

impl MinimaxCredentials {
    /// 是否可续期（有 refresh token）
    pub fn can_refresh(&self) -> bool {
        !self.refresh_token.trim().is_empty()
    }

    /// 是否已过期或临期（判不出过期时间时返回 false —— 与 trait 契约一致：
    /// 无从判断就说「需要刷新」会让维护任务白跑一趟）
    pub fn is_expiring(&self) -> bool {
        self.is_expiring_at(crate::server::logging::now_ms())
    }

    /// 指定时刻的临期判定（把「现在」作参数是为了让判据可测）
    pub fn is_expiring_at(&self, now_ms: i64) -> bool {
        match self.expires_at {
            Some(expires) => (expires as i64).saturating_sub(now_ms) <= PROACTIVE_REFRESH_MARGIN_MS,
            None => false,
        }
    }

    /// 是否已经过期（判不出时 false）
    pub fn is_expired_at(&self, now_ms: i64) -> bool {
        match self.expires_at {
            Some(expires) => (expires as i64) <= now_ms,
            None => false,
        }
    }
}

/// 一次令牌响应解析出的新凭证三件套（登录轮询与刷新共用，插件 `token()`）。
#[derive(Clone, Debug, PartialEq)]
pub struct TokenSet {
    /// 新 access token
    pub access: String,
    /// 新 refresh token（响应没给时沿用 `fallback_refresh` —— 插件同口径）
    pub refresh: String,
    /// 过期时间（毫秒时间戳 = now + expires_in × 1000）
    pub expires_at_ms: f64,
    /// JWT `sub` 声明
    pub subject: Option<String>,
    /// JWT `account_id` 声明
    pub account_id: Option<String>,
}

/// 解析令牌响应（插件 `token()` 逐条对齐）：
/// `access_token` 与正数 `expires_in` **缺一即整份作废**（返回 Err）——
/// 一份算不出过期时间的凭证落盘后，临期判定永远说「不临期」，只能等 401，
/// 那比当场报错更难查。`refresh_token` 缺失时沿用旧的那枚（刷新响应
/// 偶尔不轮换）。
pub fn parse_token_response(
    body: &Value,
    fallback_refresh: &str,
    now_ms: i64,
) -> Result<TokenSet, GatewayError> {
    let access = text(body, "access_token").ok_or_else(|| {
        GatewayError::with_status(502, "MiniMax 令牌响应缺少 access_token，旧凭证未被覆盖")
    })?;
    let expires_in = body
        .get("expires_in")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| {
            GatewayError::with_status(502, "MiniMax 令牌响应缺少 expires_in，旧凭证未被覆盖")
        })?;
    let claims = jwt_claims(&access);
    let claim_text = |key: &str| {
        claims
            .as_ref()
            .and_then(|claims| claims.get(key))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    Ok(TokenSet {
        access,
        refresh: text(body, "refresh_token").unwrap_or_else(|| fallback_refresh.to_string()),
        expires_at_ms: now_ms as f64 + expires_in * 1000.0,
        subject: claim_text("sub"),
        account_id: claim_text("account_id"),
    })
}

/// 真登出的唯一判据（插件 `signedOut`）：HTTP 400 + `invalid_grant`。
pub fn is_signed_out(status: u16, body: &Value) -> bool {
    status == 400 && text(body, "error").as_deref() == Some("invalid_grant")
}

/// 从账号记录（accounts.json 的一条）解析凭证。
///
/// 落盘键名沿用全仓口径：`accessToken` / `refreshToken` / `expiresAt`；
/// 读取侧兼容 OAuth 原名（`access_token` 等）。过期时间缺失时从 JWT 的
/// `exp` 推；uid 顺序：记录 `uid` → 记录 `account` / `accountId` → JWT
/// `account_id` → JWT `sub`。
pub fn credentials_from_record(
    record: &Value,
    region: Region,
) -> Result<MinimaxCredentials, GatewayError> {
    let access = pick(record, &["accessToken", "access_token", "token"]);
    if access.is_empty() {
        return Err(GatewayError::with_status(
            401,
            "MiniMax Code 账号缺少 accessToken，请重新登录",
        ));
    }
    if access.chars().count() > MAX_TOKEN_LENGTH {
        return Err(GatewayError::with_status(
            400,
            "MiniMax Code 的 accessToken 过长",
        ));
    }
    let refresh = pick(record, &["refreshToken", "refresh_token"]);
    if refresh.chars().count() > MAX_TOKEN_LENGTH {
        return Err(GatewayError::with_status(
            400,
            "MiniMax Code 的 refreshToken 过长",
        ));
    }
    let expires_at = record
        .get("expiresAt")
        .and_then(number)
        .or_else(|| expires_at_from_jwt(&access));
    let claims = jwt_claims(&access);
    let claim = |key: &str| {
        claims
            .as_ref()
            .and_then(|claims| claims.get(key))
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("")
            .to_string()
    };
    let uid = {
        let stored = pick(record, &["uid", "accountId", "account", "userId"]);
        if !stored.is_empty() {
            stored
        } else {
            let account_id = claim("account_id");
            if !account_id.is_empty() {
                account_id
            } else {
                claim("sub")
            }
        }
    };
    let email = {
        let stored = pick(record, &["email"]);
        if !stored.is_empty() {
            stored
        } else {
            claim("email")
        }
    };
    Ok(MinimaxCredentials {
        id: pick(record, &["id"]),
        region,
        access_token: access,
        refresh_token: refresh,
        expires_at,
        uid,
        email,
        name: pick(record, &["displayName", "name"]),
    })
}

/// 解出 JWT 的 payload 声明（非 JWT / 解码失败 → None）。不验签 —— 只读
/// 展示与标识声明，鉴权判定永远是上游那一次请求（与 cline 同口径）。
pub fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.trim().split('.').nth(1)?;
    // base64url 可能带 padding：URL_SAFE_NO_PAD 解码前先剥掉
    let decoded = URL_SAFE_NO_PAD.decode(payload.trim_end_matches('=')).ok()?;
    serde_json::from_slice(&decoded).ok()
}

/// 从 JWT 里解出 `exp`（秒）→ 毫秒时间戳。
pub fn expires_at_from_jwt(token: &str) -> Option<f64> {
    let claims = jwt_claims(token)?;
    let exp = claims.get("exp")?.as_f64()?;
    if exp <= 0.0 {
        return None;
    }
    Some(exp * 1000.0)
}

// ─── PKCE 与随机数 ──────────────────────────────────────────────

/// 生成 PKCE verifier（32 字节随机 → base64url 无填充，插件
/// `randomBytes(32).toString("base64url")` 同形态）。
pub fn generate_verifier() -> Result<String, GatewayError> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|error| {
        GatewayError::with_status(500, format!("生成 PKCE verifier 失败: {error}"))
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// PKCE S256 challenge：`base64url(sha256(verifier 的 ASCII 字节))`，无填充。
pub fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

/// 进程级随机 UUID v4（Mavis 会话 id 的兜底值，形态与插件 `randomUUID()` 一致）。
pub fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::getrandom(&mut bytes).is_err() {
        // 随机源不可用时退回时间戳派生（形态仍是 uuid，只是不再不可预测；
        // 它只做会话标识、不做任何鉴权，退化可接受，但要留痕）
        crate::server::logging::verbose("[MiniMaxCode]", "随机源不可用，会话 id 改用时间戳派生");
        let now = crate::server::logging::now_ms().to_le_bytes();
        bytes[..8].copy_from_slice(&now);
        bytes[8..].copy_from_slice(&now);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8],
        bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// 本地时区偏移秒（JS `new Date().getTimezoneOffset() * -60` 的语义：
/// UTC+8 → 28800）。`X-Mavis-Timezone-Offset` 与签名 query 的
/// `timezone_offset` 共用它。
pub fn local_timezone_offset_seconds() -> i32 {
    chrono::Local::now().offset().local_minus_utc()
}

// ─── OAuth form POST（登录与刷新共用）────────────────────────────

/// 一次 form POST 的结果（状态码 + 已解析的 JSON 体；解析不出给 Null）。
pub struct FormResponse {
    /// HTTP 状态码
    pub status: u16,
    /// 响应体（JSON 解析失败时为 Null，调用方按状态码与 error 字段判定）
    pub body: Value,
}

impl FormResponse {
    /// 插件 `postForm` 的 `ok`：HTTP 2xx **且**响应体没有 `error` 字段。
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status) && text(&self.body, "error").is_none()
    }

    /// 响应体的 `error` 字段（OAuth 错误码）
    pub fn error(&self) -> Option<String> {
        text(&self.body, "error")
    }
}

/// POST 一份 `application/x-www-form-urlencoded`（插件 `postForm`）。
///
/// 登录时机还没有账号上下文，出网走 `egress::client_for(None)`（直连 /
/// 系统口径与 cline 登录相同）。网络失败转 502 并带 egress 的可读细节。
pub async fn post_form(url: &str, fields: &[(&str, &str)]) -> Result<FormResponse, GatewayError> {
    let client = egress::client_for(None);
    let response = client
        .post(url)
        .header("Accept", "application/json")
        .timeout(Duration::from_millis(REQUEST_TIMEOUT_MS))
        .form(fields)
        .send()
        .await
        .map_err(|error| {
            GatewayError::with_status(
                502,
                format!(
                    "MiniMax Code 请求失败: {}",
                    egress::describe_error_detail(&error)
                ),
            )
        })?;
    let status = response.status().as_u16();
    let raw = response.text().await.unwrap_or_default();
    let body = serde_json::from_str(&raw).unwrap_or(Value::Null);
    if body.is_null() && (200..300).contains(&status) {
        return Err(GatewayError::with_status(
            502,
            "MiniMax Code 返回了不可解析的响应（invalid_json_response）",
        ));
    }
    Ok(FormResponse { status, body })
}

/// 取字符串字段（去空白；空串 / 非字符串 → None）
pub(crate) fn text(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// 取候选键里第一个非空字符串（去空白）
fn pick(value: &Value, keys: &[&str]) -> String {
    for key in keys {
        if let Some(found) = text(value, key) {
            return found;
        }
    }
    String::new()
}

/// 数字取值（容忍字符串形态的数字）
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok().filter(|v| v.is_finite()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// 造一枚测试用 JWT（不签名，只给 payload 声明）
    fn fake_jwt(claims: Value) -> String {
        let header = URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap_or_default());
        format!("{header}.{payload}.sig")
    }

    #[test]
    fn pkce_challenge_matches_rfc7636_vector() {
        // RFC 7636 附录 B 的已知向量
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn generated_verifier_round_trips_through_challenge() {
        let verifier = generate_verifier().unwrap_or_default();
        // 32 字节 → 43 个 base64url 字符，且只能含 URL-safe 字符
        assert_eq!(verifier.len(), 43);
        assert!(verifier
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'));
        assert_eq!(pkce_challenge(&verifier).len(), 43);
    }

    #[test]
    fn jwt_claims_reads_sub_account_id_and_exp() {
        let token =
            fake_jwt(json!({ "sub": "user-1", "account_id": "acc-9", "exp": 2_000_000_000_i64 }));
        let claims = jwt_claims(&token).unwrap_or(Value::Null);
        assert_eq!(claims["sub"], "user-1");
        assert_eq!(claims["account_id"], "acc-9");
        assert_eq!(expires_at_from_jwt(&token), Some(2_000_000_000_000.0));
        // 非 JWT 一律 None，不 panic
        assert!(jwt_claims("not-a-jwt").is_none());
        assert!(jwt_claims("").is_none());
    }

    #[test]
    fn token_response_requires_access_and_expires_in() {
        let now = 1_700_000_000_000_i64;
        let token = fake_jwt(json!({ "sub": "user-1", "account_id": "acc-9" }));
        let parsed = parse_token_response(
            &json!({ "access_token": token, "refresh_token": "r2", "expires_in": 3600 }),
            "r1",
            now,
        )
        .unwrap_or_else(|error| panic!("应解析成功: {}", error.message));
        assert_eq!(parsed.refresh, "r2");
        assert_eq!(parsed.expires_at_ms, now as f64 + 3_600_000.0);
        assert_eq!(parsed.subject.as_deref(), Some("user-1"));
        assert_eq!(parsed.account_id.as_deref(), Some("acc-9"));
        // refresh 缺失时沿用旧的那枚
        let kept = parse_token_response(
            &json!({ "access_token": "a.b.c", "expires_in": 60 }),
            "r-old",
            now,
        )
        .unwrap_or_else(|error| panic!("应解析成功: {}", error.message));
        assert_eq!(kept.refresh, "r-old");
        // access / expires_in 缺一即失败
        assert!(parse_token_response(&json!({ "expires_in": 60 }), "", now).is_err());
        assert!(parse_token_response(&json!({ "access_token": "a.b.c" }), "", now).is_err());
        assert!(parse_token_response(
            &json!({ "access_token": "a.b.c", "expires_in": 0 }),
            "",
            now
        )
        .is_err());
    }

    #[test]
    fn signed_out_is_only_400_invalid_grant() {
        assert!(is_signed_out(400, &json!({ "error": "invalid_grant" })));
        assert!(!is_signed_out(
            400,
            &json!({ "error": "temporarily_unavailable" })
        ));
        assert!(!is_signed_out(401, &json!({ "error": "invalid_grant" })));
        assert!(!is_signed_out(500, &json!({ "error": "invalid_grant" })));
    }

    #[test]
    fn credentials_from_record_prefers_stored_uid_then_jwt() {
        let token =
            fake_jwt(json!({ "sub": "user-1", "account_id": "acc-9", "exp": 2_000_000_000_i64 }));
        let record = json!({
            "id": "minimax-code-acc-9",
            "accessToken": token,
            "refreshToken": "r1",
        });
        let credentials = credentials_from_record(&record, Region::Cn)
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(credentials.uid, "acc-9");
        assert_eq!(credentials.expires_at, Some(2_000_000_000_000.0));
        assert!(credentials.can_refresh());
        // 记录显式 uid 优先于 JWT
        let record2 = json!({ "accessToken": token, "uid": "stored-uid" });
        let credentials2 = credentials_from_record(&record2, Region::Global)
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(credentials2.uid, "stored-uid");
        // 缺 accessToken → 401
        assert_eq!(
            credentials_from_record(&json!({}), Region::Cn)
                .unwrap_err()
                .status_code,
            401
        );
    }
}
