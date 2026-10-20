//! MiniMax Code **设备授权登录**（OAuth 设备流 + PKCE S256）。
//!
//! ── 协议（来源：社区插件 `@magpie-community/opencode-minimax-auth` v0.1.2
//! 的 `startDevice` / `pollDevice`，逐条对齐）────────────────────
//! ```text
//! ① POST {account}/oauth2/device/code   （form）
//!      client_id=mcode-public, scope=agent.default, audience=agent-backend,
//!      code_challenge=<S256>, code_challenge_method=S256
//!    → { user_code, device_code?, verification_uri(_complete)?,
//!        expires_in?, expired_in?, interval? }
//!
//! ② 用户在浏览器打开 verification_uri_complete 并确认
//!
//! ③ POST {account}/oauth2/token          （form，轮询）
//!      grant_type=urn:ietf:params:oauth:grant-type:device_code
//!      + device_code（或 user_code，见下）+ client_id + code_verifier
//!    → 成功：{ access_token, refresh_token, expires_in }
//! ```
//!
//! ── MiniMax 的三个变体（标准 RFC 8628 之外，插件实测）──────────
//!   1. **可能没有 `device_code`**：此时改按 `user_code` 轮询（判据是
//!      「无 device_code 且有 user_code 且有 `expired_in`」）；
//!   2. **`expired_in` 可能是到期毫秒时间戳**（≥1e12 时按时间戳换算成
//!      剩余秒数；<1e12 时按秒数时长）；`expires_in` 缺失时靠它兜底，
//!      再缺就用 10 分钟缺省（插件 `SIGN_IN_MS`）；
//!   3. **`interval` 可能是毫秒**：仅在「按 user_code 轮询」形态下按毫秒
//!      用，其余形态按秒 ×1000。轮询状态除了标准 OAuth `error`
//!      （`authorization_pending` / `slow_down`）外，还有响应体 `status`
//!      字段形态（`pending` / `slow_down` / `denied` / `expired`），
//!      `slow_down` 一律在当前间隔上 **+5 秒**。
//!
//! ── 登录后不调 identity（v1 范围，见 `super` 模块头）────────────
//! 插件在轮询成功后还会调签名账号 API 的 `/v1/api/user/info` 补
//! realUserID / email / name。v1 跳过它：账号标识用令牌 JWT 的
//! `account_id` / `sub`，展示名用邮箱声明（有才有）或站点名兜底。
//! 签名账号 API 整体列为后续项，不在这里半接。
//!
//! ── 落账号走既有入口 ────────────────────────────────────────
//! 与 cline 同一纪律：成功后调 `AccountStore::add_minimaxcode_account`，
//! 不另写一份落盘逻辑。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：绝不 unwrap/expect/panic；所有失败转
//! `GatewayError`。轮询循环不持任何锁。

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::errors::GatewayError;
use crate::server::logging;

use super::credentials::{self, TokenSet};
use super::region::Region;

/// 缺省轮询间隔（上游没给 `interval` 时，插件同值 5 秒）
const DEFAULT_INTERVAL_SECONDS: u64 = 5;
/// 缺省有效期（上游 `expires_in` / `expired_in` 都没有时，插件 `SIGN_IN_MS` = 10 分钟）
const DEFAULT_EXPIRES_SECONDS: u64 = 600;
/// `slow_down` 时在当前间隔上追加的量（插件同值 5 秒）
const SLOW_DOWN_EXTRA_MS: u64 = 5_000;

/// 轮询时带哪个码（MiniMax 变体：有 device_code 用它，没有就用 user_code）
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PollKey {
    /// 标准形态：`device_code=<…>`
    DeviceCode(String),
    /// MiniMax 变体：`user_code=<…>`
    UserCode(String),
}

impl PollKey {
    /// 拼进轮询 form 的那一对字段
    fn field(&self) -> (&'static str, &str) {
        match self {
            PollKey::DeviceCode(code) => ("device_code", code),
            PollKey::UserCode(code) => ("user_code", code),
        }
    }
}

/// 设备授权的第一步产物（给界面展示「去哪儿确认、输什么码」用）。
#[derive(Clone, Debug)]
pub struct DeviceAuthStart {
    /// PKCE verifier（只在本进程内保管，轮询时用；不进界面、不落盘）
    pub verifier: String,
    /// 用户码（界面展示给用户核对）
    pub user_code: String,
    /// 轮询键（device_code 或 user_code 变体）
    pub poll: PollKey,
    /// 授权页（优先 `verification_uri_complete`，已带 user_code）
    pub verification_uri: String,
    /// 纯授权页（没有 complete 时与上面相同）
    pub verification_uri_base: String,
    /// 授权有效期（秒）
    pub expires_in_seconds: u64,
    /// 初始轮询间隔（毫秒）
    pub interval_ms: u64,
    /// 轮询截止时刻（毫秒时间戳）
    pub deadline_ms: i64,
}

/// 发起设备授权（第一步）。
pub async fn start(region: Region) -> Result<DeviceAuthStart, GatewayError> {
    let verifier = credentials::generate_verifier()?;
    let challenge = credentials::pkce_challenge(&verifier);
    let url = format!("{}/oauth2/device/code", region.account_base());
    let response = credentials::post_form(
        &url,
        &[
            ("client_id", credentials::CLIENT_ID),
            ("scope", credentials::SCOPE),
            ("audience", credentials::AUDIENCE),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
        ],
    )
    .await?;
    if !response.ok() {
        let detail = response
            .error()
            .unwrap_or_else(|| format!("HTTP {}", response.status));
        return Err(GatewayError::with_status(
            502,
            format!("MiniMax Code 设备授权失败（{}）: {detail}", response.status),
        ));
    }
    parse_device_start(&response.body, verifier, crate::server::logging::now_ms())
}

/// 解析设备授权响应（纯函数，MiniMax 三个变体都在这里收口，见模块头）。
pub fn parse_device_start(
    body: &Value,
    verifier: String,
    now_ms: i64,
) -> Result<DeviceAuthStart, GatewayError> {
    let invalid = || {
        GatewayError::with_status(
            502,
            "MiniMax Code 设备授权响应不完整（invalid_device_authorization_response）",
        )
    };
    let user_code = credentials::text(body, "user_code").ok_or_else(invalid)?;
    let device_code = credentials::text(body, "device_code");
    let expired_in = positive(body, "expired_in");
    // 变体判据（插件逐字）：无 device_code + 有 user_code + 有 expired_in
    let by_user = device_code.is_none() && expired_in.is_some();
    // expired_in 换算成「剩余秒数」：≥1e12 是到期毫秒时间戳，<1e12 是秒数时长
    let derived_seconds = expired_in.map(|value| {
        if value < 1e12 {
            value as u64
        } else {
            let remaining_ms = value - now_ms as f64;
            if remaining_ms <= 0.0 {
                0
            } else {
                (remaining_ms / 1000.0).ceil() as u64
            }
        }
    });
    let expires_in_seconds = positive(body, "expires_in")
        .map(|value| value as u64)
        .or(derived_seconds)
        .unwrap_or(DEFAULT_EXPIRES_SECONDS);
    let poll = match &device_code {
        Some(code) => PollKey::DeviceCode(code.clone()),
        None if by_user => PollKey::UserCode(user_code.clone()),
        None => return Err(invalid()),
    };
    let uri = credentials::text(body, "verification_uri")
        .or_else(|| credentials::text(body, "verification_url"))
        .ok_or_else(invalid)?;
    let complete = credentials::text(body, "verification_uri_complete");
    let interval = positive(body, "interval");
    // interval 单位（插件逐字）：user_code 形态按毫秒，其余按秒
    let interval_ms = match (by_user, interval) {
        (true, Some(value)) => value as u64,
        (_, Some(value)) => (value as u64).saturating_mul(1000),
        (_, None) => DEFAULT_INTERVAL_SECONDS * 1000,
    }
    .max(1);
    Ok(DeviceAuthStart {
        verifier,
        user_code,
        poll,
        verification_uri: complete.unwrap_or_else(|| uri.clone()),
        verification_uri_base: uri,
        expires_in_seconds,
        interval_ms,
        deadline_ms: now_ms.saturating_add((expires_in_seconds as i64).saturating_mul(1000)),
    })
}

/// 一次轮询的归类结果（纯判定，插件 `pollDevice` 的分支逐条对应）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PollStep {
    /// 还没确认，继续等
    Pending,
    /// 太快了，间隔 +5 秒后继续
    SlowDown,
    /// 用户拒绝了（终止）
    Denied,
    /// 码已过期（终止）
    Expired,
    /// 成功，响应体交给 `parse_token_response` 解析
    Success,
    /// 其它错误（终止，带 OAuth 错误码）
    Failed(String),
}

/// 归类一次轮询响应。顺序照插件：先看「成功形态」的 `status` 字段，
/// 再看标准 OAuth `error` 字段。
pub fn classify_poll(status: u16, body: &Value) -> PollStep {
    let ok = (200..300).contains(&status) && credentials::text(body, "error").is_none();
    if ok {
        return match credentials::text(body, "status").as_deref() {
            Some("pending") => PollStep::Pending,
            Some("slow_down") => PollStep::SlowDown,
            Some("denied") | Some("access_denied") => PollStep::Denied,
            Some("expired") | Some("expired_token") => PollStep::Expired,
            _ => PollStep::Success,
        };
    }
    match credentials::text(body, "error").as_deref() {
        Some("authorization_pending") => PollStep::Pending,
        Some("slow_down") => PollStep::SlowDown,
        Some(other) => PollStep::Failed(other.to_string()),
        None => PollStep::Failed("device_authorization_failed".to_string()),
    }
}

/// 轮询直到用户确认，成功后**落账号**并返回账号 id。
///
/// 会阻塞到「确认 / 拒绝 / 超时 / 取消」之一：调用方（`core::login` 的
/// 后台任务）本来就在等一个异步结果，因此这里是 await 循环（与 cline
/// 的 `poll_and_register` 同一形态）。
pub async fn poll_and_register(
    store: &AccountStore,
    region: Region,
    start: &DeviceAuthStart,
    name: Option<&str>,
) -> Result<String, GatewayError> {
    let tokens = poll_tokens(region, start).await?;
    // 落账号走既有入口（与「粘贴凭证」同一条路径）。identity 接口跳过
    // （v1 范围）：uid 用 JWT 的 account_id → sub，展示名由账号层按
    // 邮箱声明 → 站点名兜底。
    let mut payload = json!({
        "accessToken": tokens.access,
        "refreshToken": tokens.refresh,
        "expiresAt": tokens.expires_at_ms,
    });
    if let Some(uid) = tokens.account_id.clone().or(tokens.subject.clone()) {
        if let Some(object) = payload.as_object_mut() {
            object.insert("uid".to_string(), Value::String(uid));
        }
    }
    let account = store
        .add_minimaxcode_account(region.provider_id(), &payload, name)
        .map_err(|error| GatewayError::with_status(error.status_code, error.message))?;
    let id = account
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    logging::log(
        "[Login]",
        &format!(
            "✅ {} 登录成功，账号已加入列表",
            crate::server::core::providers::label_of(region.provider_id())
        ),
    );
    Ok(id)
}

/// 轮询换令牌（第三步）。先睡一个间隔再问（插件 `pollDevice` 同款节奏）。
async fn poll_tokens(region: Region, start: &DeviceAuthStart) -> Result<TokenSet, GatewayError> {
    let url = format!("{}/oauth2/token", region.account_base());
    let (poll_key, poll_value) = start.poll.field();
    let mut wait_ms = start.interval_ms;
    loop {
        if crate::server::logging::now_ms() >= start.deadline_ms {
            return Err(GatewayError::with_status(
                400,
                "MiniMax Code 登录码已过期（expired_token），请重新发起登录",
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
        let response = credentials::post_form(
            &url,
            &[
                ("grant_type", credentials::DEVICE_GRANT),
                (poll_key, poll_value),
                ("client_id", credentials::CLIENT_ID),
                ("code_verifier", start.verifier.as_str()),
            ],
        )
        .await?;
        match classify_poll(response.status, &response.body) {
            PollStep::Pending => continue,
            PollStep::SlowDown => {
                wait_ms = wait_ms.saturating_add(SLOW_DOWN_EXTRA_MS);
                continue;
            }
            PollStep::Denied => {
                return Err(GatewayError::with_status(
                    400,
                    "MiniMax Code 登录被拒绝（access_denied），请重新发起登录",
                ));
            }
            PollStep::Expired => {
                return Err(GatewayError::with_status(
                    400,
                    "MiniMax Code 登录码已过期（expired_token），请重新发起登录",
                ));
            }
            PollStep::Failed(code) => {
                return Err(GatewayError::with_status(
                    502,
                    format!("MiniMax Code 设备授权失败: {code}"),
                ));
            }
            PollStep::Success => {
                return credentials::parse_token_response(
                    &response.body,
                    "",
                    crate::server::logging::now_ms(),
                );
            }
        }
    }
}

/// 正数数值字段（插件 `pos()`：有限正数才算数）
fn positive(body: &Value, key: &str) -> Option<f64> {
    body.get(key)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const NOW: i64 = 1_700_000_000_000;

    #[test]
    fn standard_device_response_uses_device_code_and_seconds_interval() {
        let start = parse_device_start(
            &json!({
                "user_code": "ABCD-EFGH",
                "device_code": "dev-123",
                "verification_uri": "https://account.minimax.cn/device",
                "verification_uri_complete": "https://account.minimax.cn/device?user_code=ABCD-EFGH",
                "expires_in": 300,
                "interval": 5,
            }),
            "verifier".to_string(),
            NOW,
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(start.poll, PollKey::DeviceCode("dev-123".to_string()));
        assert_eq!(
            start.verification_uri,
            "https://account.minimax.cn/device?user_code=ABCD-EFGH"
        );
        assert_eq!(start.interval_ms, 5_000);
        assert_eq!(start.deadline_ms, NOW + 300_000);
    }

    #[test]
    fn user_code_variant_with_ms_timestamp_and_ms_interval() {
        // MiniMax 变体：没有 device_code、expired_in 是到期毫秒时间戳、interval 是毫秒
        let start = parse_device_start(
            &json!({
                "user_code": "WXYZ-1234",
                "verification_uri": "https://account.minimax.io/device",
                "expired_in": NOW + 240_000,
                "interval": 3000,
            }),
            "verifier".to_string(),
            NOW,
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(start.poll, PollKey::UserCode("WXYZ-1234".to_string()));
        assert_eq!(start.expires_in_seconds, 240);
        assert_eq!(start.interval_ms, 3_000);
        assert_eq!(start.deadline_ms, NOW + 240_000);
    }

    #[test]
    fn expired_in_as_seconds_duration_is_used_when_expires_in_missing() {
        let start = parse_device_start(
            &json!({
                "user_code": "U1",
                "device_code": "d1",
                "verification_uri": "https://account.minimax.cn/device",
                "expired_in": 180,
            }),
            "verifier".to_string(),
            NOW,
        )
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(start.expires_in_seconds, 180);
    }

    #[test]
    fn incomplete_device_response_is_rejected() {
        // 没有 device_code 又没有 expired_in（构不成 user_code 变体）
        assert!(parse_device_start(
            &json!({ "user_code": "U1", "verification_uri": "https://x" }),
            "v".to_string(),
            NOW,
        )
        .is_err());
        // 缺 user_code / verification_uri
        assert!(parse_device_start(
            &json!({ "device_code": "d", "verification_uri": "https://x" }),
            "v".to_string(),
            NOW,
        )
        .is_err());
    }

    #[test]
    fn poll_status_field_variants_are_classified() {
        assert_eq!(
            classify_poll(200, &json!({ "status": "pending" })),
            PollStep::Pending
        );
        assert_eq!(
            classify_poll(200, &json!({ "status": "slow_down" })),
            PollStep::SlowDown
        );
        assert_eq!(
            classify_poll(200, &json!({ "status": "denied" })),
            PollStep::Denied
        );
        assert_eq!(
            classify_poll(200, &json!({ "status": "access_denied" })),
            PollStep::Denied
        );
        assert_eq!(
            classify_poll(200, &json!({ "status": "expired" })),
            PollStep::Expired
        );
        // 成功形态：没有 status 字段、带令牌
        assert_eq!(
            classify_poll(200, &json!({ "access_token": "a.b.c", "expires_in": 60 })),
            PollStep::Success
        );
    }

    #[test]
    fn poll_standard_oauth_errors_are_classified() {
        assert_eq!(
            classify_poll(400, &json!({ "error": "authorization_pending" })),
            PollStep::Pending
        );
        assert_eq!(
            classify_poll(400, &json!({ "error": "slow_down" })),
            PollStep::SlowDown
        );
        assert_eq!(
            classify_poll(400, &json!({ "error": "access_denied" })),
            PollStep::Failed("access_denied".to_string())
        );
        assert_eq!(
            classify_poll(500, &json!({})),
            PollStep::Failed("device_authorization_failed".to_string())
        );
    }
}
