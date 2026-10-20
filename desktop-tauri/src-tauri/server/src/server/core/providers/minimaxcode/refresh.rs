//! MiniMax Code 凭证续期：`POST {account}/oauth2/token`（refresh_token grant）
//! + 单飞 + 比较再写。
//!
//! ── 为什么单飞是本家的硬要求（不是优化）──────────────────────
//! refresh token **单次使用**：花掉就换新，旧的再发一次就是
//! `invalid_grant`、上游会把账号判成登出（插件 `renew` 的注释逐字说明了
//! 这条，mcode 自己也要跨进程加锁防它）。网关里转发、用量、签到、维护
//! 任务都可能在同一时刻发现 token 临期——各自刷一次，第二发就把账号
//! 刷下线。因此走仓库共用的 `refresh_flight::Table`（键含账号 id 与
//! **双 token 指纹**，凭证一变就是新的一轮），与 cline / qoder 同一设施。
//!
//! ── 失败口径（插件 `signedOut` / `fresh` 逐条对齐）──────────────
//!   - 只有 **400 + invalid_grant** 是真登出 → 401 终态，调用方如实让
//!     用户重新登录；
//!   - 其余失败（5xx / 网络 / 其它 error）都是暂态 → 502，**登录态保留**；
//!     非强制路径上若手里的 access 还没真过期，`ensure_fresh` 直接沿用
//!     它（插件 `fresh` 的「passing: the token in hand may still do」）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：绝不 unwrap/expect/panic；不持锁跨 await
//! （快照在 await 前取完，回写在 await 之后走账号层的比较-再写）。

use std::sync::OnceLock;

use serde_json::Value;

use crate::server::core::account_store::{AccountStore, CredentialWrite};
use crate::server::core::providers::refresh_flight::{self, Join, Table};
use crate::server::errors::GatewayError;
use crate::server::logging;

use super::credentials::{self, MinimaxCredentials, TokenSet};
use super::region::Region;

static FLIGHTS: OnceLock<Table<MinimaxCredentials>> = OnceLock::new();

/// 取某账号的凭证快照（读账号记录 → 解析成凭证形态）。
///
/// `account_id` 为空 → 取该地区组内优先级最小的启用账号；点名了却找不到
/// → 401 如实报（不回落到队首，那会变成「选了 A、用的是 B」）。
pub fn snapshot(
    store: &AccountStore,
    region: Region,
    account_id: &str,
) -> Result<MinimaxCredentials, GatewayError> {
    let record = store.minimaxcode_account_record(region.provider_id(), account_id);
    if !account_id.is_empty() && record.is_none() {
        return Err(GatewayError::with_status(
            401,
            format!("MiniMax Code 账号 {account_id} 不存在或不属于该地区（请重新添加）"),
        ));
    }
    let Some(record) = record else {
        return Err(GatewayError::with_status(
            401,
            "没有可用的 MiniMax Code 登录态（请先添加账号）",
        ));
    };
    credentials::credentials_from_record(&record, region)
}

/// 取可用凭证（临期主动刷新，窗口 2 分钟见 `credentials`）；`force = true`
/// 时不看临期窗口直接续期（401 之后的强制刷新走这条）。
pub async fn ensure_fresh(
    store: &AccountStore,
    region: Region,
    account_id: &str,
    force: bool,
) -> Result<MinimaxCredentials, GatewayError> {
    let credentials = snapshot(store, region, account_id)?;
    if !force && !credentials.is_expiring() {
        return Ok(credentials);
    }
    if !credentials.can_refresh() {
        // 没 refresh token：手里这枚还没过期就沿用（插件同口径）；真过期了
        // 才是终态 401 —— 不是错误文案的差别，是「还能不能用」的事实差别。
        if !credentials.is_expired_at(crate::server::logging::now_ms()) {
            return Ok(credentials);
        }
        return Err(GatewayError::with_status(
            401,
            "MiniMax Code 登录已过期且没有 refreshToken，请重新登录",
        ));
    }
    let key = format!(
        "{}:{}:{}:{}",
        store.file_string(),
        credentials.id,
        refresh_flight::fingerprint(&credentials.access_token),
        refresh_flight::fingerprint(&credentials.refresh_token),
    );
    match FLIGHTS.get_or_init(Table::new).join(&key) {
        Join::Waiter(waiter) => waiter.wait().await,
        Join::Leader(leader) => {
            let result = refresh_and_save(store, &credentials).await;
            // 非强制路径的暂态失败：手里这枚还没真过期就沿用它（登录态保留，
            // 见模块头）。强制路径（401 后）不沿用 —— 上游已经明确拒过它。
            let result = match result {
                Err(error)
                    if !force
                        && error.status_code != 401
                        && !credentials.is_expired_at(crate::server::logging::now_ms()) =>
                {
                    logging::verbose(
                        "[MiniMaxCode]",
                        &format!("续期暂态失败，沿用未过期凭证: {}", error.message),
                    );
                    Ok(credentials.clone())
                }
                other => other,
            };
            leader.finish(result.clone());
            result
        }
    }
}

/// 真的去打一次续期，并把结果比较-再写回账号文件。
async fn refresh_and_save(
    store: &AccountStore,
    credentials: &MinimaxCredentials,
) -> Result<MinimaxCredentials, GatewayError> {
    let fresh = refresh_request(credentials).await?;
    persist_refresh(store, credentials, &fresh);
    Ok(fresh)
}

/// 打一次续期请求（不落盘，纯网络 + 解析）。
async fn refresh_request(
    credentials: &MinimaxCredentials,
) -> Result<MinimaxCredentials, GatewayError> {
    let region = credentials.region;
    let url = format!("{}/oauth2/token", region.account_base());
    let response = credentials::post_form(
        &url,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", credentials.refresh_token.as_str()),
            ("client_id", credentials::CLIENT_ID),
            ("scope", credentials::SCOPE),
            ("audience", credentials::AUDIENCE),
        ],
    )
    .await?;
    if !response.ok() {
        if credentials::is_signed_out(response.status, &response.body) {
            // 唯一终态：refresh 已被作废（可能已被别处花掉并轮换，而本进程
            // 没跟上）—— 如实让用户重新登录，不重试、不沿用。
            return Err(GatewayError::with_status(
                401,
                "MiniMax Code 的登录态已失效（refresh token 已被上游作废），请重新登录",
            ));
        }
        let detail = response
            .error()
            .unwrap_or_else(|| format!("HTTP {}", response.status));
        return Err(GatewayError::with_status(
            502,
            format!("MiniMax Code 续期失败（{}）: {detail}", response.status),
        ));
    }
    let tokens: TokenSet = credentials::parse_token_response(
        &response.body,
        &credentials.refresh_token,
        crate::server::logging::now_ms(),
    )?;
    let mut fresh = credentials.clone();
    fresh.access_token = tokens.access.clone();
    fresh.refresh_token = tokens.refresh.clone();
    fresh.expires_at = Some(tokens.expires_at_ms);
    if fresh.uid.is_empty() {
        fresh.uid = tokens
            .account_id
            .clone()
            .or(tokens.subject.clone())
            .unwrap_or_default();
    }
    Ok(fresh)
}

/// 刷新成功后回写（比较-再写，纪律与 cline 同款）：记录里仍是刷新前那份
/// 凭证才写；已被替换（用户重登 / 另一轮先落地）就作废本次结果 ——
/// 尤其是 refresh 已轮换时，拿旧结果覆盖会把新登录态冲掉。
///
/// 回写失败只记日志不报错：新凭证已在本进程手里（本次调用能用），
/// 落盘失败的后果是重启后回到旧凭证、再刷一次（旧 refresh 若已被花掉
/// 会走 invalid_grant 终态，那时用户重登即可——比在这里把一次成功的
/// 续期报成失败更诚实）。
fn persist_refresh(store: &AccountStore, before: &MinimaxCredentials, fresh: &MinimaxCredentials) {
    if before.id.is_empty() {
        return;
    }
    match store.update_minimaxcode_account_tokens_if_current(
        &before.id,
        &before.access_token,
        &before.refresh_token,
        &fresh.access_token,
        &fresh.refresh_token,
        fresh.expires_at,
    ) {
        Ok(CredentialWrite::Written) => {
            logging::verbose(
                "[MiniMaxCode]",
                &format!("账号 {} 的凭证已续期并回写", before.id),
            );
        }
        Ok(CredentialWrite::Stale) => {
            logging::verbose(
                "[MiniMaxCode]",
                &format!(
                    "账号 {} 的凭证在续期期间已被替换，本次结果未回写",
                    before.id
                ),
            );
        }
        Err(message) => {
            logging::log(
                "[MiniMaxCode]",
                &format!("账号 {} 的凭证回写失败: {message}", before.id),
            );
        }
    }
}
