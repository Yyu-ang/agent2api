//! Tabbit 网页登录：**官方登录页** + 壳侧读 Cookie 收尾。
//!
//! ── 为什么是这套流程 ────────────────────────────────────────
//! Tabbit 的登录是浏览器 OAuth（Google / Apple 第三方登录都在官方页面上，
//! 参考实现用「纯净模式」真浏览器就是为了过 Google 的风控）。纯 HTTP 复刻
//! 登录流程既不稳定也容易触发风控 —— 把「怎么登录」还给用户：打开官方登录页，
//! 用户正常登录（官方页面自己处理风控），壳侧从登录窗口的 Cookie 存储读
//! `token`（HttpOnly，页面脚本读不到）交回。
//!
//! ── 流程 ──────────────────────────────────────────────────
//!   1. 适配器 `build_login_url` 返回地区主站地址（国内 `web.tabbit.com` /
//!      国际 `web.tabbit.ai`）+ 一次性 state（`new_login_state`）；
//!   2. 壳侧打开内嵌窗口（可见），用户在其中完成登录；
//!   3. 壳侧从窗口 Cookie 存储读 `token`（含 HttpOnly），拼成 Cookie 头
//!      `POST /api/session/login/tabbit/complete`（`state` + `cookie` 在请求体）；
//!   4. 后端 `complete_login` 用 `base-info` 复核并取昵称/uid，落账号
//!      （见 `core::login::finish_tabbit_login`）。
//!
//! ── 安全边界 ────────────────────────────────────────────────
//! 收尾路由**免鉴权**（调用方是壳侧进程的登录窗口，不带 API Key），安全性由
//! 登录任务的一次性 state 承担（与 kuku / raccoon 回调同口径）。cookie 走本机
//! loopback 请求体，一次性传输、任务结束后即作废。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use crate::server::errors::GatewayError;

use super::credentials::{self, TabbitCredentials};
use super::region::Region;

/// 生成登录任务 state（不可预测的随机串）。
pub fn new_login_state() -> String {
    super::protocol::public_random_hex(32)
}

/// Tabbit 登录页地址：地区主站（用户像平时用网页版一样登录）。
pub fn build_login_url(region: Region, _state: &str) -> String {
    region.login_url().to_string()
}

/// 复核结果：凭证（登录收尾把警告透传给前端暂不需要，本家复核失败直接 400）。
pub type CompleteOutcome = TabbitCredentials;

/// 从壳侧交回的 Cookie 串里提取登录态，并用 `base-info` 复核。
///
/// 壳侧交回的是登录窗口的 Tabbit 域 Cookie（至少含 HttpOnly 的 `token`；
/// `user_id` 有就一起带，没有就从 JWT 解析）。
pub async fn complete_login(region: Region, cookie: &str) -> Result<CompleteOutcome, GatewayError> {
    let credentials = credentials::credentials_from_paste(cookie, region).map_err(|reason| {
        GatewayError::with_status(
            400,
            format!("登录窗口没有捕获到有效的 token（Tabbit 登录态），请重新发起登录：{reason}"),
        )
    })?;
    // 复核：凭证无效（过期/伪造）→ 400，不落空记录
    match credentials::enrich_identity(&credentials, None).await {
        Ok(enriched) => Ok(enriched),
        Err(error) => Err(GatewayError::with_status(
            400,
            format!("Tabbit 登录态复核未通过（{}），请重新登录", error.message),
        )),
    }
}
