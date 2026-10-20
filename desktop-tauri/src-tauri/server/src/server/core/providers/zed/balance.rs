//! Zed 的用量（账号页「积分」读数）：套餐名 + 账期结束时间，**没有已用量
//! 窗口**（插件 `usage` 的对应物，源头是 magpie 内置的 zed_usage.go）。
//!
//! ── 为什么只有这两样 ──────────────────────────────────────
//! Zed 只告诉它的编辑器「这个账号是什么套餐、账期到哪天」，不报已用
//! 额度 —— 因此 `available` 如实给 null、`wallets` / `windows` 都不给，
//! 不许编一个用量数字出来（与 zcode「窗口比例不是余额数字、available
//! 保持 null」同一口径）。前端按统一形状渲染：套餐名在
//! `subscription.planName`，账期结束在 `subscription.expireAt`（毫秒）。
//!
//! ── 读这一趟顺手做的两件事（插件 usage 逐字）───────────────
//!   1. 组织/套餐按 users/me 的当前事实重新选一遍（`org_of` 以已保存的
//!      组织为起点），变了就回写账号 —— 账号在 Zed 侧换了套餐组织时，
//!      网关这边跟着走，不必等转发撞上 402 才 repick；
//!   2. `plan.has_overdue_invoices` 为真时，模型已被 Zed 暂停：整条读数
//!      作 error 报出（文案逐字），而不是给一个看起来正常的套餐名。
//!
//! ── 401 ────────────────────────────────────────────────────
//! users/me 回 401 = 登录过期，按适配器契约原样透出 401
//! （`GatewayError::with_status(401, …)`）；本家 `supports_refresh`
//! 为 false，调用方不会拿它去走刷新重试（见 `core::usage_query`）。

use std::time::Duration;

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::errors::GatewayError;

use super::credentials::{org_of, plan_name, plan_of, Credential};
use super::http::{self, CloudError};

/// 逾期发票的文案（插件逐字）。
const OVERDUE_MESSAGE: &str =
    "Zed: this account has an overdue invoice, so its models are paused (see zed.dev/account)";

/// 账期结束时间是不是 RFC3339 形状（插件用正则 + Date.parse 双检；本仓
/// 用 chrono 的 RFC3339 解析一检到位，形状不对就不给 expireAt）。
fn rfc3339_to_ms(text: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|moment| moment.timestamp_millis())
}

/// 查询一个账号的用量（适配器 `query_usage` 的实现体）。
pub async fn query_usage(store: &AccountStore, account_id: &str) -> Result<Value, GatewayError> {
    let record = store.zed_account_record(account_id).ok_or_else(|| {
        GatewayError::with_status(404, "Zed 账号不存在或不可用")
    })?;
    let resolved_id = if account_id.is_empty() {
        record
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    } else {
        account_id.to_string()
    };
    let credential = Credential::from_record(&record).ok_or_else(|| {
        GatewayError::with_status(401, "Zed 账号缺少访问令牌或 userId，请重新登录")
    })?;
    let me = http::users_me(&credential, None, Duration::from_secs(15))
        .await
        .map_err(|error| match &error {
            CloudError::Status(failure) if failure.status == 401 => GatewayError::with_status(
                401,
                format!(
                    "{}：Zed 登录已过期，请重新登录",
                    if credential.display.is_empty() {
                        "该账号".to_string()
                    } else {
                        credential.display.clone()
                    }
                ),
            ),
            _ => GatewayError::with_status(
                i32::from(error.status().unwrap_or(502)),
                format!("Zed：读取账号信息失败（{}）", error.message()),
            ),
        })?;

    // 组织/套餐重选与回写（插件：org 变了 move 并保存；只有 plan 变了也保存）
    let org = org_of(&me, &credential.org);
    let plan = plan_of(&me, &org);
    if org != credential.org || plan != credential.plan {
        let _ = store.update_zed_organization(&resolved_id, &org, &plan);
    }

    // 逾期发票：模型已暂停，整条读数作 error（见模块头）
    let overdue = me
        .get("plan")
        .and_then(|plan| plan.get("has_overdue_invoices"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if overdue {
        return Err(GatewayError::with_status(402, OVERDUE_MESSAGE));
    }

    // 套餐展示名：账号行里 Free 就叫 Free，但卡片上 Free/无套餐显示
    // 「No plan」（插件的卡片口径逐字）
    let display_plan = if plan.is_empty() || plan == "zed_free" {
        "No plan".to_string()
    } else {
        plan_name(&plan)
    };
    let mut subscription = serde_json::Map::new();
    subscription.insert("planName".to_string(), Value::String(display_plan));
    if let Some(ended_at) = me
        .get("plan")
        .and_then(|plan| plan.get("subscription_period"))
        .and_then(|period| period.get("ended_at"))
        .and_then(Value::as_str)
        .and_then(rfc3339_to_ms)
    {
        subscription.insert("expireAt".to_string(), Value::from(ended_at));
    }

    Ok(json!({
        // Zed 不报已用量：如实 null（见模块头），不编数字
        "available": Value::Null,
        "unit": "套餐",
        "wallets": [],
        "subscription": Value::Object(subscription),
        "raw": me,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_parsing_accepts_zed_shape_only() {
        assert!(rfc3339_to_ms("2026-11-01T00:00:00Z").is_some());
        assert!(rfc3339_to_ms("2026-11-01T00:00:00.123+08:00").is_some());
        assert!(rfc3339_to_ms("2026-11-01").is_none());
        assert!(rfc3339_to_ms("soon").is_none());
    }
}
