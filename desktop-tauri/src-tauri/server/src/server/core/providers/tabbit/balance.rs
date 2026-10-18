//! Tabbit 余额查询：`GET /api/commerce/quota/v1/usage?user_id=<uid>`。
//!
//! ── 接口事实（tabbit2api 实测）───────────────────────────────
//! 返回上游**真实**的会员等级 / 额度百分比 / 周期（`member_level` /
//! `usage_percentage`），面板展示的会员标识全部来自上游真实数据，
//! **不伪造任何会员标识**（参考实现「合规」节）。
//!
//! ── 归一化形状（`ProviderAdapter::query_usage` 的契约）────────
//! `{available, unit, wallets, subscription, raw}`。Tabbit 的额度和模型强相关
//! （各模型的 `display_multiplier` 倍率），这里给会员等级 + 已用百分比；
//! `available` 判不出给 null（上游只给百分比，不给绝对值）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use serde_json::{Value, json};

use crate::server::core::account_store::AccountStore;
use crate::server::errors::GatewayError;

use super::credentials;
use super::region::Region;

/// 余额请求超时（给 15 秒；参考实现同量级）
const BALANCE_TIMEOUT_MS: u64 = 15_000;

/// 查询某账号的余额 / 额度（归一化形状见 `ProviderAdapter::query_usage` 文档）。
pub(super) async fn query_usage(
    region: Region,
    store: &AccountStore,
    account_id: &str,
) -> Result<Value, GatewayError> {
    let credentials = credentials::snapshot_for(region, store, account_id)?;
    let url = format!(
        "{}/api/commerce/quota/v1/usage?user_id={}",
        region.base_url(),
        credentials.user_id
    );
    let headers = super::protocol::base_headers(region, &credentials, "/");
    let account = store.tabbit_account_record(region.provider_id(), account_id);
    let resolution = crate::server::core::proxies::resolve_account_proxy(
        account.as_ref().and_then(|record| record.get("proxy")),
    );
    let proxy = resolution.as_ref().and_then(|item| item.resolved());
    let value = super::http::get_json_value(&url, &headers, proxy, Some(BALANCE_TIMEOUT_MS)).await?;
    // 宽松读取：member_level / usage_percentage / 周期（字段名以实测为准，
    // 读不到给空串，不伪造）
    let member_level = value
        .get("member_level")
        .or_else(|| value.get("memberLevel"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let usage_percentage = value
        .get("usage_percentage")
        .or_else(|| value.get("usagePercentage"))
        .and_then(|percentage| match percentage {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .unwrap_or_default();
    let plan_name = value
        .get("plan_name")
        .or_else(|| value.get("planName"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Ok(json!({
        "available": null,
        "unit": "额度",
        "wallets": [],
        "subscription": {
            "planName": if plan_name.is_empty() { member_level.clone() } else { plan_name },
            "status": if member_level.is_empty() { Value::Null } else { Value::String(member_level) },
            "usagePercentage": usage_percentage,
        },
        "raw": value,
    }))
}
