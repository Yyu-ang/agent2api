//! MiniMax Code 用量：M Plan 窗口（`coding_plan/remains`，纯 Bearer）。
//!
//! ── 数据来源（v1 边界，见 `super` 模块头「已知缺口」）──────────
//! `GET {platform}/v1/api/openplatform/coding_plan/remains` —— 只用
//! Bearer，**不带** `X-Group-Id`（组织 id 要靠签名账号 API 的 membership
//! 才能知道，v1 未接）。该接口按当前登录态直接给窗口用量，插件
//! `planQuota` 在没有组织信息时同样不带该头。
//!
//! ── 窗口归一（插件 `planWindows` 逐条对齐）────────────────────
//! `model_remains[]` 每项给两个窗口：短周期（interval）与周（weekly）。
//! 每窗口的已用百分比：`status == 2`（无限制）记 0% 已用；否则
//! `100 - remaining_percent` 钳到 [0, 100]。跳过规则：
//!   - 模型名为空；整项两个窗口都 `status == 3`（已耗尽封顶形态）且
//!     总量都为 0；
//!   - 单窗口 `status == 3`，或「剩余百分比缺失且 status != 2」。
//! 窗口展示名由跨度推导：周窗口叫「7 days」，整天数叫「N days」，
//! 整小时叫「N hours」，其余「N minutes」，推不出叫「Allowance」；
//! `general`（通用组）之外的模型窗口名带模型名前缀并标 `aside`。
//! `base_resp.status_code != 0` 是上游业务错误，整份按失败处理。
//!
//! ── 统一形状 ────────────────────────────────────────────────
//! remains 只给**百分比**、不给绝对量：`available` / `totalQuota` 如实
//! 给 null（不拿百分比冒充额度）；`credits` 不出（积分余额在签名账号
//! API 的 membership 里，v1 未接——缺口见模块头）。套餐等级（plan /
//! tier）同样在 membership 里，v1 的 `subscription` 给 null。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：绝不 unwrap/expect/panic。

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::auth_http;
use crate::server::errors::GatewayError;

use super::refresh;
use super::region::Region;

/// remains 接口的超时（插件 `planQuota` 的 15 秒同档）
const REMAINS_TIMEOUT_MS: u64 = 15_000;

/// 一天的毫秒数（窗口跨度推导用）
const DAY_MS: f64 = 86_400_000.0;

/// 查询用量（适配器 `query_usage` 的实现）。
pub async fn query_usage(
    store: &AccountStore,
    region: Region,
    account_id: &str,
) -> Result<Value, GatewayError> {
    // 先 ensure（临期自动续期）：用量是只读查询，401 由统一重试层处理，
    // 这里拿到的应该已经是新鲜凭证。
    let credentials = refresh::ensure_fresh(store, region, account_id, false).await?;
    let url = format!(
        "{}/v1/api/openplatform/coding_plan/remains",
        region.platform_base()
    );
    let headers = vec![
        ("Accept".to_string(), "application/json".to_string()),
        (
            "Authorization".to_string(),
            format!("Bearer {}", credentials.access_token),
        ),
    ];
    let response = auth_http::send_raw("GET", &url, None, &headers, None, Some(REMAINS_TIMEOUT_MS))
        .await
        .map_err(|error| {
            GatewayError::with_status(
                502,
                format!(
                    "MiniMax Code 用量查询网络失败: {}",
                    crate::server::core::egress::describe_error_detail(&error)
                ),
            )
        })?;
    if response.status == 401 || response.status == 403 {
        // 交给统一重试层（api/accounts 收到 401 会先强制刷新再重试一次）
        return Err(GatewayError::with_status(
            401,
            "MiniMax Code 登录态已失效或被上游拒绝（用量查询）",
        ));
    }
    if !response.ok {
        return Err(GatewayError::with_status(
            502,
            format!("MiniMax Code 用量接口返回 HTTP {}", response.status),
        ));
    }
    let payload = response
        .payload
        .ok_or_else(|| GatewayError::with_status(502, "MiniMax Code 用量响应不是 JSON"))?;
    let windows = parse_plan_windows(&payload)?;
    Ok(json!({
        // remains 只给百分比：绝对可用量判不出，按契约给 null
        "available": Value::Null,
        "unit": "percent",
        "wallets": [],
        // 套餐等级 / 到期在签名账号 API（membership），v1 未接 → null（见模块头缺口）
        "subscription": Value::Null,
        "windows": windows,
        "planName": Value::Null,
        "raw": payload,
    }))
}

/// 解析 remains 响应为统一 `windows` 数组（插件 `planWindows`，纯函数）。
///
/// 业务错误（`base_resp.status_code != 0`，或既没有 base_resp 也没有
/// model_remains 数组）→ Err(502)，调用方如实报错、不给假窗口。
pub fn parse_plan_windows(body: &Value) -> Result<Vec<Value>, GatewayError> {
    if let Some(base_resp) = body.get("base_resp") {
        let code = base_resp
            .get("status_code")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        if code != 0.0 {
            let message = base_resp
                .get("status_msg")
                .and_then(Value::as_str)
                .unwrap_or("MiniMax Code 用量接口返回业务错误");
            return Err(GatewayError::with_status(502, message.to_string()));
        }
    } else if body
        .get("model_remains")
        .and_then(Value::as_array)
        .is_none()
    {
        return Err(GatewayError::with_status(
            502,
            "MiniMax Code 用量响应缺少 model_remains（no plan）",
        ));
    }
    let mut windows = Vec::new();
    let Some(remains) = body.get("model_remains").and_then(Value::as_array) else {
        return Ok(windows);
    };
    for item in remains {
        let name = item
            .get("model_name")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if name.is_empty() {
            continue;
        }
        // 整项跳过：两窗都 status 3 且总量都为 0（插件的封顶占位项）
        if number(item, "current_interval_status") == Some(3.0)
            && number(item, "current_weekly_status") == Some(3.0)
            && number(item, "current_interval_total").unwrap_or(-1.0) == 0.0
            && number(item, "current_weekly_total").unwrap_or(-1.0) == 0.0
        {
            continue;
        }
        let general = name.eq_ignore_ascii_case("general");
        for weekly in [false, true] {
            if let Some(window) = plan_window(item, name, general, weekly) {
                windows.push(window);
            }
        }
    }
    Ok(windows)
}

/// 一个窗口的归一（interval / weekly 二选一由 `weekly` 决定取哪组字段）。
fn plan_window(item: &Value, model_name: &str, general: bool, weekly: bool) -> Option<Value> {
    let (left_key, status_key, start_key, end_key) = if weekly {
        (
            "current_weekly_remaining_percent",
            "current_weekly_status",
            "weekly_start_time",
            "weekly_end_time",
        )
    } else {
        (
            "current_interval_remaining_percent",
            "current_interval_status",
            "start_time",
            "end_time",
        )
    };
    let left = number(item, left_key);
    let status = number(item, status_key).unwrap_or(0.0);
    let start = number(item, start_key).unwrap_or(0.0);
    let end = number(item, end_key).unwrap_or(0.0);
    // 跳过：已封顶（3），或剩余缺失且不是无限制（2）
    if status == 3.0 || (left.is_none() && status != 2.0) {
        return None;
    }
    // status 2 = 无限制 → 已用 0%；否则 100 - 剩余，钳制
    let used = if status == 2.0 {
        0.0
    } else {
        (100.0 - left.unwrap_or(100.0)).clamp(0.0, 100.0)
    };
    // 跨度：起止都有且止 > 起就用实测；推不出时周窗口按 7 天
    let span_ms = if start > 0.0 && end > start {
        to_ms(end) - to_ms(start)
    } else if weekly {
        7.0 * DAY_MS
    } else {
        0.0
    };
    let window_name = window_label(span_ms, weekly);
    let mut window = json!({
        "name": if general { window_name.clone() } else { format!("{} · {}", capitalize(model_name), window_name) },
        "usedPercent": used,
        "remainingPercent": 100.0 - used,
    });
    if let Some(object) = window.as_object_mut() {
        if !general {
            object.insert("aside".to_string(), Value::Bool(true));
        }
        if span_ms > 0.0 {
            object.insert("spanSeconds".to_string(), json!((span_ms / 1000.0) as i64));
        }
        if end > 0.0 {
            object.insert("resetsAt".to_string(), json!(to_ms(end) as i64));
        }
    }
    Some(window)
}

/// 窗口展示名（插件 `planWindows` 逐条对齐：周窗口恒为「7 days」，
/// 其余按跨度推导）
fn window_label(span_ms: f64, weekly: bool) -> String {
    if weekly {
        return "7 days".to_string();
    }
    if span_ms > DAY_MS && span_ms % DAY_MS == 0.0 {
        return format!("{} days", (span_ms / DAY_MS) as i64);
    }
    if span_ms >= 3_600_000.0 && span_ms % 3_600_000.0 == 0.0 {
        return format!("{} hours", (span_ms / 3_600_000.0) as i64);
    }
    if span_ms > 0.0 {
        return format!("{} minutes", (span_ms / 60_000.0).floor() as i64);
    }
    "Allowance".to_string()
}

/// 上游时间戳归一：小于 1e12 视为秒、否则毫秒（插件 `planWindows` 的 ms()）
fn to_ms(value: f64) -> f64 {
    if value < 1e12 {
        value * 1000.0
    } else {
        value
    }
}

/// 数字字段（有限数才算数）
fn number(value: &Value, key: &str) -> Option<f64> {
    value
        .get(key)
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite())
}

/// 首字母大写（插件对非 general 窗口的模型名做 `charAt(0).toUpperCase()`；
/// 模型名是 ASCII 的 `MiniMax-*`，按 ASCII 处理即可）
fn capitalize(name: &str) -> String {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn plan_windows_normalizes_interval_and_weekly() {
        let body = json!({
            "base_resp": { "status_code": 0 },
            "model_remains": [{
                "model_name": "general",
                "current_interval_remaining_percent": 40,
                "current_interval_status": 1,
                "current_interval_total": 100,
                "start_time": 1_700_000_000_i64,
                "end_time": 1_700_018_000_i64,
                "current_weekly_remaining_percent": 75,
                "current_weekly_status": 1,
                "current_weekly_total": 500,
                "weekly_start_time": 1_699_900_000_i64,
                "weekly_end_time": 1_700_504_800_i64,
            }, {
                "model_name": "MiniMax-M3",
                "current_interval_remaining_percent": 100,
                "current_interval_status": 2,
                "current_weekly_remaining_percent": 90,
                "current_weekly_status": 1,
                "weekly_start_time": 0,
                "weekly_end_time": 0,
            }],
        });
        let windows = parse_plan_windows(&body).unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(windows.len(), 4);
        // general 短窗：已用 60%，跨度 5 小时 → "5 hours"
        assert_eq!(windows[0]["name"], "5 hours");
        assert_eq!(windows[0]["usedPercent"], 60.0);
        assert_eq!(windows[0]["resetsAt"], 1_700_018_000_000_i64);
        // general 周窗 → "7 days"
        assert_eq!(windows[1]["name"], "7 days");
        assert_eq!(windows[1]["usedPercent"], 25.0);
        // 非 general 带前缀 + aside；status 2 → 已用 0%
        assert_eq!(windows[2]["name"], "MiniMax-M3 · Allowance");
        assert_eq!(windows[2]["aside"], true);
        assert_eq!(windows[2]["usedPercent"], 0.0);
    }

    #[test]
    fn plan_windows_skips_capped_and_empty_items() {
        let body = json!({
            "model_remains": [{
                "model_name": "capped",
                "current_interval_status": 3,
                "current_weekly_status": 3,
                "current_interval_total": 0,
                "current_weekly_total": 0,
            }, {
                "model_name": "  ",
                "current_interval_remaining_percent": 10,
            }, {
                "model_name": "partial",
                "current_interval_status": 3,
                "current_interval_remaining_percent": 5,
                "current_weekly_remaining_percent": 50,
                "current_weekly_status": 1,
            }],
        });
        let windows = parse_plan_windows(&body).unwrap_or_else(|error| panic!("{}", error.message));
        // 只剩 partial 的周窗（它的短窗 status 3 被跳过）
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0]["usedPercent"], 50.0);
    }

    #[test]
    fn plan_windows_business_error_is_an_error() {
        let body = json!({
            "base_resp": { "status_code": 1004, "status_msg": "invalid token" },
        });
        let error = parse_plan_windows(&body).unwrap_err();
        assert_eq!(error.status_code, 502);
        assert!(error.message.contains("invalid token"));
        // 既无 base_resp 又无 model_remains → 失败
        assert!(parse_plan_windows(&json!({})).is_err());
    }
}
