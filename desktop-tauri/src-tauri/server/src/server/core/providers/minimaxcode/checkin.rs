//! MiniMax Code 每日签到：签名账号 API 的
//! `GET /minimax-cloud/api/v1/signin/status` + `POST …/signin/claim`。
//!
//! ── 签名（插件 `signed()` 逐条对齐，本模块是全仓唯一实现处）──────
//! 签到与 identity / membership 共用同一套「mcode 签名」：请求带全套
//! query 参数（`device_platform=mcode&biz_id=3&app_id=3001&
//! version_code=22201&unix=<毫秒>&timezone_offset=<秒>&sys_language=
//! <lang>&lang=<lang>&device_id=0&os_name=<平台>&browser_name=mcode&
//! user_id=<账号 uid 或 0>&client=mcode`），并带三个签名头：
//!   - `yy`：GET = `md5( encodeURIComponent(pathname+search) + "_{}" +
//!     md5(String(now_ms)) + "ooui" )`；POST 把 `_{}` 换成 `_<json body>`；
//!   - `x-timestamp`：秒级时间戳；
//!   - `x-signature`：GET = `md5(ts + SECRET)`，POST = `md5(ts + SECRET + json)`。
//! `SECRET = "I*7Cf%WZ#S&%1RlZJ&C2"`（插件 `SIGN_SECRET`，mcode 客户端
//! 公开常量；magpie 本体的 Go 实现 `minimax_checkin.go` 用的是同一串
//! 密钥，但只签 `x-timestamp`/`x-signature` 且 query 只带 timezone_id——
//! 本实现以**插件 `checkinOf` 的 signed 调用为准**，它与 mcode 现行
//! 客户端同形）。
//! 请求头还带 `User-Agent: MiniMaxCode`（注意：与对话转发的
//! `MiniMaxAgent` 不同，是插件逐字口径）与 Bearer。
//!
//! ── 面板校验（插件 `checkinOf` 逐条对齐，不放宽）────────────────
//! `status` 响应的 `data.days` 必须是**恰好 7 天**、`day_no` 依次
//! 1..7 的面板；`is_today` 至多一个、`status == 2`（可领取）至多一个，
//! 否则整份按「面板畸形」失败——宁可报错也不在看不懂的面板上乱 claim。
//! 今天的状态：1 未开始、2 可领取、3 已领取、4 未开放。**只有
//! status == 2 才发 claim**；claim 回 `claim_result` 1 = 本次领取、
//! 2 = 今天已领过，其它值按失败处理。
//!
//! ── 与仓库签到框架的衔接 ────────────────────────────────────
//! 入口是 [`claim_daily_checkin`]，由 `core::billing::checkin` 的
//! `checkin_for` 按 provider 分发，返回全仓统一的 `{success, msg}`
//! claim 形状（已领取给 `alreadyCompleted: true` 让落账口径认得出；
//! 「未开始」文案带上「暂无可领取活动」以命中框架的中性 pending 判据，
//! 让自动签到当天稍后自动重试）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：绝不 unwrap/expect/panic。

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::auth_http;
use crate::server::errors::GatewayError;

use super::credentials;
use super::refresh;
use super::region::Region;

/// mcode 签名密钥（插件 `SIGN_SECRET`，客户端公开常量）
const SIGN_SECRET: &str = "I*7Cf%WZ#S&%1RlZJ&C2";
/// 签名请求的超时（插件 accountCall 的 15 秒同档）
const SIGNED_TIMEOUT_MS: u64 = 15_000;

/// 一次签名请求组装的结果（纯数据，便于单测逐字核对）
struct SignedRequest {
    /// 完整 URL（path + 签名 query）
    url: String,
    /// 请求头（含 yy / x-timestamp / x-signature）
    headers: Vec<(String, String)>,
}

/// 组装一次签名请求（插件 `signed()`，纯函数 —— `now_ms` 作参数注入，
/// 让签名向量可测）。
///
/// `origin` 是站点根（签到用 llm 站点）；`body` 为 None 时是 GET 形态，
/// Some 时是 POST 形态（body 先序列化成 JSON 字符串再参与签名，
/// 与插件 `JSON.stringify(body)` 同一份字节）。
fn build_signed(
    origin: &str,
    path: &str,
    region: Region,
    user_id: &str,
    access_token: &str,
    body: Option<&str>,
    now_ms: i64,
) -> SignedRequest {
    let lang = region.lang();
    let uid = if user_id.trim().is_empty() {
        "0"
    } else {
        user_id.trim()
    };
    let params: [(&str, String); 13] = [
        ("device_platform", "mcode".to_string()),
        ("biz_id", "3".to_string()),
        ("app_id", "3001".to_string()),
        ("version_code", "22201".to_string()),
        ("unix", now_ms.to_string()),
        (
            "timezone_offset",
            credentials::local_timezone_offset_seconds().to_string(),
        ),
        ("sys_language", lang.to_string()),
        ("lang", lang.to_string()),
        ("device_id", "0".to_string()),
        ("os_name", os_name().to_string()),
        ("browser_name", "mcode".to_string()),
        ("user_id", uid.to_string()),
        ("client", "mcode".to_string()),
    ];
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={}", form_url_encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    // `at` = pathname + search（search 带前导 ?），签名对它做 encodeURIComponent
    let at = format!("{path}?{query}");
    let inner = md5_hex(now_ms.to_string().as_bytes());
    let yy = match body {
        None => md5_hex(format!("{}_{{}}{}ooui", encode_uri_component(&at), inner).as_bytes()),
        Some(json) => {
            md5_hex(format!("{}_{json}{}ooui", encode_uri_component(&at), inner).as_bytes())
        }
    };
    let ts = (now_ms / 1000).to_string();
    let signature = match body {
        None => md5_hex(format!("{ts}{SIGN_SECRET}").as_bytes()),
        Some(json) => md5_hex(format!("{ts}{SIGN_SECRET}{json}").as_bytes()),
    };
    SignedRequest {
        url: format!("{origin}{at}"),
        headers: vec![
            ("Accept".to_string(), "application/json".to_string()),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("User-Agent".to_string(), "MiniMaxCode".to_string()),
            (
                "Authorization".to_string(),
                format!("Bearer {access_token}"),
            ),
            ("yy".to_string(), yy),
            ("x-timestamp".to_string(), ts),
            ("x-signature".to_string(), signature),
        ],
    }
}

/// 发一次签名请求并做信封检查（401/403 → 401 终态风格报错；`checked()`
/// 的业务码非 0 → 502；`statusInfo.code == 1000048` 是上游的「账号被拒」
/// 专用码，与插件一致按 401 处理）。
async fn signed_call(
    region: Region,
    path: &str,
    user_id: &str,
    access_token: &str,
    body: Option<Value>,
) -> Result<Value, GatewayError> {
    let body_json = body.as_ref().map(|value| value.to_string());
    let request = build_signed(
        region.llm_base(),
        path,
        region,
        user_id,
        access_token,
        body_json.as_deref(),
        crate::server::logging::now_ms(),
    );
    let method = if body.is_some() { "POST" } else { "GET" };
    let response = auth_http::send_raw(
        method,
        &request.url,
        body.as_ref(),
        &request.headers,
        None,
        Some(SIGNED_TIMEOUT_MS),
    )
    .await
    .map_err(|error| {
        GatewayError::with_status(
            502,
            format!(
                "MiniMax Code 签到请求网络失败: {}",
                crate::server::core::egress::describe_error_detail(&error)
            ),
        )
    })?;
    if response.status == 401 || response.status == 403 {
        return Err(GatewayError::with_status(
            401,
            "MiniMax Code 签到被上游拒绝（登录态失效或账号被拒），请重新登录",
        ));
    }
    if !response.ok {
        return Err(GatewayError::with_status(
            502,
            format!("MiniMax Code 签到接口返回 HTTP {}", response.status),
        ));
    }
    let payload = response
        .payload
        .filter(Value::is_object)
        .ok_or_else(|| GatewayError::with_status(502, "MiniMax Code 签到响应不是 JSON 对象"))?;
    check_envelope(&payload)?;
    Ok(payload)
}

/// 信封业务码检查（插件 `checked()`）：`statusInfo.code` 与
/// `base_resp.status_code` 任一非 0 即失败。
fn check_envelope(body: &Value) -> Result<(), GatewayError> {
    if let Some(code) = body
        .get("statusInfo")
        .and_then(|info| info.get("code"))
        .and_then(Value::as_f64)
    {
        if code != 0.0 {
            if code == 1_000_048.0 {
                return Err(GatewayError::with_status(
                    401,
                    "MiniMax Code 账号被上游拒绝（statusInfo.code=1000048），请重新登录",
                ));
            }
            let message = body
                .get("statusInfo")
                .and_then(|info| info.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("MiniMax Code 签到接口返回业务错误");
            return Err(GatewayError::with_status(502, message.to_string()));
        }
    }
    if let Some(code) = body
        .get("base_resp")
        .and_then(|base| base.get("status_code"))
        .and_then(Value::as_f64)
    {
        if code != 0.0 {
            let message = body
                .get("base_resp")
                .and_then(|base| base.get("status_msg"))
                .and_then(Value::as_str)
                .unwrap_or("MiniMax Code 签到接口返回业务错误");
            return Err(GatewayError::with_status(502, message.to_string()));
        }
    }
    Ok(())
}

// ─── 签到面板 ───────────────────────────────────────────────────

/// 面板解析结论（插件 `checkinOf` 前半段）
#[derive(Clone, Debug, PartialEq)]
pub enum Panel {
    /// 面板里没有「今天」（活动未开始或已结束）
    Inactive,
    /// 今天未到（status 1）
    NotStarted,
    /// 今天可领取（status 2）：带上今天在面板中的下标与此前连签天数
    Claimable {
        /// 今天在 7 天面板中的下标（0 起）
        index: usize,
        /// 今天之前已连签的天数
        streak_before: u32,
    },
    /// 今天已领取（status 3）：积分（有才有）与连签天数（含今天）
    Done {
        /// 今天的积分
        credit: Option<f64>,
        /// 连签天数（含今天）
        streak: u32,
    },
    /// 今天未开放（status 4）
    Ineligible,
}

/// 解析并**校验**签到面板（纯函数；7 天规则与畸形容忍度照插件，见模块头）。
pub fn parse_panel(body: &Value) -> Result<Panel, GatewayError> {
    let malformed =
        || GatewayError::with_status(502, "MiniMax Code 签到面板畸形（malformed signin panel）");
    let days = body
        .get("data")
        .and_then(|data| data.get("days"))
        .and_then(Value::as_array)
        .ok_or_else(malformed)?;
    let mut sorted: Vec<&Value> = days.iter().collect();
    sorted.sort_by_key(|day| day.get("day_no").and_then(Value::as_i64).unwrap_or(0));
    // 恰好 7 天、day_no 依次 1..7
    if sorted.len() != 7 {
        return Err(malformed());
    }
    for (index, day) in sorted.iter().enumerate() {
        if day.get("day_no").and_then(Value::as_i64) != Some((index + 1) as i64) {
            return Err(malformed());
        }
    }
    let status_of = |day: &Value| day.get("status").and_then(Value::as_i64).unwrap_or(0);
    // is_today 至多一个、status 2 至多一个
    if sorted
        .iter()
        .filter(|day| day.get("is_today").and_then(Value::as_bool) == Some(true))
        .count()
        > 1
        || sorted.iter().filter(|day| status_of(day) == 2).count() > 1
    {
        return Err(malformed());
    }
    let Some(today_index) = sorted
        .iter()
        .position(|day| day.get("is_today").and_then(Value::as_bool) == Some(true))
    else {
        return Ok(Panel::Inactive);
    };
    let today = sorted[today_index];
    match status_of(today) {
        3 => Ok(Panel::Done {
            credit: positive(today.get("points")),
            streak: streak_ending_at(&sorted, today_index as i64),
        }),
        2 => Ok(Panel::Claimable {
            index: today_index,
            streak_before: streak_ending_at(&sorted, today_index as i64 - 1),
        }),
        4 => Ok(Panel::Ineligible),
        1 => Ok(Panel::NotStarted),
        _ => Err(malformed()),
    }
}

/// 从 `index` 往前数连续 status==3 的天数（含 index 本身；index < 0 → 0）
fn streak_ending_at(days: &[&Value], index: i64) -> u32 {
    let mut streak = 0;
    let mut cursor = index;
    while cursor >= 0 {
        let Some(day) = days.get(cursor as usize) else {
            break;
        };
        if day.get("status").and_then(Value::as_i64) != Some(3) {
            break;
        }
        streak += 1;
        cursor -= 1;
    }
    streak
}

/// 正数数值（插件 `pos()`）
fn positive(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite() && *number > 0.0)
}

// ─── 入口 ───────────────────────────────────────────────────────

/// 执行每日签到（`core::billing::checkin::checkin_for` 按 provider 分发到这里）。
///
/// 返回统一 claim 形状 `{success, msg}`（外加 `alreadyCompleted` /
/// `credit` 等读数键）；执行失败（网络 / 面板畸形 / 登录态被拒）走
/// Err，框架会记为失败行——**不**把失败伪装成 success:false。
pub async fn claim_daily_checkin(
    store: &AccountStore,
    region: Region,
    account_id: &str,
) -> Result<Value, GatewayError> {
    let credential_set = refresh::ensure_fresh(store, region, account_id, false).await?;
    let uid = credential_set.uid.as_str();
    let token = credential_set.access_token.as_str();
    let status = signed_call(
        region,
        "/minimax-cloud/api/v1/signin/status",
        uid,
        token,
        None,
    )
    .await?;
    match parse_panel(&status)? {
        Panel::Inactive => Ok(json!({
            "success": false,
            "msg": "签到活动未开始或已结束（暂无可领取活动）",
        })),
        Panel::NotStarted => Ok(json!({
            "success": false,
            "msg": "今日签到尚未开始（暂无可领取活动）",
        })),
        Panel::Ineligible => Ok(json!({
            "success": false,
            "msg": "今日签到未开放，该账号暂不可参与",
        })),
        Panel::Done { credit, streak } => Ok(json!({
            "success": false,
            "alreadyCompleted": true,
            "credit": credit,
            "msg": format!(
                "今日已签到（连签 {streak} 天{}）",
                credit.map(|points| format!("，+{} 积分", format_points(points))).unwrap_or_default()
            ),
        })),
        Panel::Claimable { streak_before, .. } => {
            let claim = signed_call(
                region,
                "/minimax-cloud/api/v1/signin/claim",
                uid,
                token,
                Some(json!({})),
            )
            .await?;
            let data = claim.get("data").cloned().unwrap_or(Value::Null);
            let result = positive(data.get("claim_result"));
            let credit = positive(data.get("points"));
            match result {
                Some(1.0) => Ok(json!({
                    "success": true,
                    "credit": credit,
                    "msg": format!(
                        "签到成功{}（连签 {} 天）",
                        credit
                            .map(|points| format!("，获得 {} 积分", format_points(points)))
                            .unwrap_or_default(),
                        streak_before + 1
                    ),
                })),
                Some(2.0) => Ok(json!({
                    "success": false,
                    "alreadyCompleted": true,
                    "credit": credit,
                    "msg": "今日已签到".to_string(),
                })),
                _ => Err(GatewayError::with_status(
                    502,
                    "MiniMax Code 签到领取返回了未知 claim_result",
                )),
            }
        }
    }
}

/// 积分展示：整数不带小数，非整数保留原样（插件 `pos` 读数口径）
fn format_points(points: f64) -> String {
    if points.fract() == 0.0 && points.abs() < 1e15 {
        format!("{}", points as i64)
    } else {
        format!("{points}")
    }
}

// ─── 编码与哈希 ─────────────────────────────────────────────────

/// md5 十六进制（小写）
fn md5_hex(bytes: &[u8]) -> String {
    use md5::Digest as _;
    let digest = md5::Md5::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// JS `encodeURIComponent` 等价实现：不转义
/// `A-Z a-z 0-9 - _ . ! ~ * ' ( )`，其余字节按 UTF-8 逐字节 %XX（大写）。
fn encode_uri_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => out.push(byte as char),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// URLSearchParams 的编码（application/x-www-form-urlencoded）：
/// 保留 `A-Z a-z 0-9 * - . _`，空格 → `+`，其余 %XX（大写）。
fn form_url_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(byte as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// 上游 `os_name`（插件用 Node 的 `process.platform`：win32/darwin/linux）
fn os_name() -> &'static str {
    match std::env::consts::OS {
        "windows" => "win32",
        "macos" => "darwin",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const NOW: i64 = 1_700_000_000_000;

    /// 造一个 7 天面板：statuses 依次给 7 天，today 是下标（0 起）
    fn panel(statuses: [i64; 7], today: usize, points: i64) -> Value {
        let days: Vec<Value> = statuses
            .iter()
            .enumerate()
            .map(|(index, status)| {
                json!({
                    "day_no": index + 1,
                    "status": status,
                    "is_today": index == today,
                    "points": points,
                })
            })
            .collect();
        json!({ "data": { "days": days } })
    }

    #[test]
    fn encode_uri_component_matches_js_rules() {
        assert_eq!(encode_uri_component("a b&c=d"), "a%20b%26c%3Dd");
        // JS 不转义的字符集逐字保留
        assert_eq!(encode_uri_component("-_.!~*'()"), "-_.!~*'()");
        // 中文按 UTF-8 逐字节
        assert_eq!(encode_uri_component("中"), "%E4%B8%AD");
    }

    #[test]
    fn signed_request_get_shape_and_signature() {
        let request = build_signed(
            "https://agent.minimax.cn",
            "/minimax-cloud/api/v1/signin/status",
            Region::Cn,
            "uid-1",
            "token-1",
            None,
            NOW,
        );
        assert!(request
            .url
            .starts_with("https://agent.minimax.cn/minimax-cloud/api/v1/signin/status?device_platform=mcode&biz_id=3&app_id=3001&version_code=22201&unix=1700000000000&"));
        assert!(request.url.contains("&user_id=uid-1&client=mcode"));
        let header = |name: &str| {
            request
                .headers
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        assert_eq!(header("x-timestamp"), "1700000000");
        // x-signature = md5(ts + secret)（期望值用同一 md5 实现复算，验的是拼装而非算法本身）
        assert_eq!(
            header("x-signature"),
            md5_hex(format!("1700000000{SIGN_SECRET}").as_bytes())
        );
        assert_eq!(header("User-Agent"), "MiniMaxCode");
        assert_eq!(header("Authorization"), "Bearer token-1");
        assert_eq!(header("yy").len(), 32);
    }

    #[test]
    fn signed_request_post_includes_body_in_both_signatures() {
        let get = build_signed(
            "https://agent.minimax.io",
            "/minimax-cloud/api/v1/signin/status",
            Region::Global,
            "",
            "t",
            None,
            NOW,
        );
        let post = build_signed(
            "https://agent.minimax.io",
            "/minimax-cloud/api/v1/signin/claim",
            Region::Global,
            "",
            "t",
            Some("{}"),
            NOW,
        );
        // user_id 缺省 "0"；POST 的 yy 与 x-signature 都因 body 不同而不同
        assert!(post.url.contains("user_id=0"));
        let sig = |request: &SignedRequest, name: &str| {
            request
                .headers
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        assert_ne!(sig(&get, "x-signature"), sig(&post, "x-signature"));
        assert_ne!(sig(&get, "yy"), sig(&post, "yy"));
        assert_eq!(
            sig(&post, "x-signature"),
            md5_hex(format!("1700000000{SIGN_SECRET}{{}}").as_bytes())
        );
    }

    #[test]
    fn panel_done_counts_streak_backwards_from_today() {
        let parsed = parse_panel(&panel([3, 3, 3, 3, 1, 1, 1], 3, 10))
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(
            parsed,
            Panel::Done {
                credit: Some(10.0),
                streak: 4
            }
        );
    }

    #[test]
    fn panel_claimable_reports_streak_before() {
        let parsed = parse_panel(&panel([3, 3, 2, 1, 1, 1, 1], 2, 10))
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(
            parsed,
            Panel::Claimable {
                index: 2,
                streak_before: 2
            }
        );
    }

    #[test]
    fn panel_without_today_is_inactive_and_status4_is_ineligible() {
        let mut no_today = panel([3, 3, 3, 1, 1, 1, 1], 2, 10);
        if let Some(days) = no_today["data"]["days"].as_array_mut() {
            for day in days.iter_mut() {
                day["is_today"] = json!(false);
            }
        }
        assert_eq!(
            parse_panel(&no_today).unwrap_or(Panel::Ineligible),
            Panel::Inactive
        );
        assert_eq!(
            parse_panel(&panel([1, 4, 1, 1, 1, 1, 1], 1, 10)).unwrap_or(Panel::Inactive),
            Panel::Ineligible
        );
        assert_eq!(
            parse_panel(&panel([1, 1, 1, 1, 1, 1, 1], 0, 10)).unwrap_or(Panel::Inactive),
            Panel::NotStarted
        );
    }

    #[test]
    fn malformed_panels_are_rejected() {
        // 不是 7 天
        let six_days: Vec<Value> = (1..=6)
            .map(|no| json!({ "day_no": no, "status": 1 }))
            .collect();
        let six = json!({ "data": { "days": six_days } });
        assert!(parse_panel(&six).is_err());
        // day_no 不连续
        let gap = panel([1, 1, 1, 1, 1, 1, 1], 0, 10);
        let mut gap = gap;
        gap["data"]["days"][6]["day_no"] = json!(9);
        assert!(parse_panel(&gap).is_err());
        // 两个 is_today
        let mut two_today = panel([1, 1, 1, 1, 1, 1, 1], 0, 10);
        two_today["data"]["days"][1]["is_today"] = json!(true);
        assert!(parse_panel(&two_today).is_err());
        // 两个可领取
        assert!(parse_panel(&panel([2, 2, 1, 1, 1, 1, 1], 0, 10)).is_err());
    }

    #[test]
    fn envelope_rejects_business_errors() {
        assert!(check_envelope(&json!({ "base_resp": { "status_code": 0 } })).is_ok());
        assert!(check_envelope(&json!({ "statusInfo": { "code": 0 } })).is_ok());
        assert_eq!(
            check_envelope(&json!({ "statusInfo": { "code": 1000048 } }))
                .unwrap_err()
                .status_code,
            401
        );
        assert!(
            check_envelope(&json!({ "base_resp": { "status_code": 7, "status_msg": "bad" } }))
                .is_err()
        );
    }
}
