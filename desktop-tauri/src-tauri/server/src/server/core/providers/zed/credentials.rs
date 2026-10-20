//! Zed 的凭证形态与 `GET /client/users/me` 响应的解读（选组织 / 套餐）。
//!
//! ── 凭证为什么是「一枚令牌 + 一段 JSON」──────────────────
//! 插件把登录结果存成 OpenCode 的 oauth 条目：`access` 是访问令牌，
//! `refresh` 是一段 JSON 字符串 `{userId, systemId, org, plan, planName,
//! login, name}` —— Zed **没有 refresh token**，这段 JSON 只是「调
//! `/client/*` 与铸造模型令牌时要随身带的状态」。本仓的对应物：账号记录的
//! `accessToken` 存令牌、`refreshToken` 存同一段 JSON（键名与语义见
//! `account_store::zed_accounts`）。两者缺一不可：只有令牌不知道 userId，
//! `/client/*` 的 `Authorization: "<userId> <access>"` 就拼不出来。
//!
//! ── users/me 的解读为什么单独成文件 ───────────────────────
//! 选组织（`org_of`）与取套餐（`plan_of`）是登录、用量、repick 三条路径
//! 共用的判定，且规则是「按 Zed 编辑器的行为复刻」的（插件注释引了
//! zed 源码的 user.rs / cloud.rs）：
//!   * 可用组织 = 该组织的套餐「付费」（非空且非 `zed_free`）且没有关掉
//!     hosted models（`configuration_by_organization[org].
//!     is_zed_model_provider_enabled === false` 视为关）；
//!   * 选择顺序：已保存且可用 > 默认且可用 > 个人且可用 > 任意可用 >
//!     已保存 > 默认 > 第一个；都没有组织时回退 `saved || default || ""`；
//!   * 套餐取**所选组织自己的**（`plans_by_organization[org]`），组织已知
//!     而表里没有它就是「无套餐」（空串），**不许**拿账号级套餐顶替；只有
//!     响应里根本没有组织时才回退账号的 `plan.plan_v3`。
//! 这几条都是纯函数，单测逐分支钉住（见文件尾）。

use serde_json::{json, Value};

/// 一条 Zed 账号的完整凭证（令牌 + 状态）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credential {
    /// 访问令牌（回调密文解出的明文；`/client/*` 与铸造模型令牌都用它）
    pub access: String,
    /// Zed 侧的用户 id（回调 `user_id`；账号身份与去重键）
    pub user_id: String,
    /// 登录时生成的 systemId（UUID；`/client/*` 的 `x-zed-system-id` 头）
    pub system_id: String,
    /// 当前选中的组织 id（模型令牌按组织铸造）
    pub org: String,
    /// 当前组织的套餐 id（如 `zed_pro`；可为空串 = 无套餐）
    pub plan: String,
    /// GitHub login（没有时退 username；展示与状态 JSON 用）
    pub login: String,
    /// 用户显示名（users/me 的 `user.name`，可为空）
    pub name: String,
    /// 账号展示名（`who(me)`：login → username → name → legacy_user_id）
    pub display: String,
}

impl Credential {
    /// 套餐的人读名（`planName` 规则见 [`plan_name`]）。
    pub fn plan_name(&self) -> String {
        plan_name(&self.plan)
    }

    /// 状态 JSON（存进账号记录 `refreshToken` 字段的那一段，与插件
    /// `stateOf` / 登录落盘的形态逐键一致）。
    pub fn state_json(&self) -> String {
        json!({
            "userId": self.user_id,
            "systemId": self.system_id,
            "org": self.org,
            "plan": self.plan,
            "planName": self.plan_name(),
            "login": self.login,
            "name": self.name,
        })
        .to_string()
    }

    /// 账号记录 id：`zed-<userId>`（userId 是上游给的稳定标识；
    /// 非常规字符替成 `-`，避免把奇怪字节带进主键）。
    pub fn account_id(&self) -> String {
        let cleaned: String = self
            .user_id
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                    ch
                } else {
                    '-'
                }
            })
            .collect();
        format!("zed-{cleaned}")
    }

    /// 从账号记录（`StoredAccount::to_value` 的整条 JSON）还原凭证。
    ///
    /// 认两种状态来源（与 zcode 读记录「兼容历史、写入只产一种」同一处置）：
    /// 顶层 `refreshToken` 里的 JSON 是正形；万一为空则回退顶层散字段
    /// （`userId` / `systemId` / `org` / `plan`）。令牌或 userId 缺失 → None
    /// （这条记录没法调任何 Zed 接口，按「无凭证」处理）。
    pub fn from_record(record: &Value) -> Option<Self> {
        let access = record
            .get("accessToken")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if access.is_empty() {
            return None;
        }
        let state = record
            .get("refreshToken")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            .unwrap_or(Value::Null);
        let pick = |key: &str| -> String {
            state
                .get(key)
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    record
                        .get(key)
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_default()
                .trim()
                .to_string()
        };
        let user_id = pick("userId");
        if user_id.is_empty() {
            return None;
        }
        let display = record
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        Some(Self {
            access,
            user_id,
            system_id: pick("systemId"),
            org: pick("org"),
            plan: pick("plan"),
            login: pick("login"),
            name: pick("name"),
            display,
        })
    }

    /// 换组织/套餐后的副本（repick 回写用；其余字段不动）。
    pub fn with_org(&self, org: &str, plan: &str) -> Self {
        let mut next = self.clone();
        next.org = org.to_string();
        next.plan = plan.to_string();
        next
    }
}

/// 生成一个 UUID v4 形态的 systemId（插件用 `crypto.randomUUID()`）。
///
/// 随机源失败返回 None —— systemId 是上游认「这台设备」的标识，现编一个
/// 不随机的值只会让风控形状更怪，如实失败好过造假（与 zcode 设备标识
/// 生成失败即报错同一口径）。
pub fn new_system_id() -> Option<String> {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).ok()?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

// ─── users/me 的解读（逐字对照插件 who / paid / modelsOff / orgOf / planOf / planName）───

fn user_field(me: &Value, key: &str) -> Option<String> {
    me.get("user")
        .and_then(|user| user.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// 账号展示名：github_login → username → name → legacy_user_id → 兜底文案。
pub fn who(me: &Value) -> String {
    for key in ["github_login", "username", "name"] {
        if let Some(text) = user_field(me, key) {
            return text;
        }
    }
    if let Some(id) = me
        .get("user")
        .and_then(|user| user.get("legacy_user_id"))
        .and_then(|value| match value {
            Value::Number(number) => Some(number.to_string()),
            Value::String(text) if !text.trim().is_empty() => Some(text.trim().to_string()),
            _ => None,
        })
    {
        return id;
    }
    "Zed account".to_string()
}

/// 套餐算不算「能调 hosted models」：非空且不是 `zed_free`。
pub fn paid(plan: Option<&str>) -> bool {
    matches!(plan, Some(plan) if !plan.is_empty() && plan != "zed_free")
}

fn plan_entry<'a>(me: &'a Value, org: &str) -> Option<&'a str> {
    me.get("plans_by_organization")
        .and_then(|plans| plans.get(org))
        .and_then(Value::as_str)
}

/// 该组织是否关掉了 hosted models（配置缺失不算关——只有显式 false 才算）。
pub fn models_off(me: &Value, org: &str) -> bool {
    me.get("configuration_by_organization")
        .and_then(|configs| configs.get(org))
        .and_then(|config| config.get("is_zed_model_provider_enabled"))
        == Some(&Value::Bool(false))
}

fn organizations(me: &Value) -> Vec<(String, bool)> {
    me.get("organizations")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(|org| {
                    let id = org
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())?
                        .to_string();
                    let personal = org
                        .get("is_personal")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    Some((id, personal))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// 选组织（插件 `orgOf` 逐字）：已保存且可用 > 默认且可用 > 个人且可用 >
/// 任意可用 > 已保存 > 默认 > 第一个；无组织时 `saved || default || ""`。
pub fn org_of(me: &Value, saved: &str) -> String {
    let orgs = organizations(me);
    let usable = |id: &str| paid(plan_entry(me, id)) && !models_off(me, id);
    let default_id = me
        .get("default_organization_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    let keep = orgs.iter().find(|(id, _)| id == saved);
    let default = orgs.iter().find(|(id, _)| id == default_id);
    let pick = [keep, default]
        .into_iter()
        .flatten()
        .find(|(id, _)| usable(id))
        .or_else(|| orgs.iter().find(|(id, personal)| *personal && usable(id)))
        .or_else(|| orgs.iter().find(|(id, _)| usable(id)))
        .or(keep)
        .or(default)
        .or_else(|| orgs.first());
    if let Some((id, _)) = pick {
        return id.clone();
    }
    if !saved.is_empty() {
        return saved.to_string();
    }
    default_id.to_string()
}

/// 所选组织的套餐（插件 `planOf` 逐字）：组织已知且套餐表是对象 → 表里
/// 该组织的那一份（没有就是空串）；只有响应里根本没有组织时才回退账号级
/// `plan.plan_v3`。
pub fn plan_of(me: &Value, org: &str) -> String {
    let orgs = organizations(me);
    let known = !org.is_empty()
        && orgs.iter().any(|(id, _)| id == org)
        && me
            .get("plans_by_organization")
            .is_some_and(Value::is_object);
    if known {
        return plan_entry(me, org).unwrap_or("").to_string();
    }
    // 不是「已知组织」时（没有组织、或 org 不在组织列表里）回退账号级套餐
    // —— 插件 planOf 的回退条件就是 known 为假，逐字如此。
    me.get("plan")
        .and_then(|plan| plan.get("plan_v3"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// 套餐 id → 人读名（插件 `planName` 逐字）：已知映射之外剥掉 `zed_` 前缀。
pub fn plan_name(id: &str) -> String {
    match id {
        "zed_pro" => "Pro".to_string(),
        "zed_pro_trial" => "Pro Trial".to_string(),
        "zed_business" => "Business".to_string(),
        "zed_student" => "Student".to_string(),
        "zed_vip" => "VIP".to_string(),
        "zed_free" | "" => "Free".to_string(),
        other => other.strip_prefix("zed_").unwrap_or(other).to_string(),
    }
}

/// 登录/手填校验共用：拿一份 users/me 响应组装凭证（选组织 + 判 hosted
/// models 是否被关）。组织关了 hosted models 时如实拒绝（插件
/// `signedInWith` 同一条）：这种账号登进来也调不了任何模型。
pub fn credential_from_me(
    access: &str,
    user_id: &str,
    system_id: &str,
    me: &Value,
) -> Result<Credential, String> {
    let org = org_of(me, "");
    if models_off(me, &org) {
        return Err(
            "Zed：这个账号的组织已关闭 Zed 托管模型（hosted models），无法使用".to_string(),
        );
    }
    let plan = plan_of(me, &org);
    Ok(Credential {
        access: access.to_string(),
        user_id: user_id.to_string(),
        system_id: system_id.to_string(),
        org,
        plan,
        login: user_field(me, "github_login")
            .or_else(|| user_field(me, "username"))
            .unwrap_or_default(),
        name: user_field(me, "name").unwrap_or_default(),
        display: who(me),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn me_with(orgs: Value, plans: Value, default: &str) -> Value {
        json!({
            "user": { "github_login": "octocat", "legacy_user_id": 42 },
            "organizations": orgs,
            "default_organization_id": default,
            "plans_by_organization": plans,
            "configuration_by_organization": {},
            "plan": { "plan_v3": "zed_pro" },
        })
    }

    #[test]
    fn org_pick_saved_usable_wins() {
        // 已保存的组织仍有可用套餐 → 保持它（哪怕默认组织也有套餐）
        let me = me_with(
            json!([{ "id": "org-a", "is_personal": true }, { "id": "org-b" }]),
            json!({ "org-a": "zed_pro", "org-b": "zed_business" }),
            "org-b",
        );
        assert_eq!(org_of(&me, "org-a"), "org-a");
    }

    #[test]
    fn org_pick_default_when_saved_unusable() {
        // 已保存的组织只剩免费套餐（不可用）→ 落到默认且可用的组织
        let me = me_with(
            json!([{ "id": "org-a", "is_personal": true }, { "id": "org-b" }]),
            json!({ "org-a": "zed_free", "org-b": "zed_pro" }),
            "org-b",
        );
        assert_eq!(org_of(&me, "org-a"), "org-b");
    }

    #[test]
    fn org_pick_personal_before_any_usable() {
        // 保存/默认都不可用时，个人组织优先于任意可用组织
        let me = me_with(
            json!([
                { "id": "org-team", "is_personal": false },
                { "id": "org-me", "is_personal": true },
                { "id": "org-def" },
            ]),
            json!({ "org-team": "zed_pro", "org-me": "zed_pro_trial", "org-def": "zed_free" }),
            "org-def",
        );
        assert_eq!(org_of(&me, ""), "org-me");
    }

    #[test]
    fn org_pick_models_off_is_not_usable() {
        // 关掉 hosted models 的组织即便有付费套餐也不算可用
        let mut me = me_with(
            json!([{ "id": "org-a" }, { "id": "org-b" }]),
            json!({ "org-a": "zed_pro", "org-b": "zed_pro" }),
            "org-a",
        );
        me["configuration_by_organization"] =
            json!({ "org-a": { "is_zed_model_provider_enabled": false } });
        assert_eq!(org_of(&me, "org-a"), "org-b");
    }

    #[test]
    fn org_pick_falls_back_to_saved_then_default_then_first() {
        // 谁都不可用：已保存 > 默认 > 第一个
        let me = me_with(
            json!([{ "id": "org-a" }, { "id": "org-b" }]),
            json!({ "org-a": "zed_free", "org-b": "zed_free" }),
            "org-b",
        );
        assert_eq!(org_of(&me, "org-a"), "org-a");
        assert_eq!(org_of(&me, ""), "org-b");
        let me2 = me_with(json!([{ "id": "org-a" }, { "id": "org-b" }]), json!({}), "gone");
        assert_eq!(org_of(&me2, ""), "org-a");
        // 完全没有组织：saved || default || ""
        let me3 = me_with(json!([]), json!({}), "org-def");
        assert_eq!(org_of(&me3, ""), "org-def");
        assert_eq!(org_of(&me3, "org-saved"), "org-saved");
    }

    #[test]
    fn plan_of_reads_org_entry_never_account_plan() {
        let me = me_with(
            json!([{ "id": "org-a" }]),
            json!({ "org-a": "zed_student" }),
            "org-a",
        );
        assert_eq!(plan_of(&me, "org-a"), "zed_student");
        // 组织已知但表里没有它 → 空串（无套餐），不许拿账号级 zed_pro 顶替
        let me2 = me_with(json!([{ "id": "org-a" }]), json!({}), "org-a");
        assert_eq!(plan_of(&me2, "org-a"), "");
        // 根本没有组织时才回退账号套餐
        let me3 = me_with(json!([]), json!({}), "");
        assert_eq!(plan_of(&me3, ""), "zed_pro");
    }

    #[test]
    fn plan_names_map_and_strip() {
        assert_eq!(plan_name("zed_pro"), "Pro");
        assert_eq!(plan_name("zed_pro_trial"), "Pro Trial");
        assert_eq!(plan_name("zed_business"), "Business");
        assert_eq!(plan_name("zed_student"), "Student");
        assert_eq!(plan_name("zed_vip"), "VIP");
        assert_eq!(plan_name("zed_free"), "Free");
        assert_eq!(plan_name(""), "Free");
        assert_eq!(plan_name("zed_enterprise"), "enterprise");
        assert_eq!(plan_name("custom"), "custom");
    }

    #[test]
    fn who_falls_back_in_order() {
        let me = json!({ "user": { "github_login": "  ", "username": "u2", "name": "N", "legacy_user_id": 7 } });
        assert_eq!(who(&me), "u2");
        let me = json!({ "user": { "legacy_user_id": 7 } });
        assert_eq!(who(&me), "7");
        let me = json!({ "user": {} });
        assert_eq!(who(&me), "Zed account");
    }

    #[test]
    fn record_round_trip_keeps_state() {
        let credential = Credential {
            access: "tok-1".to_string(),
            user_id: "12345".to_string(),
            system_id: "sys".to_string(),
            org: "org-a".to_string(),
            plan: "zed_pro".to_string(),
            login: "octocat".to_string(),
            name: "Octo".to_string(),
            display: "octocat".to_string(),
        };
        let record = json!({
            "accessToken": credential.access,
            "refreshToken": credential.state_json(),
            "userId": credential.user_id,
            "name": credential.display,
        });
        let back = Credential::from_record(&record).expect("凭证应可还原");
        assert_eq!(back, credential);
        assert_eq!(credential.account_id(), "zed-12345");
        // 状态 JSON 与插件的键集合一致（planName 派生值也在里面）
        let state: Value =
            serde_json::from_str(&credential.state_json()).expect("状态应是合法 JSON");
        assert_eq!(state["planName"], "Pro");
    }
}
