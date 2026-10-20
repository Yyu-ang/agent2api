//! MiniMax Code 账号：手动添加（粘贴凭证）/ 设备流登录落账号、续期回写、
//! 公开形态。
//!
//! ── 形态（与 cline 那条最像，参数化方式相同）────────────────────
//! 凭证就是一对 OAuth 令牌（access JWT + refresh）+ 过期时间，没有
//! 第二凭证、没有桌面端登录态（mcode 的登录态不以可导入文件形式存在，
//! 因此**没有 desktop 记录形态**，`desktop` 恒 false）。两个地区是两家
//! provider，账号形态完全一样，差别只有 `provider` 字段与 id 前缀 ——
//! 每个入口都收 `provider: &str`，内部不做地区分支（站点差异在
//! `providers::minimaxcode::region`，不进账号层）。
//!
//! **账号 id 带 provider 前缀**（`minimax-code-<uid>`）：同一个 MiniMax
//! 账号在两个地区各有独立令牌（两套账号体系），两条记录必须能并存；
//! 只带 uid 会撞 id 被当成「更新同一条」（与 cline 两池同理）。
//!
//! ── 账号字段 ────────────────────────────────────────────────
//! ```text
//!   { id, provider: "minimax-code" | "minimax-code-global", name, uid,
//!     email, displayName, accessToken, refreshToken, tokenTail,
//!     expiresAt, priority, enabled, desktop: false, source, addedAt,
//!     updatedAt, rateLimits }
//! ```
//! 身份字段落在 `uid`（不是 cline 的 `account`、也不是别家的 `userId`）：
//! 上游 JWT 的 `account_id` 声明就是这个值，签名账号 API 的 `user_id`
//! 参数也用它 —— 一处落盘、三处消费（展示 / 会话 `account.uid` / 签到），
//! 见 `store_view::identifier_key_of` 的对应分支。`email` 单独落盘
//! （展示名兜底与账号辨认用，来源是登录 payload 或 JWT 的 email 声明；
//! v1 不调 identity 接口补，见 `providers::minimaxcode` 模块头缺口 1）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! 本文件全是「读-改-写」存储操作，没有任何网络请求（持锁不做网络）。
//! 绝不 unwrap/expect（release 是 panic=abort）。

use serde_json::{Map, Value};

use crate::server::core::account_store::priority::next_free_priority;
use crate::server::core::account_store::sql;
use crate::server::core::account_store::state::{mark_name_custom, StoredAccount};
use crate::server::core::account_store::store::{AccountStore, AccountStoreError};
use crate::server::core::account_store::store_util::{token_tail_of, truncate_chars};
use crate::server::core::account_store::{
    is_minimaxcode_family, CredentialWrite, MAX_TOKEN_LENGTH,
};
use crate::server::core::providers::minimaxcode::credentials;
use crate::server::core::providers::minimaxcode::region::Region;
use crate::server::logging;

/// 备注名长度上限（与另外几家一致）
const MAX_NAME_LENGTH: usize = 100;

/// 账号标识长度上限（uid / email 会进界面）
const MAX_IDENTITY_LENGTH: usize = 256;

/// 拼一个 MiniMax Code 账号的记录 id：`<provider>-<账号标识>`。
/// 前缀必须带（理由见模块头：同一账号两个地区各一条记录）。
fn record_id(provider: &str, key: &str) -> String {
    format!("{provider}-{}", truncate_chars(key, MAX_IDENTITY_LENGTH))
}

impl AccountStore {
    // ─── 读：按地区取记录 ────────────────────────────────────

    /// 取某地区的一条账号记录（原始字段形态）。
    ///
    /// `provider` 必须是两家之一 —— 它是定位记录的必要条件（同一账号
    /// 两个地区各一条）。`account_id` 为空 → 该地区组内优先级最小的
    /// 启用账号；点名了却对不上 provider → None（如实没有，不回落到
    /// 队首，那会变成「选了 A、用的是 B」）。
    pub fn minimaxcode_account_record(&self, provider: &str, account_id: &str) -> Option<Value> {
        if !is_minimaxcode_family(provider) {
            return None;
        }
        let guard = self.guard();
        if !account_id.is_empty() {
            let record = self.record_by_id(&guard, account_id)?;
            return (record.provider() == provider).then(|| record.to_value());
        }
        let mut candidates: Vec<StoredAccount> = self
            .records_for_provider(&guard, provider)
            .into_iter()
            .filter(StoredAccount::enabled)
            .collect();
        candidates.sort_by_key(StoredAccount::order_key);
        candidates.into_iter().next().map(|item| item.to_value())
    }

    /// 这条记录是不是 MiniMax Code 系（适配器按 id 找人时用，不看地区）。
    pub fn minimaxcode_account_provider(&self, account_id: &str) -> Option<String> {
        if account_id.is_empty() {
            return None;
        }
        let guard = self.guard();
        let record = self.record_by_id(&guard, account_id)?;
        let provider = record.provider();
        is_minimaxcode_family(&provider).then_some(provider)
    }

    // ─── 写：手动添加 / 登录落账号 ────────────────────────────

    /// 添加/更新一个 MiniMax Code 账号（`POST /api/accounts` 的 minimax
    /// 分支，也是设备流登录成功后的落账号入口——两条路同一入口，校验
    /// 与兜底只有一份）。
    ///
    /// 接受的 payload（键名多形态兼容：网关口径、OAuth 原名、插件的
    /// 令牌对象形态都收）：
    ///   - `accessToken` / `access_token` / `access` / `token`（任一非空）；
    ///   - `refreshToken` / `refresh_token` / `refresh`（可选，没有则无法
    ///     自动续期）；
    ///   - `expiresAt` / `expires`（毫秒，可选；缺失时从 JWT 的 `exp`
    ///     推；插件形态的 `expires` 也是毫秒时间戳）；
    ///   - `uid` / `accountId` / `account`（可选；缺失时从 JWT 的
    ///     `account_id` → `sub` 推）；
    ///   - `email`（可选；缺失时从 JWT 的 email 声明推）；
    ///   - `name`（可选，备注名；也可单独走 `name` 参数）。
    ///
    /// 校验口径刻意薄（与 cline 同理）：只做「token 非空 + 长度合理」。
    /// 令牌真伪离线判不了（上游签发、网关无公钥），真正的判定是转发时
    /// 的上游响应；形状检查只会误伤上游将来换的令牌格式。
    ///
    /// 撞 id 时：同 provider 就地更新（保留优先级与启用状态、沿用用户
    /// 改过的备注名），撞到别的 provider 报错 409。
    pub fn add_minimaxcode_account(
        &self,
        provider: &str,
        payload: &Value,
        name: Option<&str>,
    ) -> Result<Value, AccountStoreError> {
        if !is_minimaxcode_family(provider) {
            return Err(AccountStoreError::new(
                format!("{provider} 不是 MiniMax Code 系的提供商"),
                400,
            ));
        }
        let Some(object) = payload.as_object() else {
            return Err(AccountStoreError::new("上传内容必须是 JSON 对象", 400));
        };
        let access = pick(object, &["accessToken", "access_token", "access", "token"]);
        if access.is_empty() {
            return Err(AccountStoreError::new(
                "缺少 accessToken（可用「设备流登录」或粘贴 mcode 的令牌）",
                400,
            ));
        }
        let refresh = pick(object, &["refreshToken", "refresh_token", "refresh"]);
        if access.chars().count() > MAX_TOKEN_LENGTH || refresh.chars().count() > MAX_TOKEN_LENGTH {
            return Err(AccountStoreError::new(
                "accessToken 或 refreshToken 过长",
                400,
            ));
        }
        // 过期时间：显式值优先，其次 JWT 的 exp
        let expires_at = object
            .get("expiresAt")
            .and_then(number)
            .or_else(|| object.get("expires").and_then(number))
            .or_else(|| credentials::expires_at_from_jwt(&access));
        // 身份：uid（JWT account_id → sub）与 email（JWT email 声明）
        let claims = credentials::jwt_claims(&access);
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
            let given = pick(object, &["uid", "accountId", "account", "userId"]);
            if !given.is_empty() {
                given
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
            let given = pick(object, &["email"]);
            if !given.is_empty() {
                given
            } else {
                claim("email")
            }
        };
        let display = pick(object, &["displayName", "display_name"]);
        // id：优先 uid / email（同地区重复添加合并成更新），都拿不到才
        // 按 token 指纹生成稳定 id
        let id = {
            let key = if !uid.is_empty() {
                uid.clone()
            } else if !email.is_empty() {
                email.clone()
            } else {
                format!("token-{}", fingerprint(&access))
            };
            record_id(provider, &key)
        };

        let _guard = self.guard();
        let existing = self.record_by_id(&_guard, &id);
        if let Some(existing) = existing.as_ref() {
            if existing.provider() != provider {
                return Err(AccountStoreError::new(
                    format!("账号 ID {id} 已被其它提供商的账号占用，请改用其它标识"),
                    409,
                ));
            }
        }
        let now = logging::now_ms();
        // 备注名兜底链：调用方给的 → 原有 → email → uid → 站点名 + token 尾号
        let region = Region::from_provider_id(provider);
        let record_name = name
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                existing
                    .as_ref()
                    .map(StoredAccount::name)
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| {
                if !email.is_empty() {
                    email.clone()
                } else if !display.is_empty() {
                    display.clone()
                } else if !uid.is_empty() {
                    uid.clone()
                } else {
                    let site = region
                        .map(|item| item.site_name())
                        .unwrap_or("MiniMax Code");
                    format!("{site} {}", token_tail_of(&access))
                }
            });
        let priority = existing
            .as_ref()
            .map(StoredAccount::priority)
            .unwrap_or_else(|| {
                let used = self
                    .with_conn(&_guard, |conn| sql::priorities_all(conn))
                    .unwrap_or_default();
                next_free_priority(&used)
            });
        let mut fields: Map<String, Value> = existing
            .as_ref()
            .map(|item| item.fields().clone())
            .unwrap_or_default();
        fields.insert("id".to_string(), Value::String(id));
        fields.insert("provider".to_string(), Value::String(provider.to_string()));
        fields.insert(
            "name".to_string(),
            Value::String(truncate_chars(&record_name, MAX_NAME_LENGTH)),
        );
        mark_name_custom(
            &mut fields,
            name.is_some_and(|value| !value.trim().is_empty()),
            existing.as_ref(),
        );
        if !uid.is_empty() {
            fields.insert(
                "uid".to_string(),
                Value::String(truncate_chars(&uid, MAX_IDENTITY_LENGTH)),
            );
        }
        if !email.is_empty() {
            fields.insert(
                "email".to_string(),
                Value::String(truncate_chars(&email, MAX_IDENTITY_LENGTH)),
            );
        }
        if !display.is_empty() {
            fields.insert(
                "displayName".to_string(),
                Value::String(truncate_chars(&display, MAX_IDENTITY_LENGTH)),
            );
        }
        fields.insert("accessToken".to_string(), Value::String(access.clone()));
        if !refresh.is_empty() {
            fields.insert("refreshToken".to_string(), Value::String(refresh.clone()));
        }
        fields.insert(
            "tokenTail".to_string(),
            Value::String(token_tail_of(&access)),
        );
        if let Some(expires) = expires_at.filter(|value| *value > 0.0) {
            fields.insert(
                "expiresAt".to_string(),
                crate::server::core::account_store::state::json_number(expires),
            );
        }
        fields.insert("priority".to_string(), Value::from(priority));
        fields.insert(
            "enabled".to_string(),
            Value::Bool(
                existing
                    .as_ref()
                    .map(StoredAccount::enabled)
                    .unwrap_or(true),
            ),
        );
        // 本家没有桌面端实时账号形态（见模块头）：恒为 false
        fields.insert("desktop".to_string(), Value::Bool(false));
        fields.insert(
            "source".to_string(),
            Value::String(
                existing
                    .as_ref()
                    .map(|item| item.source())
                    .unwrap_or_else(|| "manual".to_string()),
            ),
        );
        fields.insert(
            "addedAt".to_string(),
            Value::from(
                existing
                    .as_ref()
                    .map(StoredAccount::added_at)
                    .filter(|value| *value != 0)
                    .unwrap_or(now),
            ),
        );
        fields.insert("updatedAt".to_string(), Value::from(now));
        // 清掉可能来自别家的残留字段（同一 id 曾被别家占用过时）；**uid /
        // email 是本家的身份字段，不在清单里**（与 cline 清 uid 的口径
        // 不同——那家的身份键是 account，本家的就是 uid）
        for key in [
            "edition",
            "endpoint",
            "prefixPath",
            "platform",
            "deviceId",
            "account",
            "userId",
        ] {
            fields.remove(key);
        }
        let saved = StoredAccount::from_map(fields);
        let is_new = existing.is_none();
        self.with_conn(&_guard, |conn| sql::put(conn, &saved))?;
        logging::log(
            "[Accounts]",
            &format!(
                "{} MiniMax Code 账号{}: {}（{}）",
                if is_new { "✅" } else { "🔄" },
                if is_new { "已添加" } else { "已更新" },
                saved.name(),
                crate::server::core::providers::label_of(provider),
            ),
        );
        Ok(self.public_account(&saved))
    }

    /// 续期成功后回写新令牌（`minimaxcode::refresh::persist_refresh` 调用）。
    ///
    /// 比较-再写（与 cline 同一纪律）：记录里此刻的凭证仍等于续期前那份
    /// 快照才写，否则返回 [`CredentialWrite::Stale`]——续期是秒级网络
    /// 动作，期间用户可能重登、或另一轮单飞先落地；本家的 refresh
    /// token 又是单次使用的，拿旧结果覆盖新凭证等于把新登录态冲掉，
    /// 后果比别家更重（旧 refresh 再发一次就是 invalid_grant 登出）。
    pub fn update_minimaxcode_account_tokens_if_current(
        &self,
        id: &str,
        expected_access_token: &str,
        expected_refresh_token: &str,
        access_token: &str,
        refresh_token: &str,
        expires_at: Option<f64>,
    ) -> Result<CredentialWrite, String> {
        let _guard = self.guard();
        let Some(mut record) = self.record_by_id(&_guard, id) else {
            // 账号已被删除：结果无处可写，也不该新建记录
            return Ok(CredentialWrite::Stale);
        };
        if record.is_desktop() {
            return Err("MiniMax Code 没有桌面端账号形态，不应走到回写".to_string());
        }
        if !is_minimaxcode_family(&record.provider()) {
            return Err(format!("账号 {id} 不是 MiniMax Code 账号"));
        }
        if record.access_token() != expected_access_token
            || record.refresh_token() != expected_refresh_token
        {
            return Ok(CredentialWrite::Stale);
        }
        if !access_token.is_empty() {
            record.set("accessToken", Value::String(access_token.to_string()));
            record.set("tokenTail", Value::String(token_tail_of(access_token)));
        }
        if !refresh_token.is_empty() {
            record.set("refreshToken", Value::String(refresh_token.to_string()));
        }
        if let Some(value) = expires_at.filter(|value| *value > 0.0) {
            record.set(
                "expiresAt",
                crate::server::core::account_store::state::json_number(value),
            );
        }
        record.set_updated_at(logging::now_ms());
        self.with_conn(&_guard, |conn| sql::update_in_place(conn, &record))
            .map_err(|error| error.message)?;
        Ok(CredentialWrite::Written)
    }
    /// MiniMax Code 账号的公开形态（**不含 token**）。
    ///
    /// 身份字段是 `uid` + `email`（与 `identifier_key_of` 的 uid 口径
    /// 一致）；展示名推导比 cline 简单：记录名是自动生成的（为空或与
    /// email / uid 同字）时按 email → displayName → uid 现算，用户改过
    /// 备注名就尊重它。
    pub fn to_minimaxcode_public_account(&self, record: &StoredAccount) -> Value {
        let value = record.to_value();
        let mut out = Map::new();
        out.insert("id".to_string(), Value::String(record.id().to_string()));
        // provider 取记录自己的：两个地区共用这份公开形态，回错地区会让
        // 账号显示在另一个分组里
        out.insert("provider".to_string(), Value::String(record.provider()));
        let uid = record.uid();
        let email = value
            .get("email")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        let stored_display = value
            .get("displayName")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        let display = if !email.is_empty() {
            email.clone()
        } else if !stored_display.is_empty() {
            stored_display.clone()
        } else {
            uid.clone()
        };
        let stored_name_value = record.name();
        let stored_name = stored_name_value.trim();
        let auto_name = stored_name.is_empty()
            || stored_name == uid
            || (!email.is_empty() && stored_name == email.as_str());
        let name = if auto_name && !display.is_empty() {
            display.clone()
        } else {
            stored_name_value.clone()
        };
        out.insert("name".to_string(), Value::String(name));
        if !uid.is_empty() {
            out.insert("uid".to_string(), Value::String(uid));
        }
        if !email.is_empty() {
            out.insert("email".to_string(), Value::String(email));
        }
        if !display.is_empty() {
            out.insert("displayName".to_string(), Value::String(display));
        }
        out.insert("tokenTail".to_string(), Value::String(token_tail(&value)));
        out.insert(
            "hasRefreshToken".to_string(),
            Value::Bool(
                value
                    .get("refreshToken")
                    .and_then(Value::as_str)
                    .map(|text| !text.trim().is_empty())
                    .unwrap_or(false),
            ),
        );
        out.insert("priority".to_string(), Value::from(record.priority()));
        out.insert("enabled".to_string(), Value::Bool(record.enabled()));
        out.insert("desktop".to_string(), Value::Bool(record.is_desktop()));
        out.insert("source".to_string(), Value::String(record.source()));
        out.insert("addedAt".to_string(), Value::from(record.added_at()));
        out.insert("updatedAt".to_string(), Value::from(record.updated_at()));
        // rateLimits 是**选路的输入**不是展示字段（限额冷却写在记录里、
        // 选路只在公开形态上读），漏了它冷却就没人看得见——与各家同款
        // 必带（cline 那次的教训，见它的公开形态注释）。
        out.insert(
            "rateLimits".to_string(),
            value
                .get("rateLimits")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
        );
        out.insert(
            "available".to_string(),
            Value::Bool(record.enabled() && record.has_credentials()),
        );
        if let Some(expires) = value.get("expiresAt").filter(|value| !value.is_null()) {
            out.insert("expiresAt".to_string(), expires.clone());
        }
        // 出网代理回显（与其余各家同形，漏了界面一律显示「直连」）
        out.insert(
            "proxy".to_string(),
            crate::server::core::proxies::describe_account_proxy(Some(&record.proxy())),
        );
        Value::Object(out)
    }
}

/// 取候选键里第一个非空字符串（去空白）
fn pick(object: &Map<String, Value>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(text) = object.get(*key).and_then(Value::as_str) {
            let text = text.trim();
            if !text.is_empty() {
                return text.to_string();
            }
        }
    }
    String::new()
}

/// 数字取值（容忍字符串形态）
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok().filter(|v| v.is_finite()),
        _ => None,
    }
}

/// 记录里的 `tokenTail`（公开形态照搬；缺失时给空串）
fn token_tail(value: &Value) -> String {
    value
        .get("tokenTail")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// token 的短指纹（生成稳定 id 用，FNV-1a，与 cline 同款）
fn fingerprint(token: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in token.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 每条用例一个独立临时库（与 cline_accounts 的测试同款守卫）
    fn store(label: &str) -> (AccountStore, crate::server::db::test_temp::TempDb) {
        let (db, guard) =
            crate::server::db::test_temp::TempDb::open(&format!("minimaxcode-accounts-{label}"));
        (AccountStore::with_db(Some(db)), guard)
    }

    /// 造一枚带 account_id / sub / exp 的假 JWT
    fn fake_jwt() -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let header = URL_SAFE_NO_PAD.encode(b"{\"alg\":\"none\"}");
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "sub": "user-1",
                "account_id": "acc-9",
                "exp": 2_000_000_000_i64,
            }))
            .unwrap_or_default(),
        );
        format!("{header}.{payload}.sig")
    }

    #[test]
    fn add_then_record_reads_back_same_fields() {
        let (store, _db) = store("add-read");
        let access = fake_jwt();
        let public = store
            .add_minimaxcode_account(
                "minimax-code",
                &json!({ "accessToken": access.clone(), "refreshToken": "r1" }),
                None,
            )
            .unwrap_or_else(|error| panic!("{}", error.message));
        // id 带 provider 前缀、uid 来自 JWT account_id
        assert_eq!(public["id"], "minimax-code-acc-9");
        assert_eq!(public["provider"], "minimax-code");
        assert_eq!(public["uid"], "acc-9");
        assert_eq!(public["hasRefreshToken"], true);
        assert_eq!(public["expiresAt"].as_f64(), Some(2_000_000_000_000.0));
        // 落盘记录与读取一致（五链验证的第 4 链：写什么读什么）
        let record = store
            .minimaxcode_account_record("minimax-code", "minimax-code-acc-9")
            .unwrap_or(Value::Null);
        assert_eq!(record["uid"], "acc-9");
        assert_eq!(record["refreshToken"], "r1");
        assert_eq!(record["accessToken"], access);
        // 地区隔离：国际版查不到国内版的记录
        assert!(store
            .minimaxcode_account_record("minimax-code-global", "minimax-code-acc-9")
            .is_none());
        // 空 id 取该地区队首
        assert!(store
            .minimaxcode_account_record("minimax-code", "")
            .is_some());
    }

    #[test]
    fn token_writeback_compares_before_writing() {
        let (store, _db) = store("writeback");
        let access = fake_jwt();
        let public = store
            .add_minimaxcode_account(
                "minimax-code-global",
                &json!({ "accessToken": access.clone(), "refreshToken": "r1" }),
                None,
            )
            .unwrap_or_else(|error| panic!("{}", error.message));
        let id = public["id"].as_str().unwrap_or("").to_string();
        // 期望值对得上 → Written，新值落盘
        let written = store
            .update_minimaxcode_account_tokens_if_current(
                &id,
                &access,
                "r1",
                "new-access",
                "r2",
                Some(2_100_000_000_000.0),
            )
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(written, CredentialWrite::Written);
        let record = store
            .minimaxcode_account_record("minimax-code-global", &id)
            .unwrap_or(Value::Null);
        assert_eq!(record["accessToken"], "new-access");
        assert_eq!(record["refreshToken"], "r2");
        // 期望值还是旧的 → Stale，不覆盖
        let stale = store
            .update_minimaxcode_account_tokens_if_current(
                &id,
                &access,
                "r1",
                "other-access",
                "r3",
                None,
            )
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(stale, CredentialWrite::Stale);
        let record = store
            .minimaxcode_account_record("minimax-code-global", &id)
            .unwrap_or(Value::Null);
        assert_eq!(record["accessToken"], "new-access");
    }

    #[test]
    fn add_rejects_missing_token_and_wrong_family() {
        let (store, _db) = store("reject");
        assert!(store
            .add_minimaxcode_account("minimax-code", &json!({}), None)
            .is_err());
        assert!(store
            .add_minimaxcode_account("cline-free", &json!({ "accessToken": "a.b.c" }), None,)
            .is_err());
    }
}
