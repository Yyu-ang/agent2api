//! TeleAgent 账号：手动粘贴 Session Key / 从本机导入登录态 + 公开形态。
//!
//! ── 为什么 TeleAgent 需要账号 ─────────────────────────────────
//! 凭证是 super-agent 的本地会话 key（`SUPER_AGENT_LOCAL_SESSION_KEY`）。
//! 网关能自动从本机 scheduler 进程环境提取（见 `providers::teleagent::auth`，
//! Unix 才有这条路），但提取到的登录态也要落成一条账号记录：账号页要展示它、
//! 用量要记账、API Key 的可用提供商白名单要能选它。零账号时保留匿名直转
//! （见 adapter 的 `allows_anonymous_default_session`），那是兜底不是常态。
//!
//! ── 记录里的字段 ────────────────────────────────────────────
//! ```text
//! { id, provider: "teleagent", name, accessToken(=session key 本体),
//!   tokenTail, desktop, source, priority, enabled, addedAt, updatedAt }
//! ```
//! 手动粘贴的 id 取 `teleagent-` + key 尾 8 位；本机导入用固定 id
//! `teleagent-desktop`（重复导入 = 更新同一条，不堆积）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic，不持锁做网络
//! （探活在 `api::accounts` 里先做，本文件的写方法只做落盘）。

use serde_json::{Map, Value};

use crate::server::core::account_store::priority::next_free_priority;
use crate::server::core::account_store::sql;
use crate::server::core::account_store::state::{mark_name_custom, StoredAccount};
use crate::server::core::account_store::store::{AccountStore, AccountStoreError};
use crate::server::core::account_store::store_util::{token_tail_of, truncate_chars};
use crate::server::core::providers::teleagent::auth;
use crate::server::core::providers::{kind_id, ProviderKind};
use crate::server::errors::GatewayError;
use crate::server::logging;

/// TeleAgent provider id（**从注册表推导**，不要手写字符串）。
pub(crate) const TELEAGENT_PROVIDER_ID: &str = kind_id(ProviderKind::TeleAgent);
/// 从本机导入的账号用固定 id（重复导入 = 更新同一条）。
pub(crate) const DESKTOP_ACCOUNT_ID: &str = "teleagent-desktop";

/// 备注名长度上限（与后端 truncate_chars(name, 100) 一致）
const MAX_NAME_LENGTH: usize = 100;

/// 从添加 payload 里取用户粘贴的 session key（`session_key` / `sessionKey` 都认）。
pub(crate) fn pasted_key(payload: &Value) -> String {
    payload
        .get("session_key")
        .or_else(|| payload.get("sessionKey"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string()
}

/// 本次转发该用哪一把 key：有账号 → 账号里存的；无账号 → 自动提取（匿名直转兜底）。
///
/// 账号指定的 key 为空/记录丢失时按 401 处理（提示重新添加），不静默回退到
/// 自动提取 —— 用户点名了账号，用的就必须是那把 key，否则账算不清。
pub(crate) fn session_key_for(store: &AccountStore, account_id: &str) -> Result<String, GatewayError> {
    if !account_id.trim().is_empty() {
        let key = store
            .teleagent_account_record(account_id)
            .and_then(|record| {
                record
                    .get("accessToken")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        if key.trim().is_empty() {
            return Err(GatewayError::with_status(
                401,
                "TeleAgent 账号缺少有效的 Session Key，请重新添加该账号",
            ));
        }
        return Ok(key);
    }
    auth::session_key(false).ok_or_else(|| {
        GatewayError::with_status(
            503,
            "无法获取 SUPER_AGENT_LOCAL_SESSION_KEY，请确保 TeleAgent 桌面应用正在运行",
        )
    })
}

impl AccountStore {
    // ─── 读：账号记录 ────────────────────────────────────────

    /// 取 TeleAgent 账号的**原始记录**（含 accessToken）。
    ///
    /// `account_id` 为空 → 取优先级最小的启用账号（与转发选路同一判据）；
    /// 找不到返回 None（调用方据此报 401/503，见 `session_key_for`）。
    pub fn teleagent_account_record(&self, account_id: &str) -> Option<Value> {
        let _guard = self.guard();
        if !account_id.is_empty() {
            let record = self.record_by_id(&_guard, account_id)?;
            return (record.provider() == TELEAGENT_PROVIDER_ID).then(|| record.to_value());
        }
        let mut candidates: Vec<StoredAccount> = self
            .records_for_provider(&_guard, TELEAGENT_PROVIDER_ID)
            .into_iter()
            .filter(StoredAccount::enabled)
            .collect();
        candidates.sort_by_key(StoredAccount::order_key);
        candidates.into_iter().next().map(|item| item.to_value())
    }

    // ─── 写：落账号 ──────────────────────────────────────────

    /// 手动添加：`key` 是已探活的 session key（探活在 `api::accounts` 里先做）。
    pub fn add_teleagent_account(
        &self,
        key: &str,
        name: Option<&str>,
        source: &str,
    ) -> Result<Value, AccountStoreError> {
        let key = key.trim().to_string();
        if key.is_empty() {
            return Err(AccountStoreError::new("Session Key 为空，无法添加", 400));
        }
        let id = format!("teleagent-{}", token_tail_of(&key));
        self.upsert_teleagent_record(&id, &key, name, source, false)
    }

    /// 从本机导入：自动提取 scheduler 进程环境里的 session key（`auth::session_key`
    /// 强制刷新，不走缓存）；提不到时 400 明说。
    pub fn import_teleagent_desktop_account(
        &self,
        source: &str,
    ) -> Result<Value, AccountStoreError> {
        let key = auth::session_key(true).ok_or_else(|| {
            AccountStoreError::new(
                "无法从本机提取 TeleAgent 登录态：请确保 TeleAgent 桌面应用正在运行（Windows 上若持续提不到，请改用「粘贴 Session Key」手动添加）",
                400,
            )
        })?;
        self.upsert_teleagent_record(DESKTOP_ACCOUNT_ID, &key, None, source, true)
    }

    /// 落盘（手动/导入共用）：同 id 已存在 → 更新 key 与 updatedAt，
    /// 保留用户改过的备注名与开关状态。
    fn upsert_teleagent_record(
        &self,
        id: &str,
        key: &str,
        name: Option<&str>,
        source: &str,
        desktop: bool,
    ) -> Result<Value, AccountStoreError> {
        let _guard = self.guard();
        let existing = self.record_by_id(&_guard, id);
        if let Some(existing) = existing.as_ref() {
            let existing_provider = existing.provider();
            if existing_provider != TELEAGENT_PROVIDER_ID {
                return Err(AccountStoreError::new(
                    format!(
                        "账号 id「{id}」已被{existing_provider}账号占用，无法添加 TeleAgent 账号（请先处理那个账号）"
                    ),
                    400,
                ));
            }
        }
        let explicit_name = name
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| truncate_chars(value, MAX_NAME_LENGTH));
        let explicit = explicit_name.is_some();
        let record_name = explicit_name
            .or_else(|| {
                existing
                    .as_ref()
                    .map(StoredAccount::name)
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| {
                if desktop {
                    "本机 TeleAgent".to_string()
                } else {
                    format!("TeleAgent 账号（…{}）", token_tail_of(key))
                }
            });
        let priority = match existing.as_ref() {
            Some(record) => record.priority(),
            None => {
                let used = self.with_conn(&_guard, |conn| sql::priorities_all(conn))?;
                next_free_priority(&used)
            }
        };
        let now = logging::now_ms();
        let mut record = Map::new();
        record.insert("id".to_string(), Value::String(id.to_string()));
        record.insert(
            "provider".to_string(),
            Value::String(TELEAGENT_PROVIDER_ID.to_string()),
        );
        record.insert("name".to_string(), Value::String(record_name.clone()));
        mark_name_custom(&mut record, explicit, existing.as_ref());
        record.insert("accessToken".to_string(), Value::String(key.to_string()));
        record.insert(
            "tokenTail".to_string(),
            Value::String(token_tail_of(key)),
        );
        record.insert("desktop".to_string(), Value::Bool(desktop));
        record.insert("source".to_string(), Value::String(source.to_string()));
        record.insert("priority".to_string(), Value::from(priority));
        record.insert(
            "enabled".to_string(),
            Value::Bool(existing.as_ref().map(StoredAccount::enabled).unwrap_or(true)),
        );
        record.insert(
            "addedAt".to_string(),
            Value::from(existing.as_ref().map(StoredAccount::added_at).unwrap_or(now)),
        );
        record.insert("updatedAt".to_string(), Value::from(now));
        let mut merged = record;
        if let Some(existing) = existing.as_ref() {
            for (key, value) in existing.fields() {
                merged.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        let saved = StoredAccount::from_map(merged);
        self.with_conn(&_guard, |conn| sql::put(conn, &saved))?;
        drop(_guard);
        logging::log(
            "[Accounts]",
            &format!("✅ TeleAgent 账号已保存: {record_name}（{id}，优先级 {priority}）"),
        );
        Ok(self.public_account(&saved))
    }

    /// 账号的公开形态（前端账号列表 / 会话摘要用；**不含 key 本体**）。
    pub fn to_teleagent_public_account(&self, record: &StoredAccount) -> Value {
        let proxy = crate::server::core::proxies::describe_account_proxy(Some(&record.proxy()));
        let mut public = Map::new();
        public.insert("id".to_string(), Value::String(record.id().to_string()));
        public.insert("provider".to_string(), Value::String(record.provider()));
        public.insert("name".to_string(), Value::String(record.name()));
        public.insert(
            "tokenTail".to_string(),
            Value::String(
                record
                    .get("tokenTail")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            ),
        );
        public.insert("desktop".to_string(), Value::Bool(record.is_desktop()));
        public.insert("source".to_string(), Value::String(record.source()));
        public.insert("priority".to_string(), Value::from(record.priority()));
        public.insert("enabled".to_string(), Value::Bool(record.enabled()));
        public.insert("addedAt".to_string(), Value::from(record.added_at()));
        public.insert("updatedAt".to_string(), Value::from(record.updated_at()));
        public.insert("proxy".to_string(), proxy);
        public.insert(
            "rateLimits".to_string(),
            record
                .get("rateLimits")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
        );
        public.insert("available".to_string(), Value::Bool(true));
        Value::Object(public)
    }
}
