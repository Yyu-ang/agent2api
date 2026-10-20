//! Zed 账号的落盘与公开形态。账号以 `userId` 识别；凭证是「访问令牌 +
//! 状态 JSON」一对（见 `providers::zed::credentials` 的模块头）：
//!   * `accessToken` —— 访问令牌（转发时换模型令牌的本钱）；
//!   * `refreshToken` —— **不是** refresh token（Zed 没有），存的是状态
//!     JSON `{userId, systemId, org, plan, planName, login, name}`，与
//!     插件把同一段 JSON 存进 oauth 条目 `refresh` 字段逐字对应。公开
//!     形态**不透出**这两样本体（只给 tokenTail 与派生展示字段）。

use serde_json::{json, Map, Value};

use crate::server::core::providers::kind_id;
use crate::server::core::providers::zed::credentials::Credential;
use crate::server::core::providers::ProviderKind;
use crate::server::logging;

use super::priority::next_free_priority;
use super::sql;
use super::state::{mark_name_custom, StoredAccount};
use super::store::{AccountStore, AccountStoreError};
use super::store_util::{token_tail_of, truncate_chars};

/// 本家的 provider id 常量（别处按它判「是不是 Zed」时用注册表，不写字面量）
pub const ZED_PROVIDER_ID: &str = kind_id(ProviderKind::Zed);

impl AccountStore {
    /// 取一条 Zed 账号的**原始记录**（`account_id` 为空时取本家优先级
    /// 最高且有令牌的那条，与各家 `*_account_record` 同一口径）。
    pub fn zed_account_record(&self, account_id: &str) -> Option<Value> {
        let guard = self.guard();
        if !account_id.is_empty() {
            let record = self.record_by_id(&guard, account_id)?;
            return (record.provider() == ZED_PROVIDER_ID).then(|| record.to_value());
        }
        self.records_for_provider(&guard, ZED_PROVIDER_ID)
            .into_iter()
            .filter(|record| record.enabled() && record.has_token())
            .min_by_key(|record| record.order_key())
            .map(|record| record.to_value())
    }

    /// 落一条 Zed 账号（网页登录完成 / 手填凭证校验后都走这里）。
    ///
    /// 身份匹配只用 userId：同一 userId 再登录就是更新同一条记录（令牌与
    /// 状态整体换新 —— 重新登录的语义就是「旧的那套作废」，与别家「空值
    /// 不覆盖」不同：这里两个字段都是登录流程现产的完整值，不存在半份）。
    pub fn add_zed_account(
        &self,
        credential: &Credential,
        name: Option<&str>,
        source: &str,
    ) -> Result<Value, AccountStoreError> {
        let user_id = credential.user_id.trim();
        if user_id.is_empty() {
            return Err(AccountStoreError::bad_request(
                "Zed 账号缺少 userId，无法保存",
            ));
        }
        let guard = self.guard();
        let existing = self
            .records_for_provider(&guard, ZED_PROVIDER_ID)
            .into_iter()
            .find(|record| record.user_id() == user_id);
        let id = match existing.as_ref() {
            Some(record) => record.id().to_string(),
            None => credential.account_id(),
        };
        if existing.is_none() && self.record_by_id(&guard, &id).is_some() {
            return Err(AccountStoreError::new(
                "Zed 账号 ID 已被其它账号占用，请先核对账号记录",
                409,
            ));
        }
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
            .unwrap_or_else(|| credential.display.clone());

        let mut fields = existing
            .as_ref()
            .map(|record| record.fields().clone())
            .unwrap_or_default();
        fields.insert(
            "accessToken".to_string(),
            Value::String(credential.access.trim().to_string()),
        );
        fields.insert(
            "refreshToken".to_string(),
            Value::String(credential.state_json()),
        );
        fields.insert(
            "userId".to_string(),
            Value::String(user_id.to_string()),
        );
        let priority = match existing.as_ref() {
            Some(record) => record.priority(),
            None => {
                let used = self
                    .with_conn(&guard, |conn| sql::priorities_all(conn))
                    .unwrap_or_default();
                next_free_priority(&used)
            }
        };
        fields.insert("id".to_string(), Value::String(id.clone()));
        fields.insert(
            "provider".to_string(),
            Value::String(ZED_PROVIDER_ID.to_string()),
        );
        fields.insert(
            "name".to_string(),
            Value::String(truncate_chars(&record_name, 100)),
        );
        mark_name_custom(
            &mut fields,
            name.is_some_and(|value| !value.trim().is_empty()),
            existing.as_ref(),
        );
        fields.insert(
            "tokenTail".to_string(),
            Value::String(token_tail_of(&credential.access)),
        );
        fields.insert("priority".to_string(), Value::from(priority));
        fields.insert(
            "enabled".to_string(),
            Value::Bool(existing.as_ref().map(StoredAccount::enabled).unwrap_or(true)),
        );
        // 本家没有「从桌面端导入登录态」这条路（见 providers::zed 模块头）
        fields.insert("desktop".to_string(), Value::Bool(false));
        fields.insert("source".to_string(), Value::String(source.to_string()));
        fields.insert(
            "addedAt".to_string(),
            Value::from(
                existing
                    .as_ref()
                    .map(StoredAccount::added_at)
                    .unwrap_or_else(logging::now_ms),
            ),
        );
        fields.insert("updatedAt".to_string(), Value::from(logging::now_ms()));
        fields.insert("rateLimits".to_string(), json!({}));
        let record = StoredAccount::from_map(fields);
        self.with_conn(&guard, |conn| sql::put(conn, &record))?;
        logging::log(
            "[Accounts]",
            &format!("✅ Zed 账号已保存（{record_name}，优先级 {priority}）"),
        );
        Ok(self.public_account(&record))
    }

    /// 回写 repick / 用量刷新选出的组织与套餐（只动状态 JSON，不动令牌、
    /// 不动 `updatedAt` —— 这是内部状态同步，不是用户改动，与
    /// `mark_zcode_claim` 同一口径）。返回是否真的写了（组织与套餐都没变
    /// 时不写盘）。
    pub fn update_zed_organization(
        &self,
        account_id: &str,
        org: &str,
        plan: &str,
    ) -> Result<bool, AccountStoreError> {
        let guard = self.guard();
        let Some(mut record) = self.record_by_id(&guard, account_id) else {
            return Err(AccountStoreError::not_found("账号不存在"));
        };
        if record.provider() != ZED_PROVIDER_ID {
            return Err(AccountStoreError::bad_request("该账号不是 Zed 账号"));
        }
        let Some(credential) = Credential::from_record(&record.to_value()) else {
            return Err(AccountStoreError::bad_request(
                "Zed 账号缺少访问令牌或 userId，无法回写组织",
            ));
        };
        if credential.org == org && credential.plan == plan {
            return Ok(false);
        }
        let next = credential.with_org(org, plan);
        record.set(
            "refreshToken",
            Value::String(next.state_json()),
        );
        self.with_conn(&guard, |conn| sql::update_in_place(conn, &record))?;
        logging::verbose(
            "[Zed]",
            &format!(
                "账号 {account_id} 的组织已更新为 {org}（套餐 {}）",
                if plan.is_empty() { "无" } else { plan }
            ),
        );
        Ok(true)
    }

    /// Zed 账号的公开形态（界面读它）。字段名与语义与各家对齐；本家特有
    /// 的是 `planName`（套餐人读名，账号行直接展示）与 `organizationId`
    /// （当前选中的组织，repick 后会变，给用户一个可见的事实）。
    pub fn to_zed_public_account(&self, record: &StoredAccount) -> Value {
        let credential = Credential::from_record(&record.to_value());
        let available = credential.is_some();
        let mut public = Map::new();
        for key in ["id", "provider", "name", "userId", "source", "tokenTail"] {
            public.insert(
                key.to_string(),
                record.get(key).cloned().unwrap_or(Value::Null),
            );
        }
        public.insert("edition".to_string(), Value::Null);
        public.insert("editionLabel".to_string(), Value::Null);
        // 本家没有续期协议（见 providers::zed::adapter 模块头），恒 false：
        // 前端据此不给这个账号显示「续期」类按钮
        public.insert("hasRefreshToken".to_string(), Value::Bool(false));
        public.insert(
            "planName".to_string(),
            credential
                .as_ref()
                .map(|credential| Value::String(credential.plan_name()))
                .unwrap_or(Value::Null),
        );
        public.insert(
            "organizationId".to_string(),
            credential
                .as_ref()
                .map(|credential| Value::String(credential.org.clone()))
                .unwrap_or(Value::Null),
        );
        public.insert("priority".to_string(), Value::from(record.priority()));
        public.insert("enabled".to_string(), Value::Bool(record.enabled()));
        public.insert("addedAt".to_string(), Value::from(record.added_at()));
        public.insert("updatedAt".to_string(), Value::from(record.updated_at()));
        public.insert(
            "proxy".to_string(),
            crate::server::core::proxies::describe_account_proxy(Some(&record.proxy())),
        );
        public.insert(
            "rateLimits".to_string(),
            record.get("rateLimits").cloned().unwrap_or_else(|| json!({})),
        );
        public.insert("desktop".to_string(), Value::Bool(false));
        public.insert("available".to_string(), Value::Bool(available));
        Value::Object(public)
    }
}
