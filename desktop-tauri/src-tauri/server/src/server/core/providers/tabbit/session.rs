//! Tabbit 会话缓存与历史指纹。会话仅在调用方提供明确的会话 ID（body 字段或
//! `x-session-id` 头）且为流式请求时复用；非流式辅助请求（标题、摘要）不读写会话。
//! 缓存以地区 + 账号 + token + 模型 + 网关调用方 + 会话 ID 隔离，避免不同聊天混线。
//! 通用设施（排队上限、失败即作废的 `Turn`、增量切片）见 `stateful_session`。
use std::sync::Arc;

use axum::http::HeaderMap;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

pub use crate::server::core::providers::stateful_session::{delta_start, fingerprints, Conversation, Turn};
use crate::server::core::providers::stateful_session::{self, Registry};

use super::region::Region;

static REGISTRY: Registry = Registry::new();

pub fn entry(
    region: Region,
    account_id: &str,
    token: &str,
    model: &str,
    body: &Value,
    headers: &HeaderMap,
) -> Option<Arc<AsyncMutex<Conversation>>> {
    REGISTRY.entry(&[region.provider_id(), account_id, token, model], body, headers)
}

/// 前缀之后新增的消息（跳过上游自己的 assistant 回复）；为空 → None（全量重建）。
pub fn delta_messages(messages: &[Value], start: usize) -> Option<Vec<Value>> {
    stateful_session::new_messages(messages, start)
}

/// 管理面只读快照：不暴露会话名、token 或上游返回内容。
pub fn stats() -> Value {
    REGISTRY.stats()
}

/// 删除空闲缓存项。执行中的会话不会被替换，避免管理清理破坏并发互斥。
pub fn reset_idle() -> usize {
    REGISTRY.reset_idle()
}

#[cfg(test)]
mod parity_tests {
    use super::{delta_messages, delta_start, fingerprints};
    use crate::server::core::providers::stateful_session::explicit_id;
    use axum::http::HeaderMap;
    use serde_json::json;

    #[test]
    fn exact_prefix_allows_incremental_and_edits_rebuild() {
        let original = json!([{"role":"user","content":"x"}]);
        let longer = json!([{"role":"user","content":"x"},{"role":"assistant","content":"y"},{"role":"user","content":"z"}]);
        let prev = fingerprints(Some(&original));
        let next = fingerprints(Some(&longer));
        assert_eq!(delta_start(&prev, &next), Some(1));
        // 上游自己的回复不回灌，只发新的 user
        let delta = delta_messages(longer.as_array().map(Vec::as_slice).unwrap_or(&[]), 1).unwrap_or_default();
        assert_eq!(delta.len(), 1);
        // 历史完全相同（重试）→ 不复用
        assert_eq!(delta_start(&prev, &prev), None);
        let changed = fingerprints(Some(&json!([{"role":"user","content":"changed"}])));
        assert_eq!(delta_start(&prev, &changed), None);
    }

    #[test]
    fn reuse_requires_explicit_conversation_id() {
        let none = HeaderMap::new();
        assert!(explicit_id(&json!({"messages":[]}), &none).is_none());
        assert_eq!(
            explicit_id(&json!({"metadata":{"conversation_id":"unique-123"}}), &none).as_deref(),
            Some("unique-123")
        );
    }
}
