//! TeleAgent 跨请求会话。只有**流式**请求带显式会话 ID（body 字段或 `x-session-id`
//! 头）才复用；按账号、密钥、模型、工作目录、工具定义与网关调用方隔离。
//! `Turn`（owned guard）覆盖整个上游 SSE 生命周期，避免一个会话上并行 prompt；
//! 未成功 commit 即作废（通用部分见 `stateful_session`）。
use std::sync::Arc;

use axum::http::HeaderMap;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

pub use crate::server::core::providers::stateful_session::{delta_start, fingerprints, Conversation, Turn};
use crate::server::core::providers::stateful_session::{self, Registry};

static REGISTRY: Registry = Registry::new();

pub fn entry(
    account_id: &str,
    key: &str,
    model: &str,
    directory: &str,
    body: &Value,
    headers: &HeaderMap,
) -> Option<Arc<AsyncMutex<Conversation>>> {
    // 工具 schema 只在建会话时注入一次，schema 变化必须换会话。
    // tool_choice 是逐轮提示（见 `protocol::tool_choice_hint`），不进键。
    let tools = body.get("tools").map(Value::to_string).unwrap_or_default();
    REGISTRY.entry(&[account_id, key, model, directory, tools.as_str()], body, headers)
}

/// 复用会话时发送的新增消息：不重发 system（建会话时已发），跳过开头的
/// assistant（上游自己的回复）。为空 → None（调用方按全量重建处理）。
pub fn incremental_messages(body: &Value, start: usize) -> Option<Value> {
    let messages = body.get("messages")?.as_array()?;
    let fresh: Vec<Value> = stateful_session::new_messages(messages, start)?
        .into_iter()
        .filter(|m| m.get("role").and_then(Value::as_str) != Some("system"))
        .collect();
    if fresh.is_empty() { None } else { Some(Value::Array(fresh)) }
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
    use super::{delta_start, fingerprints, incremental_messages};
    use serde_json::json;

    #[test]
    fn increment_skips_system_and_upstream_reply() {
        let prev = json!([{"role":"system","content":"rules"},{"role":"user","content":"hello"}]);
        let next = json!([{"role":"system","content":"rules"},{"role":"user","content":"hello"},{"role":"assistant","content":"ok"},{"role":"user","content":"next"}]);
        let a = fingerprints(Some(&prev));
        let b = fingerprints(Some(&next));
        assert_eq!(delta_start(&a, &b), Some(2));
        let delta = incremental_messages(&json!({"messages": next}), 2).unwrap_or_default();
        let items = delta.as_array().cloned().unwrap_or_default();
        assert_eq!(items.len(), 1, "system 与上游自己的回复都不应重发");
        assert_eq!(items[0]["content"], "next");
        assert_eq!(delta_start(&a, &a), None, "历史完全相同不能重发最后一条 user");
        let changed = json!([{"role":"user","content":"modified"}]);
        assert!(delta_start(&a, &fingerprints(Some(&changed))).is_none());
    }
}
