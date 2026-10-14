//! Tabbit 会话缓存与历史指纹。会话仅在调用方提供明确的 conversation_id 时复用。
//! 缓存以 provider + account + token + model + conversation ID 隔离，避免不同聊天混线。
//! 每个缓存项的异步互斥锁持续覆盖完整 SSE 生命周期。
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use sha2::{Digest, Sha256};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

use super::region::Region;

const TTL_MS: i64 = 30 * 60_000;
const MAX_SESSIONS: usize = 256;

#[derive(Default)]
pub struct Conversation {
    pub session_id: Option<String>,
    pub sent: Vec<String>,
}

impl Conversation {
    pub fn commit(&mut self, session_id: String, fingerprints: Vec<String>) {
        self.session_id = Some(session_id);
        self.sent = fingerprints;
    }
    pub fn reset(&mut self) {
        self.session_id = None;
        self.sent.clear();
    }
}

struct Entry {
    state: Arc<AsyncMutex<Conversation>>,
    touched: i64,
}
static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();

pub fn explicit_id(body: &Value) -> Option<&str> {
    body.get("conversation_id").and_then(Value::as_str)
        .or_else(|| body.get("metadata").and_then(|m| m.get("conversation_id")).and_then(Value::as_str))
        .or_else(|| body.get("metadata").and_then(|m| m.get("agent2api_conversation_id")).and_then(Value::as_str))
        .map(str::trim).filter(|id| !id.is_empty() && id.len() <= 512)
}

pub fn entry(
    region: Region, account_id: &str, token: &str, model: &str, body: &Value,
) -> Option<Arc<AsyncMutex<Conversation>>> {
    if body.get("reset_session").and_then(Value::as_bool) == Some(true) {
        // reset_session 会在加锁后操作；不能在并行请求期间替换锁。
    }
    let id = explicit_id(body)?;
    let user = body.get("user").and_then(Value::as_str).unwrap_or("");
    let mut hasher = Sha256::new();
    for field in [region.provider_id(), account_id, token, model, user, id] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    let key = format!("{:x}", hasher.finalize());
    let now = crate::server::logging::now_ms();
    let lock = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = lock.lock().unwrap_or_else(|p| p.into_inner());
    cache.retain(|_, item| now.saturating_sub(item.touched) <= TTL_MS || Arc::strong_count(&item.state) > 1);
    if let Some(item) = cache.get_mut(&key) {
        item.touched = now;
        return Some(Arc::clone(&item.state));
    }
    if cache.len() >= MAX_SESSIONS {
        if let Some(old) = cache.iter().filter(|(_, item)| Arc::strong_count(&item.state) == 1).min_by_key(|(_, item)| item.touched).map(|(k, _)| k.clone()) {
            cache.remove(&old);
        }
    }
    let state = Arc::new(AsyncMutex::new(Conversation::default()));
    cache.insert(key, Entry {state: Arc::clone(&state), touched: now});
    Some(state)
}

/// 只在完整的先前输入序列仍是本次历史的前缀时增量传输。
/// 缩短/修改历史返回 None，必须重建上游会话，不能对旧会话发全量造成重复上下文。
pub fn delta_start(previous: &[String], next: &[String]) -> Option<usize> {
    if previous.is_empty() || next.len() < previous.len() { return None; }
    if previous.iter().zip(next).all(|(a, b)| a == b) {
        Some(previous.len())
    } else {
        None
    }
}

pub fn fingerprints(messages: Option<&Value>) -> Vec<String> {
    match messages.and_then(Value::as_array) {
        Some(items) => items.iter()
            .map(|msg| format!("{:x}", Sha256::digest(msg.to_string().as_bytes())))
            .collect(),
        None => Vec::new(),
    }
}

/// 仅取前缀之后新增的消息；完全重复的输入只重发最后一条用户消息。
pub fn delta_messages<'a>(messages: &'a [Value], start: usize) -> Vec<Value> {
    if start < messages.len() {
        return messages[start..].to_vec();
    }
    messages.iter().rev().find(|m| m.get("role").and_then(Value::as_str) == Some("user"))
        .cloned().into_iter().collect()
}

#[cfg(test)]
mod parity_tests {
    use super::{delta_start, delta_messages, explicit_id, fingerprints};
    use serde_json::json;

    #[test]
    fn exact_prefix_allows_incremental_and_edits_rebuild() {
        let original=json!([{"role":"user","content":"x"}]);
        let longer=json!([{"role":"user","content":"x"},{"role":"assistant","content":"y"},{"role":"user","content":"z"}]);
        let prev=fingerprints(Some(&original));let next=fingerprints(Some(&longer));
        assert_eq!(delta_start(&prev,&next),Some(1));
        assert_eq!(delta_messages(longer.as_array().unwrap(),1).len(),2);
        assert_eq!(delta_messages(original.as_array().unwrap(),1).len(),1);
        let changed=fingerprints(Some(&json!([{"role":"user","content":"changed"}])));
        assert_eq!(delta_start(&prev,&changed),None);
    }
    #[test]
    fn reuse_requires_explicit_conversation_id() {
        assert!(explicit_id(&json!({"messages":[]})).is_none());
        assert_eq!(explicit_id(&json!({"metadata":{"conversation_id":"unique-123"}})),Some("unique-123"));
    }
}


/// 管理面只读快照：不暴露会话名、token 或上游返回内容。
pub fn stats() -> serde_json::Value {
    let now = crate::server::logging::now_ms();
    let map = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let cache = map.lock().unwrap_or_else(|e| e.into_inner());
    let sessions = cache.iter().map(|(key, item)| {
        let busy = Arc::strong_count(&item.state) > 1 || item.state.try_lock().is_err();
        let (remote, messages) = match item.state.try_lock() {
            Ok(state) => (state.session_id.is_some(), state.sent.len()),
            Err(_) => (true, 0),
        };
        serde_json::json!({
            "key": key.chars().take(12).collect::<String>(),
            "busy": busy,
            "has_upstream_session": remote,
            "messages": messages,
            "idle_ms": now.saturating_sub(item.touched),
        })
    }).collect::<Vec<_>>();
    serde_json::json!({"sessions": sessions, "count": sessions.len()})
}

/// 删除空闲缓存项。执行中的会话不会被替换，避免管理清理破坏并发互斥。
pub fn reset_idle() -> usize {
    let map = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = map.lock().unwrap_or_else(|e| e.into_inner());
    let old = cache.len();
    cache.retain(|_, entry| Arc::strong_count(&entry.state) > 1 || entry.state.try_lock().is_err());
    old.saturating_sub(cache.len())
}
