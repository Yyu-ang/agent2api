//! TeleAgent 跨请求会话。只有显式 conversation_id 才复用；按账号、密钥、模型和工作目录隔离。
//! Tokio owned guard 覆盖整个上游 SSE 生命周期，避免一个会话上并行 prompt。
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use sha2::{Digest, Sha256};
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

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

struct Entry { state: Arc<AsyncMutex<Conversation>>, touched: i64 }
static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();

pub fn entry(account_id: &str, key: &str, model: &str, directory: &str, body: &Value)
    -> Option<Arc<AsyncMutex<Conversation>>>
{
    let id = body.get("conversation_id").and_then(Value::as_str)
        .or_else(|| body.get("metadata").and_then(|m| m.get("conversation_id")).and_then(Value::as_str))
        .or_else(|| body.get("metadata").and_then(|m| m.get("agent2api_conversation_id")).and_then(Value::as_str))
        .map(str::trim).filter(|id| !id.is_empty() && id.len() <= 512)?;
    let user = body.get("user").and_then(Value::as_str).unwrap_or("");
    let mut hasher = Sha256::new();
    for item in [account_id, key, model, directory, user, id] {
        hasher.update((item.len() as u64).to_be_bytes());
        hasher.update(item.as_bytes());
    }
    let slot = format!("{:x}", hasher.finalize());
    let now = crate::server::logging::now_ms();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = cache.lock().unwrap_or_else(|e| e.into_inner());
    map.retain(|_, v| now.saturating_sub(v.touched) < TTL_MS || Arc::strong_count(&v.state) > 1);
    if let Some(entry) = map.get_mut(&slot) {
        entry.touched = now;
        return Some(Arc::clone(&entry.state));
    }
    if map.len() >= MAX_SESSIONS {
        if let Some(old) = map.iter().filter(|(_, v)| Arc::strong_count(&v.state) == 1).min_by_key(|(_, v)| v.touched).map(|(id, _)| id.clone()) {
            map.remove(&old);
        }
    }
    let state = Arc::new(AsyncMutex::new(Conversation::default()));
    map.insert(slot, Entry { state: Arc::clone(&state), touched: now });
    Some(state)
}

pub fn fingerprints(messages: Option<&Value>) -> Vec<String> {
    match messages.and_then(Value::as_array) {
        Some(msgs) => msgs.iter().map(|m| format!("{:x}", Sha256::digest(m.to_string().as_bytes()))).collect(),
        None => Vec::new(),
    }
}

/// 严格前缀校验，变更历史后不向旧会话注入新的完整历史。
pub fn delta_start(previous: &[String], current: &[String]) -> Option<usize> {
    if previous.is_empty() || current.len() < previous.len() { return None; }
    if previous.iter().zip(current).all(|(a, b)| a == b) { Some(previous.len()) }
    else { None }
}

pub fn incremental_messages(body: &Value, start: usize) -> Option<Value> {
    let messages = body.get("messages")?.as_array()?;
    let mut sliced = messages.iter().filter(|m| m.get("role").and_then(Value::as_str) == Some("system")).cloned().collect::<Vec<_>>();
    if start < messages.len() {
        sliced.extend(messages[start..].iter().filter(|m| m.get("role").and_then(Value::as_str) != Some("system")).cloned());
    } else if let Some(last) = messages.iter().rev().find(|m| m.get("role").and_then(Value::as_str) == Some("user")) {
        sliced.push(last.clone());
    }
    Some(Value::Array(sliced))
}

#[cfg(test)]
mod parity_tests {
    use super::{delta_start, fingerprints, incremental_messages};
    use serde_json::json;
    #[test]
    fn reconstructs_system_plus_increment() {
        let prev=json!([{"role":"system","content":"rules"},{"role":"user","content":"hello"}]);
        let next=json!([{"role":"system","content":"rules"},{"role":"user","content":"hello"},{"role":"assistant","content":"ok"},{"role":"user","content":"next"}]);
        let a=fingerprints(Some(&prev));let b=fingerprints(Some(&next));
        assert_eq!(delta_start(&a,&b),Some(2));
        let delta=incremental_messages(&json!({"messages":next}),2).unwrap();
        assert_eq!(delta.as_array().unwrap().len(),3);
        let changed = json!([{"role":"user","content":"modified"}]);
        let changed_fingerprints = fingerprints(Some(&changed));
        assert!(delta_start(&a, &changed_fingerprints).is_none());
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
