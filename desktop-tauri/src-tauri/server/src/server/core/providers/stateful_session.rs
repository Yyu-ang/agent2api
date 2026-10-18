//! 有状态 provider（Tabbit / TeleAgent）共用的跨请求会话设施。
//!
//! ── 解决什么 ──────────────────────────────────────────────────
//! 两家都把「显式会话 ID → 上游会话 + 已发送历史指纹」缓存起来，只把新增
//! 消息发给同一个上游会话。这里收口四件两家完全同构的事：
//!   1. **会话身份**：只认显式标识（body 的 `conversation_id` /
//!      `metadata.conversation_id` / `metadata.agent2api_conversation_id`，
//!      或与 CatPaw 同口径的 `x-session-id` 请求头）；没有标识 = 请求隔离。
//!      缓存键另外混入**网关调用方凭证的哈希**（`caller_scope`），不同 API Key
//!      的客户端用了同一个会话 ID 也不会争锁、互相 reset；
//!   2. **排队上限**：同一会话的并发请求最多排队 `LOCK_WAIT_MS`，超时回 409，
//!      并响应 `telemetry.cancel_token()`（手动终止不再傻等）；
//!   3. **失败即作废**：[`Turn`] 持有 owned guard，`Drop` 时只要没 `commit`
//!      就 reset —— 任何 `?` 提前返回、客户端断开、后台任务出错，都不会把
//!      一个可能已失效的上游会话留给下一轮（否则同一会话 ID 会永久卡死）；
//!   4. **增量切片**：严格前缀校验；增量开头的 assistant 消息是上游自己刚生成
//!      的回复，不再回灌；历史完全相同（重试 / 重新生成）不复用，按全量新建。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：零 unwrap/expect/panic；std 锁不跨 await。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use axum::http::HeaderMap;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::server::core::upstream::usage::RequestTelemetry;
use crate::server::errors::GatewayError;

/// 缓存项空闲 TTL
const TTL_MS: i64 = 30 * 60_000;
/// 缓存项上限（只驱逐空闲项）
const MAX_SESSIONS: usize = 256;
/// 同一会话排队等待上限；超时返回 409，让客户端自己决定重试。
pub const LOCK_WAIT_MS: u64 = 30_000;
/// 显式会话 ID 长度上限
const MAX_ID_LEN: usize = 512;

/// 一个显式会话的上游状态。
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

/// 一家 provider 的会话注册表（进程级 static，见各家 `session.rs`）。
pub struct Registry {
    cache: OnceLock<Mutex<HashMap<String, Entry>>>,
}

impl Registry {
    pub const fn new() -> Self {
        Self { cache: OnceLock::new() }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, Entry>> {
        self.cache
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 按隔离字段取（或建）缓存项。`fields` 由调用方决定（账号、凭证、模型…），
    /// 会话 ID 与调用方凭证哈希由这里统一追加。没有显式会话 ID → None。
    pub fn entry(
        &self,
        fields: &[&str],
        body: &Value,
        headers: &HeaderMap,
    ) -> Option<Arc<AsyncMutex<Conversation>>> {
        let id = explicit_id(body, headers)?;
        let user = body.get("user").and_then(Value::as_str).unwrap_or("");
        let caller = caller_scope(headers);
        let mut hasher = Sha256::new();
        for field in fields.iter().copied().chain([user, caller.as_str(), id.as_str()]) {
            hasher.update((field.len() as u64).to_be_bytes());
            hasher.update(field.as_bytes());
        }
        let key = format!("{:x}", hasher.finalize());
        let now = crate::server::logging::now_ms();
        let mut cache = self.map();
        cache.retain(|_, item| {
            now.saturating_sub(item.touched) <= TTL_MS || Arc::strong_count(&item.state) > 1
        });
        if let Some(item) = cache.get_mut(&key) {
            item.touched = now;
            return Some(Arc::clone(&item.state));
        }
        if cache.len() >= MAX_SESSIONS {
            let oldest = cache
                .iter()
                .filter(|(_, item)| Arc::strong_count(&item.state) == 1)
                .min_by_key(|(_, item)| item.touched)
                .map(|(key, _)| key.clone());
            if let Some(old) = oldest {
                cache.remove(&old);
            }
        }
        let state = Arc::new(AsyncMutex::new(Conversation::default()));
        cache.insert(key, Entry { state: Arc::clone(&state), touched: now });
        Some(state)
    }

    /// 管理面只读快照：不暴露会话名、token 或上游返回内容。
    pub fn stats(&self) -> Value {
        let now = crate::server::logging::now_ms();
        let cache = self.map();
        let sessions = cache
            .iter()
            .map(|(key, item)| {
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
            })
            .collect::<Vec<_>>();
        serde_json::json!({"sessions": sessions, "count": sessions.len()})
    }

    /// 删除空闲缓存项。执行中的会话不会被替换，避免管理清理破坏并发互斥。
    pub fn reset_idle(&self) -> usize {
        let mut cache = self.map();
        let old = cache.len();
        cache.retain(|_, entry| Arc::strong_count(&entry.state) > 1 || entry.state.try_lock().is_err());
        old.saturating_sub(cache.len())
    }
}

/// 显式会话 ID：body 字段优先，其次 `x-session-id` 头（CatPaw 口径）。
pub fn explicit_id(body: &Value, headers: &HeaderMap) -> Option<String> {
    let metadata = body.get("metadata");
    body.get("conversation_id")
        .and_then(Value::as_str)
        .or_else(|| metadata.and_then(|m| m.get("conversation_id")).and_then(Value::as_str))
        .or_else(|| metadata.and_then(|m| m.get("agent2api_conversation_id")).and_then(Value::as_str))
        .or_else(|| headers.get("x-session-id").and_then(|value| value.to_str().ok()))
        .map(str::trim)
        .filter(|id| !id.is_empty() && id.len() <= MAX_ID_LEN)
        .map(str::to_string)
}

/// 网关调用方身份：入站凭证头的 SHA-256（只进缓存键哈希，不落盘、不回显）。
/// 免鉴权模式下客户端不带凭证 → 空串，行为与之前相同。
pub fn caller_scope(headers: &HeaderMap) -> String {
    let raw = ["authorization", "x-api-key", "x-goog-api-key"]
        .iter()
        .find_map(|name| headers.get(*name).and_then(|value| value.to_str().ok()))
        .map(str::trim)
        .unwrap_or("");
    if raw.is_empty() {
        return String::new();
    }
    format!("{:x}", Sha256::digest(raw.as_bytes()))
}

/// 一轮对话对会话的独占持有。没有 `commit` 就被 drop → 自动 reset。
pub struct Turn {
    guard: Option<OwnedMutexGuard<Conversation>>,
    committed: bool,
}

impl Turn {
    /// 没有显式会话 ID 时的空持有（所有操作都是空操作）。
    pub fn detached() -> Self {
        Self { guard: None, committed: false }
    }

    pub fn from_guard(guard: OwnedMutexGuard<Conversation>) -> Self {
        Self { guard: Some(guard), committed: false }
    }

    /// 排队取锁：最多 `LOCK_WAIT_MS`，超时 409；手动终止立即返回。
    pub async fn acquire(
        state: Option<Arc<AsyncMutex<Conversation>>>,
        telemetry: &Arc<RequestTelemetry>,
        label: &str,
    ) -> Result<Self, GatewayError> {
        Self::acquire_within(state, telemetry, label, LOCK_WAIT_MS).await
    }

    async fn acquire_within(
        state: Option<Arc<AsyncMutex<Conversation>>>,
        telemetry: &Arc<RequestTelemetry>,
        label: &str,
        wait_ms: u64,
    ) -> Result<Self, GatewayError> {
        let Some(state) = state else { return Ok(Self::detached()) };
        let wait = tokio::time::timeout(
            std::time::Duration::from_millis(wait_ms),
            state.lock_owned(),
        );
        let locked = match telemetry.cancel_token() {
            Some(token) => tokio::select! {
                result = wait => result,
                _ = token.cancelled() => {
                    return Err(crate::server::core::upstream::cancellation::cancelled_error());
                }
            },
            None => wait.await,
        };
        match locked {
            Ok(guard) => Ok(Self::from_guard(guard)),
            Err(_) => Err(GatewayError::with_status(
                409,
                format!("{label} 同一会话已有请求在处理，排队 {} 秒仍未轮到，请稍后重试", wait_ms / 1000),
            )),
        }
    }

    pub fn is_attached(&self) -> bool {
        self.guard.is_some()
    }

    /// 可复用时返回（上游会话 ID，增量起点）。
    pub fn reusable(&self, fingerprints: &[String]) -> Option<(String, usize)> {
        let state = self.guard.as_ref()?;
        let id = state.session_id.as_ref()?;
        let start = delta_start(&state.sent, fingerprints)?;
        Some((id.clone(), start))
    }

    pub fn reset(&mut self) {
        if let Some(state) = self.guard.as_mut() {
            state.reset();
        }
    }

    /// 本轮成功：记下上游会话与已发送历史，并释放锁。
    pub fn commit(mut self, session_id: String, fingerprints: Vec<String>) {
        if let Some(state) = self.guard.as_mut() {
            state.commit(session_id, fingerprints);
        }
        self.committed = true;
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        if !self.committed {
            self.reset();
        }
    }
}

/// 只在完整的先前输入序列仍是本次历史的**真前缀**时增量传输。
/// 缩短 / 修改 / 完全相同（重试、重新生成）都返回 None，必须重建上游会话：
/// 对旧会话重发会让上游看到重复问题或重复上下文。
pub fn delta_start(previous: &[String], next: &[String]) -> Option<usize> {
    if previous.is_empty() || next.len() <= previous.len() {
        return None;
    }
    if previous.iter().zip(next).all(|(a, b)| a == b) {
        Some(previous.len())
    } else {
        None
    }
}

pub fn fingerprints(messages: Option<&Value>) -> Vec<String> {
    match messages.and_then(Value::as_array) {
        Some(items) => items
            .iter()
            .map(|msg| format!("{:x}", Sha256::digest(msg.to_string().as_bytes())))
            .collect(),
        None => Vec::new(),
    }
}

/// 前缀之后的新增消息，跳过开头的 assistant（上游自己的回复，它已经知道）。
/// 结果为空 → None（调用方按全量重建处理）。
pub fn new_messages(messages: &[Value], start: usize) -> Option<Vec<Value>> {
    let tail = messages.get(start..)?;
    let fresh: Vec<Value> = tail
        .iter()
        .skip_while(|m| m.get("role").and_then(Value::as_str) == Some("assistant"))
        .cloned()
        .collect();
    if fresh.is_empty() { None } else { Some(fresh) }
}

/// 可取消的等待：被手动终止时返回 Err（`cancelled_error`）。
pub async fn sleep_or_cancel(telemetry: &Arc<RequestTelemetry>, delay_ms: u64) -> Result<(), GatewayError> {
    let sleep = tokio::time::sleep(std::time::Duration::from_millis(delay_ms));
    tokio::pin!(sleep);
    match telemetry.cancel_token() {
        Some(token) => tokio::select! {
            _ = &mut sleep => Ok(()),
            _ = token.cancelled() => Err(crate::server::core::upstream::cancellation::cancelled_error()),
        },
        None => {
            sleep.await;
            Ok(())
        }
    }
}

/// 粗估 token：CJK 每字约 1 token，其它字符每 4 个约 1 token。
pub fn estimate_tokens(text: &str) -> i64 {
    let mut latin = 0i64;
    let mut cjk = 0i64;
    for c in text.chars() {
        if ('\u{4e00}'..='\u{9fff}').contains(&c) { cjk += 1; } else { latin += 1; }
    }
    cjk + (latin + 3) / 4
}

/// OpenAI 形状的 usage（估算值带 `estimated: true`，统计口径可区分）。
pub fn usage_json(prompt: i64, completion: i64, cache_read: i64, estimated: bool) -> Value {
    let mut usage = serde_json::json!({
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
    });
    if cache_read > 0 {
        usage["prompt_tokens_details"] = serde_json::json!({"cached_tokens": cache_read});
    }
    if estimated {
        usage["estimated"] = Value::Bool(true);
    }
    usage
}

#[cfg(test)]
mod parity_tests {
    use super::*;
    use serde_json::json;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            if let Ok(value) = value.parse() {
                map.insert(*name, value);
            }
        }
        map
    }

    #[test]
    fn identical_history_rebuilds_and_assistant_echo_is_skipped() {
        let a = fingerprints(Some(&json!([{"role":"user","content":"x"}])));
        assert_eq!(delta_start(&a, &a), None, "重试/重新生成不能把同一问题再发一次");
        let msgs = vec![
            json!({"role":"user","content":"x"}),
            json!({"role":"assistant","content":"y"}),
            json!({"role":"user","content":"z"}),
        ];
        let delta = new_messages(&msgs, 1).unwrap_or_default();
        assert_eq!(delta.len(), 1);
        assert_eq!(delta[0]["content"], "z");
        assert!(new_messages(&msgs[..2], 1).is_none());
    }

    #[test]
    fn session_header_and_caller_isolation() {
        let body = json!({"messages": []});
        assert_eq!(explicit_id(&body, &headers(&[("x-session-id", " s-1 ")])).as_deref(), Some("s-1"));
        assert!(explicit_id(&body, &HeaderMap::new()).is_none());
        let registry = Registry::new();
        let h1 = headers(&[("x-session-id", "same"), ("authorization", "Bearer key-a")]);
        let h2 = headers(&[("x-session-id", "same"), ("authorization", "Bearer key-b")]);
        let a = registry.entry(&["acct"], &body, &h1);
        let b = registry.entry(&["acct"], &body, &h2);
        let a2 = registry.entry(&["acct"], &body, &h1);
        match (a, b, a2) {
            (Some(a), Some(b), Some(a2)) => {
                assert!(!Arc::ptr_eq(&a, &b), "不同调用方凭证不能共享会话");
                assert!(Arc::ptr_eq(&a, &a2));
            }
            _ => panic!("expected entries"),
        }
        assert!(!caller_scope(&h1).contains("key-a"));
    }

    /// B1 回归：一轮失败（未 commit 就 drop，等价于任意 `?` 提前返回或后台
    /// 任务出错）后，会话必须作废，下一轮不能再走增量打到失效的上游会话。
    #[tokio::test]
    async fn failed_turn_resets_session_so_next_turn_rebuilds() {
        let registry = Registry::new();
        let body = json!({"conversation_id": "conv-b1"});
        let telemetry = Arc::new(RequestTelemetry::new());
        let first = fingerprints(Some(&json!([{"role":"user","content":"a"}])));
        let second = fingerprints(Some(&json!([
            {"role":"user","content":"a"},{"role":"assistant","content":"b"},{"role":"user","content":"c"}
        ])));
        let turn = Turn::acquire(registry.entry(&["x"], &body, &HeaderMap::new()), &telemetry, "t").await;
        let Ok(turn) = turn else { panic!("lock") };
        turn.commit("upstream-1".to_string(), first.clone());
        let turn = Turn::acquire(registry.entry(&["x"], &body, &HeaderMap::new()), &telemetry, "t").await;
        let Ok(turn) = turn else { panic!("lock") };
        assert_eq!(turn.reusable(&second), Some(("upstream-1".to_string(), 1)));
        drop(turn); // 上游报错：没有 commit
        let turn = Turn::acquire(registry.entry(&["x"], &body, &HeaderMap::new()), &telemetry, "t").await;
        let Ok(turn) = turn else { panic!("lock") };
        assert!(turn.reusable(&second).is_none(), "失败后必须新建会话 + 全量");
    }

    #[tokio::test]
    async fn queued_request_times_out_with_409() {
        let registry = Registry::new();
        let body = json!({"conversation_id": "conv-lock"});
        let telemetry = Arc::new(RequestTelemetry::new());
        let held = Turn::acquire(registry.entry(&["x"], &body, &HeaderMap::new()), &telemetry, "t").await;
        assert!(held.is_ok());
        let waiting = Turn::acquire_within(registry.entry(&["x"], &body, &HeaderMap::new()), &telemetry, "t", 50).await;
        match waiting {
            Err(error) => assert_eq!(error.status_code, 409),
            Ok(_) => panic!("second request must not get the lock"),
        }
    }
}
