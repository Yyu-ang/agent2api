//! TeleAgent 人工确认队列：只有明确的管理面回复才能批准权限。
//! 不持久化 session key；绝不在管理 API 响应中回传密钥。
//!
//! ── 默认关闭（与旧版行为一致）────────────────────────────────
//! 旧版遇到 permission/question 立即报 502；不知道管理页存在的客户端若改成
//! 静默等待，会先被自己或反代的空闲超时切断。所以人工确认要显式开启，见
//! [`manual_enabled`]。开启后等待期间流上定时下发 SSE 注释保活（`stream.rs`）。
//!
//! ── 生命周期（不残留）──────────────────────────────────────────
//! 每轮只认**本轮登记**的 id（`has_pending_ids`）；上游自己发出的
//! `permission.replied` / `question.replied` / `question.rejected` 会把对应项删掉
//! （`resolve`）；流结束（成功、出错、客户端断开）时清掉本会话的全部待确认项
//! （`clear_session`）—— 否则残留项会吞掉复用会话下一轮的 Done。
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use serde_json::{Value, json};
use crate::server::errors::GatewayError;

const TTL_MS: i64 = 10 * 60_000;
const MAX_PENDING: usize = 256;

#[derive(Clone)]
struct Pending {
    id: String,
    kind: String,
    session_id: String,
    key: String,
    description: String,
    tool: String,
    questions: Value,
    at: i64,
    in_flight: bool,
}

static STATE: OnceLock<Mutex<HashMap<String, Pending>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, Pending>> {
    STATE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn guard() -> std::sync::MutexGuard<'static, HashMap<String, Pending>> {
    registry().lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn prune(pending: &mut HashMap<String, Pending>, now: i64) {
    pending.retain(|_, p| now.saturating_sub(p.at) < TTL_MS);
}

fn slot(session_id: &str, id: &str) -> String {
    format!("{session_id}:{id}")
}

/// 人工确认是否开启：请求 `metadata.teleagent_manual_confirmation` 优先，
/// 其次设置项 `teleagentManualConfirmation`，最后环境变量
/// `AGENT2API_TELEAGENT_MANUAL_CONFIRM`（1/true/on/yes）。默认关闭。
pub fn manual_enabled(body: &Value) -> bool {
    if let Some(flag) = body
        .get("metadata")
        .and_then(|m| m.get("teleagent_manual_confirmation"))
        .and_then(Value::as_bool)
    {
        return flag;
    }
    let config = crate::server::config::current();
    if let Some(flag) = config.raw().get("teleagentManualConfirmation").and_then(Value::as_bool) {
        return flag;
    }
    std::env::var("AGENT2API_TELEAGENT_MANUAL_CONFIRM")
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"))
        .unwrap_or(false)
}

/// 上游确认事件里的请求 id（不同版本字段名不同）。
pub fn event_id(props: &Value) -> &str {
    props.get("id")
        .or_else(|| props.get("requestID"))
        .or_else(|| props.get("permissionID"))
        .or_else(|| props.get("permissionId"))
        .or_else(|| props.get("questionID"))
        .and_then(Value::as_str)
        .unwrap_or("")
}

/// 登记一条待确认项，返回它的 id（调用方记为「本轮登记」）。
pub fn record(kind: &str, session_id: &str, props: &Value, key: &str) -> Result<String, GatewayError> {
    if !matches!(kind, "permission" | "question") {
        return Err(GatewayError::with_status(400, "TeleAgent 不支持的确认事件"));
    }
    let id = event_id(props);
    if id.is_empty() || session_id.is_empty() {
        return Err(GatewayError::with_status(502, "TeleAgent 确认事件缺少 ID 或 sessionID"));
    }
    let tool = props.get("tool").or_else(|| props.get("toolID")).cloned().unwrap_or(Value::Null);
    let tool_label = tool.get("name").or_else(|| tool.get("callID"))
        .and_then(Value::as_str).or_else(|| tool.as_str()).unwrap_or("").to_string();
    let questions = tool.get("input").and_then(|v| v.get("questions"))
        .cloned().unwrap_or_else(|| json!([]));
    let desc = props.get("description").or_else(|| props.get("prompt"))
        .and_then(Value::as_str).unwrap_or("");
    let description = if desc.is_empty() {
        if kind == "question" {
            questions.as_array().map(|items| {
                items.iter()
                    .filter_map(|v| v.get("question").or_else(|| v.get("header")).and_then(Value::as_str))
                    .collect::<Vec<_>>().join("；")
            }).unwrap_or_default()
        } else {
            let p = props.get("permission").and_then(Value::as_str).unwrap_or("操作");
            format!("权限请求：{p}")
        }
    } else { desc.to_string() };
    let now = crate::server::logging::now_ms();
    let mut pending = guard();
    prune(&mut pending, now);
    let entry = slot(session_id, id);
    if let Some(existing) = pending.get_mut(&entry) {
        if existing.description.is_empty() { existing.description = description; }
        return Ok(id.to_string());
    }
    if pending.len() >= MAX_PENDING {
        return Err(GatewayError::with_status(503, "TeleAgent 待确认项数量已达上限"));
    }
    pending.insert(entry, Pending {
        id: id.to_string(), kind: kind.to_string(), session_id: session_id.to_string(),
        key: key.to_string(), description, tool: tool_label, questions, at: now,
        in_flight: false,
    });
    Ok(id.to_string())
}

pub fn list() -> Value {
    let mut pending = guard();
    prune(&mut pending, crate::server::logging::now_ms());
    let mut items: Vec<Value> = pending.values().map(|p| json!({
        "id": p.id, "type": p.kind, "session_id": p.session_id,
        "description": p.description, "tool": p.tool, "questions": p.questions,
        "created_at": p.at, "in_flight": p.in_flight
    })).collect();
    items.sort_by_key(|v| v.get("created_at").and_then(Value::as_i64).unwrap_or(0));
    json!({"pending": items})
}

pub async fn reply(session_id: &str, id: &str, payload: &Value) -> Result<(), GatewayError> {
    let slot = slot(session_id, id);
    let (kind, key, body) = {
        let mut pending = guard();
        prune(&mut pending, crate::server::logging::now_ms());
        let item = pending.get_mut(&slot).ok_or_else(|| GatewayError::with_status(404, "待确认事件不存在或已超时"))?;
        if item.in_flight {
            return Err(GatewayError::with_status(409, "此确认正在处理"));
        }
        let body = if item.kind == "permission" {
            let choice = payload.get("reply").and_then(Value::as_str).unwrap_or("");
            if !matches!(choice, "once" | "always" | "reject") {
                return Err(GatewayError::with_status(400, "permission 回复必须为 once、always、reject"));
            }
            json!({"reply": choice})
        } else {
            let answers = payload.get("answers").and_then(Value::as_array);
            if !answers.is_some_and(|items| !items.is_empty() && items.iter().all(Value::is_string)) {
                return Err(GatewayError::with_status(400, "question 回复需要非空字符串 answers 数组"));
            }
            json!({"answers": answers})
        };
        item.in_flight = true;
        (item.kind.clone(), item.key.clone(), body)
    };
    let outcome = super::protocol::reply_confirmation_with_key(&kind, id, &body, &key).await;
    let mut pending = guard();
    if outcome.is_ok() {
        pending.remove(&slot);
    } else if let Some(item) = pending.get_mut(&slot) {
        item.in_flight = false;
    }
    outcome
}

#[cfg(test)]
mod parity_tests {
    use super::{clear_session, has_pending_ids, list, record, resolve};
    use serde_json::json;

    /// B2 回归：上一轮残留的待确认项不能吞掉本轮的 Done；流结束清理本会话。
    #[test]
    fn leftover_confirmation_does_not_block_new_turn() {
        let props = json!({"id":"stale-1","sessionID":"sess-b2"});
        let id = record("permission", "sess-b2", &props, "k").unwrap_or_default();
        assert_eq!(id, "stale-1");
        // 新一轮没有登记任何 id：残留项不影响它结束
        assert!(!has_pending_ids("sess-b2", &[]));
        assert!(has_pending_ids("sess-b2", &[id.clone()]));
        // 上游自行处理（permission.replied）后本轮也能结束
        resolve("sess-b2", &id);
        assert!(!has_pending_ids("sess-b2", &[id.clone()]));
        // 流结束兜底清理
        let other = record("question", "sess-b2", &json!({"requestID":"q-2"}), "k").unwrap_or_default();
        assert_eq!(other, "q-2");
        assert_eq!(clear_session("sess-b2"), 1);
        assert!(!list().to_string().contains("sess-b2"));
    }
    #[test]
    fn register_and_dedupe_without_exposing_key() {
        let props = json!({"id":"confirm-abc","tool":{"input":{"questions":[{"question":"Proceed?"}]}}});
        assert!(record("question", "session-abc", &props, "secret-not-for-ui").is_ok());
        assert!(record("question", "session-abc", &props, "secret-not-for-ui").is_ok());
        let data = list();
        let serialized = data.to_string();
        assert!(serialized.contains("Proceed?"));
        assert!(!serialized.contains("secret-not-for-ui"));
    }
}


/// 本轮登记的 id 里是否还有未处理的（不看历史残留项）。
pub fn has_pending_ids(session_id: &str, ids: &[String]) -> bool {
    if ids.is_empty() {
        return false;
    }
    let mut pending = guard();
    prune(&mut pending, crate::server::logging::now_ms());
    ids.iter().any(|id| pending.contains_key(&slot(session_id, id)))
}

/// 上游已处理（`*.replied` / `question.rejected`，或在 TeleAgent 原生界面批准）。
pub fn resolve(session_id: &str, id: &str) {
    if id.is_empty() {
        return;
    }
    guard().remove(&slot(session_id, id));
}

/// 流结束时清掉本会话的全部待确认项（同一会话同一时刻只有一个请求持锁）。
pub fn clear_session(session_id: &str) -> usize {
    let mut pending = guard();
    let before = pending.len();
    pending.retain(|_, entry| entry.session_id != session_id);
    before.saturating_sub(pending.len())
}
