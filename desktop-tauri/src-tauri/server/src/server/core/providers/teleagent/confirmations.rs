//! TeleAgent 人工确认队列：只有明确的管理面回复才能批准权限。
//! 不持久化 session key；绝不在管理 API 响应中回传密钥。
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

pub fn record(kind: &str, session_id: &str, props: &Value, key: &str) -> Result<(), GatewayError> {
    if !matches!(kind, "permission" | "question") {
        return Err(GatewayError::with_status(400, "TeleAgent 不支持的确认事件"));
    }
    let id = props.get("id")
        .or_else(|| props.get("permissionID"))
        .or_else(|| props.get("permissionId"))
        .or_else(|| props.get("questionID"))
        .and_then(Value::as_str).unwrap_or("");
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
        return Ok(());
    }
    if pending.len() >= MAX_PENDING {
        return Err(GatewayError::with_status(503, "TeleAgent 待确认项数量已达上限"));
    }
    pending.insert(entry, Pending {
        id: id.to_string(), kind: kind.to_string(), session_id: session_id.to_string(),
        key: key.to_string(), description, tool: tool_label, questions, at: now,
        in_flight: false,
    });
    Ok(())
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
mod tests {
    use super::{record, list};
    use serde_json::json;
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
