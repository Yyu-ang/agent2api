//! 会话式 Provider 管理接口：查询状态及删除空闲缓存。只挂受保护的面板路由。
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;
use serde_json::{Value, json};

use crate::server::core::providers::{tabbit, teleagent};
use crate::server::errors;
use crate::server::http::{ok_json, parse_body};
use crate::server::ServerState;

/// GET /api/provider-conversations
pub async fn list(State(_state): State<ServerState>) -> Response {
    ok_json(json!({
        "tabbit": tabbit::session::stats(),
        "teleagent": teleagent::session::stats(),
    }))
}

/// POST /api/provider-conversations/reset {"provider":"tabbit"|"teleagent"|"all"}
/// 在途任务的缓存与锁保留；只有空闲项被清理。
pub async fn reset(State(_state): State<ServerState>, headers: HeaderMap, body: Bytes) -> Response {
    // 与确认接口同口径：免鉴权模式下拒绝跨源浏览器请求。
    if super::teleagent_confirmations::is_cross_origin(&headers) {
        return errors::management_error(403, "会话管理接口只接受同源请求");
    }
    let payload = match parse_body(&body) {
        Ok(p) => p,
        Err(e) => return errors::management_error(400, e.message),
    };
    let provider = payload.get("provider").and_then(Value::as_str).unwrap_or("");
    match provider {
        "tabbit" => ok_json(json!({"cleared": tabbit::session::reset_idle()})),
        "teleagent" => ok_json(json!({"cleared": teleagent::session::reset_idle()})),
        "all" => {
            let tabbit = tabbit::session::reset_idle();
            let teleagent = teleagent::session::reset_idle();
            ok_json(json!({"cleared": tabbit + teleagent, "tabbit": tabbit, "teleagent": teleagent}))
        }
        _ => errors::management_error(400, "provider 只能是 tabbit、teleagent 或 all"),
    }
}
