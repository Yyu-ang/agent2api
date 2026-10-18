//! Tabbit 出站请求的薄封装：JSON 请求、纯文本请求与 SSE 流式请求。
//!
//! ── 为什么收在一处 ──────────────────────────────────────────
//! 对话链路（sign-key / 建会话 / chat completion / 模型目录 / 额度）共用同一套
//! 头集合、代理与错误翻译；各写一份会让「哪个状态码算登录失效」这类判据分叉。
//!
//! ── 错误翻译口径 ────────────────────────────────────────────
//!   - 连接 / 超时 → 502 / 504（提示语带「上游」字样，与各家同款）；
//!   - HTTP 401 / 403 → 401（登录态失效；本家**没有刷新接口**，
//!     `supports_refresh = false`，用户重新登录/重新添加即可 ——
//!     与 KukuAI / CatPaw 同一处境）；
//!   - 499（签名 key 过期）→ 由调用方刷新 key 重试，**不**在这里翻译成错误；
//!   - 其余非 2xx → 原状态码 + 响应体前 240 字符（排障用）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use serde_json::Value;

use crate::server::core::egress;
use crate::server::core::proxies::ResolvedProxy;
use crate::server::errors::GatewayError;

/// 纯文本响应的读取结果（sign-key 接口返回纯文本，不是 JSON）。
pub struct RawText {
    pub status: u16,
    pub text: String,
}

/// 把传输错误翻译成网关错误（超时 → 504，其余 → 502）。
fn transport_error(what: &str, error: reqwest::Error) -> GatewayError {
    if error.is_timeout() {
        GatewayError::with_status(504, format!("{what}超时，请稍后重试"))
    } else {
        GatewayError::with_status(
            502,
            format!("{what}失败：{}", egress::describe_error_detail(&error)),
        )
    }
}

/// 一次 GET，返回原始文本（sign-key 这类纯文本接口用）。
pub async fn get_raw(
    url: &str,
    headers: &[(String, String)],
    proxy: Option<&ResolvedProxy>,
    timeout_ms: Option<u64>,
) -> Result<RawText, GatewayError> {
    let client = egress::client_for(proxy);
    let mut request = client.get(url);
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some(timeout) = timeout_ms {
        request = request.timeout(std::time::Duration::from_millis(timeout));
    }
    let response = request
        .send()
        .await
        .map_err(|error| transport_error("Tabbit 请求", error))?;
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    Ok(RawText { status, text })
}

/// 一次 GET，返回已解析的 JSON（HTTP 非 2xx 翻译成网关错误）。
pub async fn get_json_value(
    url: &str,
    headers: &[(String, String)],
    proxy: Option<&ResolvedProxy>,
    timeout_ms: Option<u64>,
) -> Result<Value, GatewayError> {
    let client = egress::client_for(proxy);
    let mut request = client.get(url).header("Accept", "application/json");
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some(timeout) = timeout_ms {
        request = request.timeout(std::time::Duration::from_millis(timeout));
    }
    let response = request
        .send()
        .await
        .map_err(|error| transport_error("Tabbit 请求", error))?;
    http_json(response, url, "Tabbit").await
}

/// 一次 JSON POST，返回已解析的 JSON。
pub async fn post_json_value(
    url: &str,
    headers: &[(String, String)],
    body: &Value,
    proxy: Option<&ResolvedProxy>,
    timeout_ms: Option<u64>,
) -> Result<Value, GatewayError> {
    let client = egress::client_for(proxy);
    let mut request = client.post(url).header("Accept", "application/json");
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some(timeout) = timeout_ms {
        request = request.timeout(std::time::Duration::from_millis(timeout));
    }
    let response = request
        .json(body)
        .send()
        .await
        .map_err(|error| transport_error("Tabbit 请求", error))?;
    http_json(response, url, "Tabbit").await
}

/// Tabbit 建会话请求必须是空 body；JSON "{}" 与零字节并不等价。
pub async fn post_empty_value(
    url: &str,
    headers: &[(String, String)],
    proxy: Option<&ResolvedProxy>,
    timeout_ms: Option<u64>,
) -> Result<Value, GatewayError> {
    let client = egress::client_for(proxy);
    let mut request = client.post(url).header("Accept", "application/json");
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    if let Some(timeout) = timeout_ms {
        request = request.timeout(std::time::Duration::from_millis(timeout));
    }
    let response = request.body(Vec::<u8>::new()).send().await
        .map_err(|error| transport_error("Tabbit 建会话", error))?;
    http_json(response, url, "Tabbit").await
}

/// 一次 SSE POST（`stream_mode: "sse"`），返回原始响应（调用方逐帧解析）。
///
/// 499（签名 key 过期）**不**在这里翻译：调用方刷新 key 后重试一次
/// （参考实现同逻辑）。
pub async fn sse_post(
    url: &str,
    headers: &[(String, String)],
    raw_body: &str,
    proxy: Option<&ResolvedProxy>,
    what: &str,
) -> Result<reqwest::Response, GatewayError> {
    let client = egress::client_for(proxy);
    let mut request = client.post(url).header("Accept", "text/event-stream");
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request
        .header("Content-Type", "application/json")
        .body(raw_body.to_string())
        .send()
        .await
        .map_err(|error| transport_error(what, error))?;
    let status = response.status().as_u16();
    if status == 499 {
        // 签名 key 过期：调用方刷新后重试（不是登录失效，别报 401）
        return Err(GatewayError::with_status(499, "Tabbit 签名 key 过期（499）"));
    }
    if !(200..300).contains(&status) {
        let text = response.text().await.unwrap_or_default();
        let head: String = text.chars().take(240).collect();
        return Err(super::protocol::gateway_error(what, status, &head));
    }
    Ok(response)
}

/// 把响应翻译成网关错误或返回 JSON payload。
async fn http_json(
    response: reqwest::Response,
    url: &str,
    what: &str,
) -> Result<Value, GatewayError> {
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    if !(200..300).contains(&status) {
        let head: String = text.chars().take(240).collect();
        return Err(super::protocol::gateway_error(what, status, &head));
    }
    serde_json::from_str::<Value>(&text).map_err(|_| {
        GatewayError::with_status(502, format!("{what}返回非 JSON 响应（{url}）"))
    })
}
