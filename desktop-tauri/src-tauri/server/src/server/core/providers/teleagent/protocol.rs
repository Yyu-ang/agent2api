//! TeleAgent 会话协议：签名请求 / 建会话 / 异步发消息 / prompt 构造。
//!
//! ── 移植自 ────────────────────────────────────────────────────
//! `teleagent-openai-proxy/openai_proxy.py`（MIT，原作者 lizhun781021）：
//!   - `sa_request()` → [`signed_request`]（401/403 时强制刷新 key 重试一次
//!     的语义保留）；
//!   - `create_session()` → [`create_session`]（`POST /session`
//!     `{directory, title?}` → `id`）；
//!   - `send_prompt_async()` → [`prompt_async`] + [`build_prompt`]
//!     （`POST /session/{id}/prompt_async`，204/200 算成功；OpenAI messages
//!     压成单条 prompt 文本：system 拼接在前、assistant 前缀 `[assistant]`、
//!     tool 结果前缀 `[tool_result]`，与原实现逐字同口径）。
//!
//! 不搬的：`get_or_create_session` 的标题复用（机器人场景的；网关每次请求
//! 独立会话，见 `chat.rs`）、`reply_permission` / `reply_question`（网关无法
//! 代用户做确认，见 `stream.rs` 的处置）、`get_messages`（排障用，不需要）。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic；持锁不跨 await。

use serde_json::{Value, json};

use crate::server::core::egress;
use crate::server::errors::GatewayError;

use super::auth::{base_http_url, session_key, sign_headers, sign_headers_with_key};

/// 管理接口的默认超时（原实现 `sa_request` 默认 30 秒）
const REQUEST_TIMEOUT_MS: u64 = 30_000;
/// `prompt_async` 的超时（原实现 10 秒：只是投递，不等生成）
const PROMPT_TIMEOUT_MS: u64 = 10_000;

/// 一次带签名的 super-agent 请求（原实现 `sa_request`）。
///
/// 成功返回 `(status, body_text)`；HTTP 401/403 时强制刷新 key 重试一次
/// （原实现语义）；连接/超时等传输错误按各家同款口径转 502/504。
/// `proxy` 恒为 `None`：super-agent 只监听本机回环（见 `mod.rs`）。
async fn signed_request(
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout_ms: u64,
) -> Result<(u16, String), GatewayError> {
    // 最多两次：首次 + 401/403 时强制刷新 key 重试一次
    //（原实现 `sa_request` 的 `_retry` 语义；写成循环而不是递归，
    // 避免 async 递归的装箱要求）
    for attempt in 0..2 {
        let headers = sign_headers(method, path)?;
        let url = format!("{}{path}", base_http_url());
        // 直连本机回环：proxy 恒为 None（见 `mod.rs`「出网」）
        let client = egress::client_for(None);
        let mut builder = match method {
            "POST" => client.post(&url),
            _ => client.get(&url),
        };
        for (name, value) in &headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if let Some(payload) = body {
            builder = builder
                .header("Content-Type", "application/json")
                .body(payload.to_string());
        }
        let response = builder
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    GatewayError::with_status(504, format!("TeleAgent 请求超时（{path}），请稍后重试"))
                } else {
                    GatewayError::with_status(
                        502,
                        format!("TeleAgent 请求失败（{path}）：{}", egress::describe_error_detail(&error)),
                    )
                }
            })?;
        let status = response.status().as_u16();
        let text = response.text().await.unwrap_or_default();
        if (status == 401 || status == 403) && attempt == 0 {
            // 认证失败：强制刷新 key 重试一次
            session_key(true);
            crate::server::logging::verbose(
                "[TeleAgent]",
                &format!("super-agent 认证失败（{status}），已刷新 key 重试"),
            );
            continue;
        }
        if status == 401 || status == 403 {
            return Err(GatewayError::with_status(
                401,
                "TeleAgent 认证失败（super-agent 拒绝了签名），请确认 TeleAgent 桌面应用正在运行且 session key 有效",
            ));
        }
        return Ok((status, text));
    }
    // 循环恒返回：这里不可达（防御性兜底，满足类型检查）
    Err(GatewayError::with_status(502, "TeleAgent 请求失败（重试逻辑异常）"))
}

/// 一次带签名的 JSON 请求，返回已解析的 body（2xx 才算成功）。
pub async fn signed_json(
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout_ms: u64,
) -> Result<Value, GatewayError> {
    let (status, text) = signed_request(method, path, body, timeout_ms).await?;
    if !(200..300).contains(&status) {
        let head: String = text.chars().take(240).collect();
        return Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 返回 HTTP {status}（{path}）：{head}"),
        ));
    }
    serde_json::from_str(&text).map_err(|_| {
        GatewayError::with_status(502, format!("TeleAgent 返回非 JSON 响应（{path}）"))
    })
}

/// 建会话（原实现 `create_session`）：`POST /session` → 会话 id。
///
/// `directory` 为空时不带该字段（与原实现 `if directory` 同口径）；
/// title 不传 —— 网关每次请求独立会话（见模块头）。
pub async fn create_session(directory: &str) -> Result<String, GatewayError> {
    let mut body = json!({});
    if !directory.is_empty() {
        body["directory"] = json!(directory);
    }
    let value = signed_json("POST", "/session", Some(&body), REQUEST_TIMEOUT_MS).await?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.get("data").and_then(|data| data.get("id")).and_then(Value::as_str))
        .unwrap_or("")
        .to_string();
    if id.is_empty() {
        return Err(GatewayError::with_status(
            502,
            "TeleAgent 建会话返回缺少 id（上游响应异常）",
        ));
    }
    Ok(id)
}

/// 异步发消息（原实现 `send_prompt_async`）：204/200 算成功。
///
/// `provider_id` / `model_id` 进 `body["model"] = {providerID, modelID}`
/// （与原实现同构）；多模态图片走原实现的临时文件口径（见 `build_prompt`）。
pub async fn prompt_async(
    session_id: &str,
    prompt_text: &str,
    directory: &str,
    provider_id: &str,
    model_id: &str,
) -> Result<(), GatewayError> {
    let mut body = json!({
        "sessionID": session_id,
        "parts": [{"type": "text", "text": prompt_text}],
    });
    if !directory.is_empty() {
        body["directory"] = json!(directory);
    }
    if !provider_id.is_empty() && !model_id.is_empty() {
        body["model"] = json!({"providerID": provider_id, "modelID": model_id});
    }
    let path = format!("/session/{session_id}/prompt_async");
    let (status, _) = signed_request("POST", &path, Some(&body), PROMPT_TIMEOUT_MS).await?;
    if status == 200 || status == 204 {
        Ok(())
    } else {
        Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 发送消息失败（HTTP {status}，会话 {session_id}）"),
        ))
    }
}

/// 一次带签名的请求（显式 key 版）：单次尝试，不重试。
///
/// 与 [`signed_request`] 的区别：key 由调用方给（账号里存的那一把），
/// 不读 key 缓存；401/403 不触发「强制刷新 key 重试」—— 存下来的 key
/// 没有可刷新的来源，重试没有意义，调用方直接按认证失败处理。
async fn signed_request_with_key(
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout_ms: u64,
    key: &str,
) -> Result<(u16, String), GatewayError> {
    let headers = sign_headers_with_key(method, path, key)?;
    let url = format!("{}{path}", base_http_url());
    // 直连本机回环：proxy 恒为 None（见 `mod.rs`「出网」）
    let client = egress::client_for(None);
    let mut builder = match method {
        "POST" => client.post(&url),
        _ => client.get(&url),
    };
    for (name, value) in &headers {
        builder = builder.header(name.as_str(), value.as_str());
    }
    if let Some(payload) = body {
        builder = builder
            .header("Content-Type", "application/json")
            .body(payload.to_string());
    }
    let response = builder
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .send()
        .await
        .map_err(|error| {
            if error.is_timeout() {
                GatewayError::with_status(504, format!("TeleAgent 请求超时（{path}），请稍后重试"))
            } else {
                GatewayError::with_status(
                    502,
                    format!("TeleAgent 请求失败（{path}）：{}", egress::describe_error_detail(&error)),
                )
            }
        })?;
    let status = response.status().as_u16();
    let text = response.text().await.unwrap_or_default();
    Ok((status, text))
}

/// 一次带签名的 JSON 请求（显式 key 版），返回已解析的 body（2xx 才算成功）。
pub(crate) async fn signed_json_with_key(
    method: &str,
    path: &str,
    body: Option<&Value>,
    timeout_ms: u64,
    key: &str,
) -> Result<Value, GatewayError> {
    let (status, text) = signed_request_with_key(method, path, body, timeout_ms, key).await?;
    if !(200..300).contains(&status) {
        let head: String = text.chars().take(240).collect();
        return Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 返回 HTTP {status}（{path}）：{head}"),
        ));
    }
    serde_json::from_str(&text).map_err(|_| {
        GatewayError::with_status(502, format!("TeleAgent 返回非 JSON 响应（{path}）"))
    })
}

/// 建会话（显式 key 版）：`POST /session` → 会话 id。
pub async fn create_session_with_key(directory: &str, key: &str) -> Result<String, GatewayError> {
    let mut body = json!({});
    if !directory.is_empty() {
        body["directory"] = json!(directory);
    }
    let value = signed_json_with_key("POST", "/session", Some(&body), REQUEST_TIMEOUT_MS, key).await?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.get("data").and_then(|data| data.get("id")).and_then(Value::as_str))
        .unwrap_or("")
        .to_string();
    if id.is_empty() {
        return Err(GatewayError::with_status(
            502,
            "TeleAgent 建会话返回缺少 id（上游响应异常）",
        ));
    }
    Ok(id)
}

/// 异步发消息（显式 key 版）：204/200 算成功。
pub async fn prompt_async_with_key(
    session_id: &str,
    prompt_text: &str,
    directory: &str,
    provider_id: &str,
    model_id: &str,
    key: &str,
) -> Result<(), GatewayError> {
    let mut body = json!({
        "sessionID": session_id,
        "parts": [{"type": "text", "text": prompt_text}],
    });
    if !directory.is_empty() {
        body["directory"] = json!(directory);
    }
    if !provider_id.is_empty() && !model_id.is_empty() {
        body["model"] = json!({"providerID": provider_id, "modelID": model_id});
    }
    let path = format!("/session/{session_id}/prompt_async");
    let (status, _) = signed_request_with_key("POST", &path, Some(&body), PROMPT_TIMEOUT_MS, key).await?;
    if status == 200 || status == 204 {
        Ok(())
    } else if status == 401 || status == 403 {
        Err(GatewayError::with_status(
            401,
            "TeleAgent 认证失败（存的 session key 已失效，请重新添加该账号）",
        ))
    } else {
        Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 发送消息失败（HTTP {status}，会话 {session_id}）"),
        ))
    }
}

/// 用显式 key 打一次上游探活（`GET /provider`，带签名）。
///
/// 添加账号时验证用户粘贴的 key 用：
///   - 2xx → Ok（key 有效）；
///   - 401/403 → 400「Session Key 无效」；
///   - 连不上/超时 → 502「本机 super-agent 没在跑」。
pub async fn probe_with_key(key: &str, timeout_ms: u64) -> Result<(), GatewayError> {
    let (status, _) = signed_request_with_key("GET", "/provider", None, timeout_ms, key).await.map_err(|error| {
        // 连接层面的失败转成更直白的文案：用户此刻最可能的情况是
        // TeleAgent 桌面应用根本没在跑。
        if error.status_code == 502 || error.status_code == 504 {
            GatewayError::with_status(
                502,
                format!(
                    "无法连接本机 TeleAgent 桌面应用（127.0.0.1:4397）：{}。请确保 TeleAgent 正在运行后再添加账号",
                    error.message
                ),
            )
        } else {
            error
        }
    })?;
    match status {
        200..=299 => Ok(()),
        401 | 403 => Err(GatewayError::with_status(
            400,
            "Session Key 无效：super-agent 拒绝了签名（401/403），请检查后重新粘贴",
        )),
        other => Err(GatewayError::with_status(
            400,
            format!("TeleAgent 探活失败：super-agent 返回 HTTP {other}"),
        )),
    }
}

/// 默认工作目录（原实现 `DEFAULT_DIRECTORY` =
/// `~/.local/share/TeleAgent/TeleAgent的工作空间`，`expanduser` 口径）。
pub fn default_directory() -> String {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_default();
    if home.is_empty() {
        return String::new();
    }
    format!("{home}/.local/share/TeleAgent/TeleAgent的工作空间")
}

/// 把 OpenAI messages 压成单条 prompt 文本（原实现 `send_prompt_async`
/// 的前半段，逐字同口径）。
///
///   - system 消息拼接（多条换行连接），放在最前；
//   - user 原样；assistant 前缀 `[assistant]`；tool 结果前缀 `[tool_result]`；
//   - content 是数组时只取 `type == "text"` 的部件；`image_url` 走临时文件
///     口径（原实现：base64 存 `/tmp/reachy_vision`，prompt 里提示用
///     `image_understanding` 工具查看 —— 这里目录改名 `teleagent_vision`，
///     其余逐字保留）；
///   - 只有一条 user 且无 system 时直接用那条内容，否则 `\n\n` 连接。
pub fn build_prompt(messages: Option<&Value>, tools: Option<&Value>) -> String {
    let mut system_content: Option<String> = None;
    let mut user_messages: Vec<String> = Vec::new();

    if let Some(Value::Array(items)) = messages {
        for message in items {
            let role = message.get("role").and_then(Value::as_str).unwrap_or("user");
            let content = message_content_text(message.get("content"));
            match role {
                "system" => {
                    system_content = Some(match system_content {
                        Some(existing) => format!("{existing}\n{content}"),
                        None => content,
                    });
                }
                "user" => user_messages.push(content),
                "assistant" => user_messages.push(format!("[assistant] {content}")),
                "tool" => user_messages.push(format!("[tool_result] {content}")),
                _ => user_messages.push(content),
            }
        }
    }

    // 按参考代理的约定，将 function schema 注入 TeleAgent 文本提示词。
    if let Some(Value::Array(definitions)) = tools {
        let mut descriptions = Vec::new();
        for tool in definitions {
            let function = tool.get("function").unwrap_or(tool);
            let name = function.get("name").and_then(Value::as_str).unwrap_or("");
            if name.trim().is_empty() { continue; }
            let description = function.get("description").and_then(Value::as_str).unwrap_or("");
            let parameters = function.get("parameters").cloned().unwrap_or_else(|| json!({}));
            descriptions.push(format!("  - {name}: {description}\n    参数: {parameters}"));
        }
        if !descriptions.is_empty() {
            let instruction = format!(
                "你可以使用以下工具：\n\n{}\n\n需要调用工具时请只输出：[TOOL_CALL]{{\"name\":\"工具名称\",\"arguments\":{{}}}}[/TOOL_CALL]\n不需要调用工具时正常回复。",
                descriptions.join("\n")
            );
            system_content = Some(match system_content {
                Some(existing) => format!("{existing}\n\n{instruction}"),
                None => instruction,
            });
        }
    }

    if user_messages.len() == 1 && system_content.is_none() {
        return user_messages.remove(0);
    }
    let mut parts: Vec<String> = Vec::new();
    if let Some(system) = system_content {
        parts.push(system);
    }
    parts.extend(user_messages);
    if parts.is_empty() {
        return "你好".to_string();
    }
    parts.join("\n\n")
}

/// 取一条消息的文本（`content` 字符串 / text 部件数组 / image_url 临时文件）。
fn message_content_text(content: Option<&Value>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    match content {
        Value::String(text) => text.clone(),
        Value::Array(parts) => {
            let mut texts: Vec<String> = Vec::new();
            for part in parts {
                let part_type = part.get("type").and_then(Value::as_str).unwrap_or("");
                if part_type == "text" {
                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                        texts.push(text.to_string());
                    }
                } else if part_type == "image_url" {
                    texts.push(image_part_text(part.get("image_url")));
                }
            }
            texts.join("\n")
        }
        _ => String::new(),
    }
}

/// 图片部件 → 原实现的临时文件口径（base64 落盘 + prompt 提示）。
fn image_part_text(image_url: Option<&Value>) -> String {
    let url = match image_url {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Object(_)) => image_url
            .and_then(|value| value.get("url"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    };
    let Some(rest) = url.strip_prefix("data:image") else {
        return "[图片：非 base64 图片暂不支持]".to_string();
    };
    let Some(comma) = rest.find(',') else {
        return "[图片解析失败]".to_string();
    };
    let b64data = &rest[comma + 1..];
    let decoded = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        b64data,
    );
    let Ok(bytes) = decoded else {
        return "[图片解析失败]".to_string();
    };
    let dir = std::env::temp_dir().join("teleagent_vision");
    let path = dir.join(format!("proxy_{}.jpg", crate::server::logging::now_ms()));
    let saved = std::fs::create_dir_all(&dir)
        .and_then(|_| std::fs::write(&path, &bytes))
        .is_ok();
    if saved {
        format!(
            "请用 image_understanding 工具查看图片 {} 并回答问题。",
            path.to_string_lossy()
        )
    } else {
        "[图片解析失败]".to_string()
    }
}


/// 回复需要人工确认的 TeleAgent permission/question。只接受服务器刚发出的 ID。
pub(crate) async fn reply_confirmation_with_key(
    kind: &str,
    id: &str,
    payload: &Value,
    key: &str,
) -> Result<(), GatewayError> {
    if !matches!(kind, "permission" | "question")
        || id.is_empty()
        || id.len() > 256
        || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    {
        return Err(GatewayError::with_status(400, "TeleAgent 确认类型或 ID 非法"));
    }
    let path = format!("/{kind}/{id}/reply");
    let (status, body) = signed_request_with_key("POST", &path, Some(payload), 10_000, key).await?;
    if matches!(status, 200 | 204) {
        Ok(())
    } else {
        Err(GatewayError::with_status(
            status as i32,
            format!("TeleAgent 确认回复失败（HTTP {status}）：{}", body.chars().take(200).collect::<String>()),
        ))
    }
}
