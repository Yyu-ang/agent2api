//! Zed 云端（`cloud.zed.dev`）的 HTTP 出口：`/client/*` 账号调用、模型
//! 目录与 `/completions` 转发，四条路径共用同一组头与错误形态。
//!
//! ── 两组头不要混 ──────────────────────────────────────────
//!   * `/client/*`（users/me、llm_tokens）：`Authorization: "<userId>
//!     <access>"`（**不是 Bearer**，是 userId 与令牌空格拼接的 Zed 自有
//!     形态）+ `x-zed-system-id` + UA + JSON content-type；
//!   * 模型面（/models、/completions）：`Authorization: Bearer <模型令牌>`
//!     + 各自的 `x-zed-*` 能力头（见调用点）。
//! 拼错一组就是稳定的 401，而且日志里与「令牌过期」长得一模一样 —— 所以
//! 拼头只在这里发生一次，调用方只给凭证与路径。
//!
//! ── 超时 ──────────────────────────────────────────────────
//! 账号面调用按插件的量级设总超时（users/me 15~20 秒、铸造 20 秒、目录
//! 15 秒）：这些是短请求，挂住不该拖住登录/刷新。`/completions` 是长流，
//! **不设总超时**（与 trae 转发同一取舍，读空闲由编排层兜）。

use std::time::Duration;

use reqwest::Response;

use crate::server::core::egress;
use crate::server::core::proxies::ResolvedProxy;

use super::credentials::Credential;
use super::{user_agent, CLOUD};

/// 上游回了一个非 2xx（插件的 `ZedStatus`）：状态 + 原始 body + 两个
/// stale 判定头是否存在（目录/转发路径的重铸判定要用）。
#[derive(Debug)]
pub struct HttpFailure {
    pub status: u16,
    pub body: String,
    pub expired_token: bool,
    pub outdated_token: bool,
}

impl HttpFailure {
    /// 模型令牌被判 stale：HTTP 401 或任一 stale 头存在（插件 `stale` 逐字）。
    pub fn is_stale(&self) -> bool {
        self.status == 401 || self.expired_token || self.outdated_token
    }
}

/// 一次云端调用的失败：上游拒绝（带状态）或传输层错误（只有文案）。
#[derive(Debug)]
pub enum CloudError {
    Status(HttpFailure),
    Transport(String),
}

impl CloudError {
    pub fn status(&self) -> Option<u16> {
        match self {
            CloudError::Status(failure) => Some(failure.status),
            CloudError::Transport(_) => None,
        }
    }

    pub fn message(&self) -> String {
        match self {
            CloudError::Status(failure) => {
                super::protocol::failure(failure.status, &failure.body)
            }
            CloudError::Transport(detail) => detail.clone(),
        }
    }
}

fn stale_flags(headers: &reqwest::header::HeaderMap) -> (bool, bool) {
    (
        headers.contains_key("x-zed-expired-token"),
        headers.contains_key("x-zed-outdated-token"),
    )
}

/// `/client/*` 的头组（见模块头）。
fn client_headers(credential: &Credential) -> Vec<(String, String)> {
    let mut headers = vec![
        (
            "Authorization".to_string(),
            format!("{} {}", credential.user_id, credential.access),
        ),
        ("Content-Type".to_string(), "application/json".to_string()),
        ("User-Agent".to_string(), user_agent()),
    ];
    if !credential.system_id.is_empty() {
        headers.push((
            "x-zed-system-id".to_string(),
            credential.system_id.clone(),
        ));
    }
    headers
}

/// 发一次 `/client/*` 调用并把 body 读成文本（短请求，整段读完再判状态）。
async fn cloud_call(
    credential: &Credential,
    method: reqwest::Method,
    path: &str,
    body: Option<&serde_json::Value>,
    timeout: Duration,
    proxy: Option<&ResolvedProxy>,
) -> Result<String, CloudError> {
    let mut request = egress::client_for(proxy)
        .request(method, format!("{CLOUD}{path}"))
        .timeout(timeout);
    for (name, value) in client_headers(credential) {
        request = request.header(name, value);
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = request
        .send()
        .await
        .map_err(|error| CloudError::Transport(egress::describe_error_detail(&error)))?;
    let status = response.status().as_u16();
    let (expired_token, outdated_token) = stale_flags(response.headers());
    let text = response
        .text()
        .await
        .map_err(|error| CloudError::Transport(egress::describe_error_detail(&error)))?;
    if !(200..300).contains(&status) {
        return Err(CloudError::Status(HttpFailure {
            status,
            body: text,
            expired_token,
            outdated_token,
        }));
    }
    Ok(text)
}

/// `GET /client/users/me` → 解析后的 JSON（解析失败按传输错误报：调用方
/// 拿到的是「读不懂的账号」，与插件 `parse` 失败后报错同一处置）。
pub async fn users_me(
    credential: &Credential,
    proxy: Option<&ResolvedProxy>,
    timeout: Duration,
) -> Result<serde_json::Value, CloudError> {
    let text = cloud_call(
        credential,
        reqwest::Method::GET,
        "/client/users/me",
        None,
        timeout,
        proxy,
    )
    .await?;
    serde_json::from_str(&text)
        .map_err(|_| CloudError::Transport("Zed：users/me 的响应无法解析".to_string()))
}

/// 铸造一枚模型令牌（`POST /client/llm_tokens`）。401 在这里是**登录过期**
/// 的信号，由调用方（`tokens`）翻译，其余状态原样带回。
pub async fn mint_model_token(
    credential: &Credential,
    proxy: Option<&ResolvedProxy>,
) -> Result<String, CloudError> {
    let body = serde_json::json!({ "organization_id": credential.org });
    let text = cloud_call(
        credential,
        reqwest::Method::POST,
        "/client/llm_tokens",
        Some(&body),
        Duration::from_secs(20),
        proxy,
    )
    .await?;
    let parsed: serde_json::Value = serde_json::from_str(&text)
        .map_err(|_| CloudError::Transport("Zed：llm_tokens 的响应无法解析".to_string()))?;
    parsed
        .get("token")
        .and_then(serde_json::Value::as_str)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .ok_or_else(|| CloudError::Transport("Zed：llm_tokens 的响应里没有模型令牌".to_string()))
}

/// `GET /models` 的原始结果（状态 + body + stale 头）：判 stale / 402 的
/// 动作在 `models` 那边（它要决定重铸与 repick），这里只负责取回。
pub struct RawModels {
    pub status: u16,
    pub text: String,
    pub expired_token: bool,
    pub outdated_token: bool,
}

pub async fn fetch_models_raw(
    model_token: &str,
    proxy: Option<&ResolvedProxy>,
) -> Result<RawModels, CloudError> {
    let response = egress::client_for(proxy)
        .get(format!("{CLOUD}/models"))
        .timeout(Duration::from_secs(15))
        .header("Authorization", format!("Bearer {model_token}"))
        .header("x-zed-client-supports-x-ai", "true")
        .header("User-Agent", user_agent())
        .send()
        .await
        .map_err(|error| CloudError::Transport(egress::describe_error_detail(&error)))?;
    let status = response.status().as_u16();
    let (expired_token, outdated_token) = stale_flags(response.headers());
    let text = response
        .text()
        .await
        .map_err(|error| CloudError::Transport(egress::describe_error_detail(&error)))?;
    Ok(RawModels {
        status,
        text,
        expired_token,
        outdated_token,
    })
}

/// `POST /completions`：返回未读的响应（调用方判状态后自己消费字节流）。
/// 头组逐字对照插件 `complete`：版本头 + 两个能力声明头缺一不可 —— 少了
/// status-messages 声明，上游就不发 `stream_ended` 状态行，「回复是否
/// 完整」的判定就没了依据。
pub async fn post_completions(
    model_token: &str,
    body: &serde_json::Value,
    proxy: Option<&ResolvedProxy>,
) -> Result<Response, String> {
    egress::client_for(proxy)
        .post(format!("{CLOUD}/completions"))
        .header("Authorization", format!("Bearer {model_token}"))
        .header("Content-Type", "application/json")
        .header("x-zed-version", super::VERSION)
        .header("x-zed-client-supports-status-messages", "true")
        .header(
            "x-zed-client-supports-stream-ended-request-completion-status",
            "true",
        )
        .header("User-Agent", user_agent())
        .json(body)
        .send()
        .await
        .map_err(|error| egress::describe_error_detail(&error))
}

/// 响应是否声明了 status 信封（`x-zed-server-supports-status-messages`)。
pub fn wrapped(headers: &reqwest::header::HeaderMap) -> bool {
    headers.contains_key("x-zed-server-supports-status-messages")
}

/// repick：重读 users/me，按当前 org 重新选组织；换了才返回新凭证。
///
/// 插件 `repick` 的三条语义逐条保留：读失败/读不懂 → 不换（如实 false）；
/// 选出的还是当前组织 → 不换；换了 → 返回换后的凭证，由调用方回写账号。
/// 回写动作不在这里做：三条调用路径（转发 / 目录 / 用量）里只有带 store 的
/// 才写得回去，统一由调用方在拿到新凭证后调
/// `AccountStore::update_zed_organization`。
pub async fn repick(
    credential: &Credential,
    proxy: Option<&ResolvedProxy>,
) -> Option<Credential> {
    let me = users_me(credential, proxy, Duration::from_secs(15)).await.ok()?;
    let org = super::credentials::org_of(&me, &credential.org);
    if org.is_empty() || org == credential.org {
        return None;
    }
    let plan = super::credentials::plan_of(&me, &org);
    Some(credential.with_org(&org, &plan))
}
