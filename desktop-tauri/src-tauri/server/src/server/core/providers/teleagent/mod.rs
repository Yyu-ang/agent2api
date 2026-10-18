//! TeleAgent 适配器：把本地 TeleAgent 桌面应用的 super-agent 接成 provider。
//!
//! ── 协议来源 ──────────────────────────────────────────────────
//! 移植自 `lizhun781021/teleagent-openai-proxy`（MIT，`openai_proxy.py`，
//! 见本目录各文件的「移植自」标注）：TeleAgent 桌面应用在本机
//! `127.0.0.1:4397` 跑一个 super-agent HTTP 服务，对外暴露会话式 API，
//! 本模块把它翻译成网关的 provider 形态。原项目的 HTTP 服务 / Web 控制台 /
//! launchd 持久化不搬 —— 网关自己都有（管理页 / 任务调度 / 安装包服务）。
//!
//! ── 上游长什么样 ──────────────────────────────────────────────
//!   - 鉴权：`SUPER_AGENT_LOCAL_SESSION_KEY`（scheduler 进程环境变量，见
//!     `auth.rs`）做 HMAC-SHA256 签名，`local-v1` 方案；
//!   - 对话：`POST /session` 建会话 → `POST /session/{id}/prompt_async`
//!     异步发消息 → `GET /global/event` SSE 收回复（按 `sessionID` 过滤）；
//!   - 模型：`GET /provider` → `all[]` 里 `source == "config"` 的 provider
//!     才是有密钥的可用模型（内置未配置的不展示），模型 id 形如
//!     `{providerID}/{modelID}`（如 `NewApi/chat-pro`）。
//!
//! ── 与 KukuAI 同属「有状态」但更彻底 ──────────────────────────
//! `is_stateful()` 为 true：一次对话 = 建会话 → 发消息 → SSE 三步，
//! 塞不进单请求契约（`build_chat_request` 返回 503 防御）。
//! 默认每请求新建会话（没有身份的请求复用会让不同客户端的上下文串味）；
//! **流式**请求带显式会话 ID（body `conversation_id` / `metadata.*` 或
//! `x-session-id` 头）时，按账号、密钥、模型、目录、工具定义与网关调用方
//! 隔离复用，只发增量；复用失败自动作废并以新会话 + 全量重试一次（`session.rs`）。
//!
//! ── 人工确认（permission / question）─────────────────────────
//! 默认与旧版一致：上游请求用户确认时立即中断本轮并报 502。显式开启
//! （设置 `teleagentManualConfirmation`、环境变量
//! `AGENT2API_TELEAGENT_MANUAL_CONFIRM=1` 或请求 `metadata.teleagent_manual_confirmation`）
//! 后改为登记进受保护的确认队列，等待管理页 / API 回复（见 `confirmations.rs`）。
//!
//! ── 凭证：session key（账号里存的 / 本机自动提取） ────────────
//! 凭证就是 super-agent 的 session key：账号页手动粘贴（添加时探活）、
//! 或从本机导入（scheduler 进程环境，Unix 才有这条路）、或零账号时网关
//! 自动提取（60 秒缓存，401/403 时强制重取）。因此：
//!   - `allows_anonymous_default_session() = true`（零账号可转发）；
//!   - `env_credentials_present()` = 此刻能否取到 key（决定 `/v1/models`
//!     广告不广告 TeleAgent 的模型）；
//!   - 没有 `login.rs`（无网页登录）、没有 `balance.rs`（上游无额度接口）。
//!
//! ── 出网 ──────────────────────────────────────────────────────
//! super-agent 只监听 `127.0.0.1:4397`（本机回环），**永远直连**：
//! 账号配了代理也不走代理（走代理连不上回环地址），`chat.rs` 里全部传
//! `proxy=None`。这是本机型 provider 的固有性质，不是配置项。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本目录零 unwrap/expect/panic，不持任何锁跨 await。

pub mod adapter;
pub mod auth;
pub mod chat;
pub mod confirmations;
pub mod models;
pub mod protocol;
pub mod stream;
pub mod session;

/// super-agent 的监听地址（teleagent-openai-proxy 写死的常量）
pub const SUPER_AGENT_HOST: &str = "127.0.0.1";
/// super-agent 的监听端口
pub const SUPER_AGENT_PORT: u16 = 4397;

/// 签名方案版本（`X-SA-Sign-Version` 头的值，也是待签串第一段）
pub const SIGN_VERSION: &str = "local-v1";

/// 默认 provider / 模型（原项目常量：`NewApi/chat-pro`）
pub const DEFAULT_PROVIDER_ID: &str = "NewApi";
pub const DEFAULT_MODEL_ID: &str = "chat-pro";

/// 进程级实例（`adapter_for` 返回它的 `&'static` 引用）
pub static TELEAGENT_ADAPTER: adapter::TeleAgentAdapter = adapter::TeleAgentAdapter;

/// 本家 provider id（`providers::kind_id` 的常量形态，供账号存储使用）
pub fn teleagent_id() -> &'static str {
    crate::server::core::providers::kind_id(crate::server::core::providers::ProviderKind::TeleAgent)
}
