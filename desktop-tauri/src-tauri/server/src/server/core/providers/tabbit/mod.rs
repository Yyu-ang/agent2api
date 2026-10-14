//! Tabbit（浏览器 AI 助手）适配器：把 tabbit2api 的协议逻辑移植为
//! agent2api 的 Rust provider。
//!
//! ── 上游长什么样（tabbit2api 2026-10 实测，本模块照抄）─────────
//! Tabbit 浏览器网页端的模型能力：`token`（HttpOnly JWT）Cookie 认证；
//! `GET /chat/sign-key` 取动态签名 key（各域名独立）；请求签名
//! `x-nonce = HMAC-SHA256(key, "{ts}.{uuid}.{sha256hex(body)}")`；
//! 对话 `POST /panel/session` 建会话 → `POST /api/v1/chat/completion`（SSE）；
//! `selected_model` 必须传目录里的 `display_name`；额度
//! `GET /api/commerce/quota/v1/usage`；模型目录
//! `GET /proxy/v1/model_config/models`。
//!
//! ── 两个地区是两家 provider ───────────────────────────────────
//! 国内版（`tabbit`，`web.tabbit.com`）/ 国际版（`tabbit-intl`，`web.tabbit.ai`，
//! 国内直连 403）：同一份实现按 `region::Region` 参数化（与 AutoClaw / Qoder /
//! Accio / ZCode 同款）。地区是 provider 身份，不是账号属性。
//!
//! ── 转发路线 ──────────────────────────────────────────────────
//! 有状态（`is_stateful = true`）：建会话 → 签名 POST → SSE 的两步时序塞不进
//! 单请求契约，走 `forward_conversation`（`chat.rs`）；会话是请求内的，
//! 不跨请求保持状态（与 KukuAI 同形态）。
//!
//! ── 账号 ──────────────────────────────────────────────────────
//! 粘贴 Cookie（`token`）/ 网页登录（官方登录页 + 壳侧读 Cookie 收尾）。
//! 上游**没有**刷新接口（`supports_refresh = false`），JWT 过期只能重登。
//!
//! ── 子模块 ────────────────────────────────────────────────────
//!   region.rs      地区（国内/国际）参数化
//!   protocol.rs    HMAC 签名 / sign-key 缓存 / 请求头 / unique-uuid / 请求体
//!   http.rs        出站请求的薄封装（JSON / 纯文本 / SSE）
//!   credentials.rs 凭证解析与快照
//!   models.rs      模型目录（静态兜底 + 远程刷新）
//!   chat.rs        会话式转发（建会话 → SSE → OpenAI 形状）
//!   balance.rs     额度查询
//!   login.rs       网页登录
//!   adapter.rs     ProviderAdapter 实现
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本模块零 unwrap/expect/panic，不持任何锁跨 await。

pub mod adapter;
pub mod balance;
pub mod chat;
pub mod credentials;
pub mod http;
pub mod login;
pub mod models;
pub mod protocol;
pub mod region;
pub mod session;

pub use region::Region;

/// 客户端硬编码的 OAuth client_id（tabbit2api 实测值）
pub const CLIENT_ID: &str = "2dd8eb4c1ed9c344d173";

/// 本家 provider id（`providers::kind_id` 的常量形态，供账号存储使用）
pub fn tabbit_id(region: Region) -> &'static str {
    crate::server::core::providers::kind_id(region.kind())
}
