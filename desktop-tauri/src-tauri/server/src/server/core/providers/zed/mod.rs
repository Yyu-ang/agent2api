//! Zed（`zed.dev` 的编辑器订阅：Zed Pro / Pro Trial / Business / Student /
//! VIP）——把账号在 Zed 云端托管的四家模型（Anthropic / OpenAI / Google /
//! xAI）接进网关。
//!
//! ── 协议来源（移植出处，MIT）──────────────────────────────
//! 社区插件 `@magpie-community/opencode-zed-auth` v0.1.10
//! （<https://github.com/magpie-community/plugins/tree/main/packages/zed>，
//! MIT License）。该插件本身移植自 magpie（yetone/magpie，MIT）内置的 Zed
//! 账号实现，其协议读自 Zed 官方源码（zed-industries/zed **1.23.0**）。
//! 本模块是按该协议的 **Rust 重写**（不复制其 JS 代码组织），逐条语义以
//! 插件 `index.mjs` 为准，差异处在各子模块头注明。
//!
//! ── 上游形态 ──────────────────────────────────────────────
//!   * **登录**：Zed 官方 `native_app_signin`。本进程生成 RSA-2048 密钥对 +
//!     `systemId`（UUID），在 127.0.0.1 随机端口起一次性监听，把
//!     `{SITE}/native_app_signin?native_app_port=…&native_app_public_key=
//!     <url-safe base64（带 padding）的 PKCS#1 DER 公钥>&system_id=…`
//!     交给浏览器；回调 query 带 `user_id` 与 RSA 加密的 `access_token`，
//!     解密（OAEP-SHA256，失败再 PKCS#1 v1.5）后得到访问令牌，再
//!     `GET {CLOUD}/client/users/me` 读账号与组织/套餐。形态与 Trae 的
//!     「本地监听回调」同类，因此登录编排照 Trae 的待办表模式落在
//!     `core::login::zed`（见 `login.rs`）。
//!   * **凭证**：`access` = 访问令牌；其余状态（userId / systemId / org /
//!     plan / planName / login / name）以 JSON 存进账号记录的 `refreshToken`
//!     字段 —— 与插件把同一份 JSON 存进 oauth 条目 `refresh` 字段逐字对应。
//!     **Zed 没有 refresh token**：令牌被拒（铸造接口 401）即登录过期，
//!     `supports_refresh()` 恒 false，只能重新登录。
//!   * **模型令牌**：转发/拉目录前用 `POST {CLOUD}/client/llm_tokens`
//!     `{organization_id}` 换一枚短期模型令牌（内存缓存 + 单飞铸造，见
//!     `tokens.rs`）；令牌被判 stale（HTTP 401 或 `x-zed-expired-token` /
//!     `x-zed-outdated-token` 响应头）时重铸一次再发。
//!   * **目录**：`GET {CLOUD}/models`（Bearer 模型令牌），剔除 disabled 与
//!     四家之外的 provider；进程内 10 分钟 TTL + `catalog_cache` 落盘恢复，
//!     拉不到时回落插件同款的静态 6 模型（见 `models.rs`）。
//!   * **转发**：`POST {CLOUD}/completions`，body `{provider, model,
//!     provider_request}` —— `provider_request` 是该 vendor **自己的原生
//!     请求**；响应是 NDJSON 逐行（`{"event": <vendor 原生流事件>}` 或状态
//!     行）。四家 wire 各不相同，所以 `is_stateful() = true`，走
//!     `forward_conversation` 自行完成一轮（见下「路线取舍」）。
//!   * **用量**：Zed 只报套餐与账期、不报已用量 —— `query_usage` 给套餐名
//!     （Free 显示为「No plan」）+ 账期结束时间，**没有 windows**；逾期发票
//!     使模型暂停、作 error 报出（见 `balance.rs`）。
//!   * **组织 repick**：组织被 402 拒绝（无可用套餐）时重读 users/me、换到
//!     有套餐的组织、回写账号并重试一次（插件 `repick`/`move` 语义），接在
//!     转发与拉目录两条路径上；用量路径只回写 org/plan 不重试查询本身。
//!
//! ── 路线取舍：为什么走 `forward_conversation` 而不是新开
//!    `UpstreamResponse` 变体 ────────────────────────────────
//! 入站三端点（chat / messages / responses）在 `api` 层已统一归一成 Chat
//! 形态才进编排层，所以适配器要做的事是「Chat → vendor 原生请求 → NDJSON
//! 解包 → vendor SSE → Chat SSE」：请求侧复用 `protocol` 既有的
//! `anthropic_request_from_chat` / `responses_request_from_chat` /
//! `antigravity_request_from_chat`（取其 Gemini 内层请求），响应侧复用
//! `ChatFromAnthropicStream` / `ChatFromResponsesStream` /
//! `ChatFromAntigravityStream` 三台既有翻译状态机（xAI 的 Chat wire 本身
//! 就是 Chat SSE、直通）。新开 `UpstreamResponse` 变体则要动编排层的
//! 分派、SSE 出口与聚合三处公共设施，去换一个本家独占的 NDJSON 解包器 ——
//! 复用更少、公共面改动更大。代价如实记下：有状态路径**没有**无状态路径
//! 的原地退避重试与 token 强制刷新动作；但账号轮换（队列顺延）、限额
//! 冷却记账（`classify_conversation_error` 把 402/429 记成 QuotaLimited）
//! 与 telemetry 记账都由编排层照常提供，插件的 stale 重铸与 402 repick
//! 在本家内部各重试一次，合起来不弱于无状态路径的处置。
//!
//! ── v1 范围与已知缺口（如实清单）─────────────────────────
//!   * 四个 vendor（anthropic / open_ai / x_ai / google）全部接通转发，
//!     没有「广告了却转不了」的模型：目录里出现哪家就转发哪家。
//!   * **effort 档位**：目录条目带 `reasoningLevels`（上游
//!     `supported_effort_levels`）；出站的思考强度走本仓既有的
//!     `reasoning_effort` 通道由各 vendor 的请求转换器折算（Anthropic →
//!     thinking budget、Responses → reasoning.effort、Gemini →
//!     thinkingConfig）。插件那种「按 vendor 拼 providerOptions 变体」的
//!     逐档变体表没有等价物，未单独复刻。
//!   * **签到**：不适用 —— Zed 没有签到概念，本家不进 `auto_checkin` /
//!     签到中心（与别家「没有此能力就不登记」同一处置）。
//!   * **桌面端导入**：不提供 —— Zed 客户端的登录态没有 auth.json 那种
//!     稳定可读形态，给了入口只会稳定失败（与 Accio / ZCode 同一处置）。
//!   * 登录超时取网关共用的 `LOGIN_TIMEOUT_MS`（5 分钟），插件是 10 分钟
//!     —— 与 Trae 同一取舍（共用 `/wait` 协议与界面计时口径），超时后
//!     重新发起即可。
//!
//! ── 功能对照（与 cline / zcode / tabbit 的完整性审计）──────
//!   登录（native_app_signin 本地监听）……… 有（`login.rs` + `core::login::zed`）
//!   续期声明 …………………………………… 无续期协议，`supports_refresh=false`（如实）
//!   模型刷新 + 排期 ………………………… 有（`supports_model_refresh=true`，
//!                                       进 `catalog_refresh` 统一排期；
//!                                       scope = `zed` 的落盘缓存）
//!   用量 ……………………………………… 有（套餐/账期，无 windows，见上）
//!   签到 ……………………………………… 不适用（无此概念）
//!   credential_maintenance ……………… 参与但恒跳过（`supports_refresh=false`
//!                                       → 维护任务如实报「不支持主动刷新」）
//!   catalog_refresh ………………………… 有（同模型刷新）
//!   账号公开形态 / store_view ………… 有（`account_store::zed_accounts`，
//!                                       不透出 access 与状态 JSON 本体）
//!   路由与目录参与 ………………………… 有（注册表 + `catalog::refresh_meta`）
//!
//! ── 硬约束 ────────────────────────────────────────────────
//! release 是 `panic=abort`：本目录零 unwrap / expect / panic。

pub mod adapter;
pub mod balance;
pub mod credentials;
pub mod forward;
pub mod http;
pub mod login;
pub mod models;
pub mod protocol;
pub mod tokens;

/// 站点（登录页与成功页）。
pub const SITE: &str = "https://zed.dev";
/// 云端 API 基址（`/client/*`、`/models`、`/completions` 都在它下面）。
pub const CLOUD: &str = "https://cloud.zed.dev";
/// 请求自报的 Zed 版本（读自 Zed 官方源码 1.23.0；`x-zed-version` 头与 UA 用它）。
pub const VERSION: &str = "1.23.0";
/// 登录成功后浏览器被 302 送去的页面（插件 `SUCCEEDED` 逐字）。
pub const SUCCEEDED_URL: &str = "https://zed.dev/native_app_signin_succeeded";

/// 本家的 provider id（注册表是唯一事实来源，这里只是本目录内的简写）。
pub const PROVIDER_ID: &str = "zed";

/// Zed 的 User-Agent：`Zed/<版本> (<os>; <arch>)`，os / arch 用 Rust 的名字
/// （`std::env::consts` 的取值正好是 macos / windows / linux 与
/// aarch64 / x86_64，与插件在 Node 侧手工映射的结果一致）。
pub fn user_agent() -> String {
    format!(
        "Zed/{VERSION} ({}; {})",
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}
