//! MiniMax Code 渠道（国内版 `minimax-code` / 国际版 `minimax-code-global`）。
//!
//! ── 上游形态 ────────────────────────────────────────────────
//! MiniMax 的编程订阅客户端（mcode / MiniMax Code）的账号化网关：
//! 登录是 **OAuth 设备流 + PKCE S256**（RFC 8628 变体，见 [`login`]），
//! 对话走其内部 **Mavis** 通道 —— `POST {llm}/mavis/api/v1/llm/v1/
//! messages`，**Anthropic Messages 协议**、无状态转发；令牌是 JWT，
//! refresh token **单次使用**（见 [`credentials`] / [`refresh`] 的纪律）。
//! 两个地区是两套独立站点与账号体系（见 [`region`] 的站点表），因此
//! 按两家 provider 建模、共用一份参数化实现（与 tabbit / zcode 同款）。
//!
//! ── 协议来源（移植而非搬代码）────────────────────────────────
//! 社区插件 **`@magpie-community/opencode-minimax-auth` v0.1.2**
//! （magpie-community/plugins，**MIT License**）：设备流、令牌轮换、
//! Mavis 请求头、模型清单解析、remains 窗口、签名与签到的协议细节
//! 均按该插件的公开实现**逐条核对、用 Rust 重写**，未复制其代码。
//! 签到另参考 magpie 本体（yetone/magpie，MIT）的 Go 实现
//! `internal/provider/minimax_checkin.go`（密钥与端点同源，签名细节
//! 以插件 `checkinOf` 的 signed 调用为准，见 [`checkin`]）。
//!
//! ── v1 范围（已实现）────────────────────────────────────────
//!   - 设备流登录（`core::login` 分发 + 壳侧窗口/浏览器打开授权页，
//!     轮询成功自动落账号）与「粘贴凭证（accessToken/refreshToken）」
//!     两个添加入口；
//!   - 凭证临期主动续期（2 分钟窗口、单飞、比较-再写；只有
//!     400 + invalid_grant 是真登出）+ 401 后强制刷新一次（网关统一
//!     重试层）；
//!   - Anthropic 无状态转发（含 402/403/429 额度语义 → QuotaLimited
//!     的错误分类）；
//!   - 模型目录：静态兜底 4 个 + 上游实时清单（按 `model_order` 解析，
//!     走 `catalog_cache` 分地区缓存）；
//!   - 用量：M Plan 窗口 remains（纯 Bearer，归一为统一 windows 形状）；
//!   - 每日签到：签名调用 status/claim（7 天面板校验、status 2 才
//!     claim），接入 `auto_checkin` 清单与签到中心。
//!
//! ── 功能对照与已知缺口（完整性审计记录）────────────────────────
//! 以 cline（设备流 + 轮换刷新，最接近的同类）与 tabbit / zcode 为
//! 基准逐项对照仓库的按-provider 参与点，结论如下（「已接」均有对应
//! 接线，不是占位）：
//!
//! | 参与点 | 本家 | 说明 |
//! |---|---|---|
//! | 提供商注册表 / adapter_for / implemented_kinds | 已接 | 两家在枚举与清单末尾 |
//! | catalog::refresh_meta（来源/更新日期列） | 已接 | 编译器穷尽 match 逼出来的参与点：两地区各查自己那格目录状态 |
//! | account_transfer 身份映射（导出/导入判重） | 已接 | identity_of_item / identity_of_record 各一支（uid → email）；不接会落到 userId 兜底、导出后导不回来 |
//! | credential_maintenance 主动续期遍历 | 已接 | 框架全泛型：supports_refresh + credentials_expiring 即参与 |
//! | catalog_refresh 模型刷新排期 | 已接 | 框架按 implemented_kinds 泛型遍历，两地区各一份排期键 |
//! | accounts 用量端点 | 已接 | supports_usage + query_usage，走统一端点与 401 刷新重试 |
//! | auto_checkin 清单 + 签到分发 | 已接 | CHECKIN_PROVIDERS 两条 + checkin_for 分发到 [`checkin`] |
//! | store_view 公开形状 / identifier | 已接 | 专属公开形状（uid / email），identifier=uid |
//! | api/accounts 粘贴添加 | 已接 | 两个 kind 走 add_minimaxcode_account；无桌面端可导入 |
//! | api/session 登录分发 + 壳侧 normalize_provider / allowed_hosts | 已接 | 设备流同 cline 链；壳侧两处登记（主机不设限：授权页可能跳身份提供方） |
//! | 前端 add-account-configs / accounts-domain / mobile providers+facts | 已接 | 设备流 webLogin 配置与粘贴表单；mobile 图标与 facts 各两条 |
//! | ui/providers.js / preset-providers.js | 不需要 | 前者目录由后端注册表动态下发；后者是「自定义提供商」模板，与内置家无关（其内已有一条同名的 MiniMax **API Key** 预设，与本渠道无关） |
//! | provider_conversations（有状态会话统计） | 不需要 | 只服务有状态会话的两家（tabbit/teleagent）；本家无状态转发，不进 |
//! | http.rs 专属路由 | 不需要 | tabbit 的 complete 路由是它 Cookie 交回形态专属；本家设备流走通用 login/start|wait|cancel（与 cline 同） |
//! | account_bootstrap / store_admin 存量迁移 | 不需要 | 那是 cline 拆池等**历史 id 改名**的一次性迁移；本家是新家、没有存量旧形态 |
//! | models-page 色相表 / models-fetch-modal 无账号刷新集 | 不接 | 色相未登记有哈希兜底（tabbit 同样未登记）；本家目录刷新是「有账号优先、无账号匿名兜底」的 refresh_uses_account=true 形态，不进无账号集 |
//! | onboarding_memory / readiness 新手任务 | 不接 | 新手任务是按家定制的产品功能（raccoon/chatglm 等才有），cline/tabbit 均无；无上游依据不硬造 |
//! | model_rules 推理档位种子 | 不接 | 档位由模型目录的 efforts 字段驱动（model_meta 读取），无需种子规则 |
//!
//! 已知缺口（均为**主动取舍**，不是漏做）：
//!   1. **identity 未调**：插件登录后用签名账号 API
//!      （`/v1/api/user/info`）补 realUserID / email / name。v1 账号
//!      标识用 JWT 的 `account_id` / `sub`、展示名用邮箱声明（JWT
//!      里有才有）→ 站点名兜底。后果：账号列表的展示名可能不如插件
//!      精确；**不影响**转发、续期、用量、签到（签到的 user_id 用
//!      JWT 派生的 uid，上游缺省 0 也可签）。签名机制本身已因签到
//!      落地在 [`checkin`]，后续补 identity 只需复用它换一次调用。
//!   2. **用量缺 credits 余额与 plan 等级**：它们来自签名账号 API 的
//!      membership（`get_user_extra_info` / `get_membership_info`），
//!      且 remains 带 `X-Group-Id` 也需要先从 membership 拿组织 id。
//!      v1 用量只给 remains 的窗口百分比，`available` /
//!      `subscription` / `planName` 如实给 null（见 [`balance`]
//!      模块头），不拿百分比冒充额度。补齐路径同上：复用 [`checkin`]
//!      的 signed 调用加两个端点即可。
//!   3. **429 的分类口径**：插件对「裸 429」原样透传、只对命中额度
//!      语义的 402/403 改写。本家 429 一律判 QuotaLimited（与本仓
//!      各家既有口径一致，对外同为 429 语义），402/403 才要求命中
//!      额度语义——差异只影响内部冷却记账，不影响客户端所见状态码。
//!   4. **M3.1-Flash 的 effort 档位**：插件经 AI SDK 发
//!      `thinking: {type: "adaptive"}` + effort；本家经网关统一的
//!      Anthropic 转换把客户端的 reasoning_effort 换成 thinking
//!      预算（等价语义、不同线格式），未单独为本家改转换层。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本模块绝不 unwrap/expect/panic。

pub mod adapter;
pub mod balance;
pub mod checkin;
pub mod credentials;
pub mod login;
pub mod models;
pub mod refresh;
pub mod region;

pub use adapter::{MINIMAX_CODE_ADAPTER, MINIMAX_CODE_GLOBAL_ADAPTER};
