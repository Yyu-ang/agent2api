//! MiniMax Code 地区：国内版 / 国际版是**两家 provider**（同一份实现按地区参数化）。
//!
//! ── 为什么拆家（与 Tabbit / ZCode / AutoClaw 同一思路）──────
//! 两站是**两套独立的账号体系**：OAuth 站点、LLM 站点、平台站点各不相同，
//! 一边的 refresh token 在另一边毫无意义。把地区做成账号上的字段，会让
//! 「这条账号走哪个域名」在记录里看不出来、目录缓存与刷新排期也只能共用
//! 一格互相覆盖。按两家建模后各自独立，转发域名由 provider 身份决定。
//!
//! ── 站点表（协议来源：社区插件 `@magpie-community/opencode-minimax-auth`
//! v0.1.2 的 `SITES`，即 mcode 正式构建里的 prod 配置）──────────────
//! ```text
//!   国内版 minimax-code：
//!     account  https://account.minimax.cn   （OAuth 设备流 / 刷新）
//!     llm      https://agent.minimax.cn     （对话 / 模型清单 / 签到）
//!     agent    https://agent.minimaxi.com   （签名账号 API，v1 未接，见 mod 头）
//!     platform https://www.minimaxi.com     （M Plan 窗口 remains）
//!     region=cn, lang=zh
//!   国际版 minimax-code-global：
//!     account  https://account.minimax.io
//!     llm      https://agent.minimax.io
//!     agent    https://agent.minimax.io
//!     platform https://platform.minimax.io
//!     region=en, lang=en
//! ```
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use super::super::{kind_id, ProviderKind};

/// MiniMax Code 地区（国内版 / 国际版）。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Region {
    /// 国内版（`minimax-code`）
    Cn,
    /// 国际版（`minimax-code-global`）
    Global,
}

impl Region {
    /// 全部地区（顺序 = 注册表顺序：国内版在前）
    pub const ALL: [Region; 2] = [Region::Cn, Region::Global];

    /// 地区 → ProviderKind
    pub const fn kind(self) -> ProviderKind {
        match self {
            Region::Cn => ProviderKind::MinimaxCode,
            Region::Global => ProviderKind::MinimaxCodeGlobal,
        }
    }

    /// 地区 → provider id（从注册表推导，不写字面量）
    pub fn provider_id(self) -> &'static str {
        kind_id(self.kind())
    }

    /// ProviderKind → 地区（非 MiniMax Code 系返回 None）
    pub fn from_kind(kind: ProviderKind) -> Option<Region> {
        match kind {
            ProviderKind::MinimaxCode => Some(Region::Cn),
            ProviderKind::MinimaxCodeGlobal => Some(Region::Global),
            _ => None,
        }
    }

    /// provider id → 地区（非 MiniMax Code 系返回 None）
    pub fn from_provider_id(id: &str) -> Option<Region> {
        Region::ALL
            .into_iter()
            .find(|region| region.provider_id() == id)
    }

    /// OAuth 站点（设备流发码 / 轮询 / 刷新都在这里）
    pub const fn account_base(self) -> &'static str {
        match self {
            Region::Cn => "https://account.minimax.cn",
            Region::Global => "https://account.minimax.io",
        }
    }

    /// LLM 站点（对话 `/mavis/api/v1/llm/v1/messages`、模型清单、签到都在这里）
    pub const fn llm_base(self) -> &'static str {
        match self {
            Region::Cn => "https://agent.minimax.cn",
            Region::Global => "https://agent.minimax.io",
        }
    }

    /// 签名账号 API 站点（identity / membership；v1 只有签到用到同一套签名，
    /// 账号 API 本身未接，见 `super::mod` 模块头「已知缺口」）
    pub const fn agent_base(self) -> &'static str {
        match self {
            Region::Cn => "https://agent.minimaxi.com",
            Region::Global => "https://agent.minimax.io",
        }
    }

    /// 开放平台站点（M Plan 窗口 `coding_plan/remains` 在这里）
    pub const fn platform_base(self) -> &'static str {
        match self {
            Region::Cn => "https://www.minimaxi.com",
            Region::Global => "https://platform.minimax.io",
        }
    }

    /// 上游 `region` 参数（模型清单接口与签名账号 API 的 query 都用它那一侧的值：
    /// 插件 `SITES.region` —— 国内 `cn`、国际 `en`，**不是** `zh/en`）
    pub const fn region_param(self) -> &'static str {
        match self {
            Region::Cn => "cn",
            Region::Global => "en",
        }
    }

    /// 上游 `lang` / `sys_language` 参数（签名账号 API 用）
    pub const fn lang(self) -> &'static str {
        match self {
            Region::Cn => "zh",
            Region::Global => "en",
        }
    }

    /// 产品名（插件 `SITES.name`；账号名兜底与文案用）
    pub const fn site_name(self) -> &'static str {
        match self {
            Region::Cn => "MiniMax Code",
            Region::Global => "MiniMax Code (Global)",
        }
    }

    /// 给人看的地区名
    pub const fn label(self) -> &'static str {
        match self {
            Region::Cn => "国内版",
            Region::Global => "国际版",
        }
    }
}
