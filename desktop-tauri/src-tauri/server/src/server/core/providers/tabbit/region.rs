//! Tabbit 地区：国内版 / 国际版是**两家 provider**（同一份实现按地区参数化）。
//!
//! ── 为什么拆家（与 AutoClaw / Qoder / Accio / ZCode 同一思路）──────
//! 参考实现（tabbit2api）里地区是**账号的属性**（`region` 字段），后果与那几次
//! 拆家前一模一样：两板块 14 个同名模型（GLM-5.3、Kimi-K3…）在同一家目录里靠
//! `cn/` / `intl/` 前缀区分，账号按板块分组、转发按前缀选板块 ——
//! 「这条账号走哪个域名」在账号记录里看不出来，且国际版在国内直连固定 403
//! （地区限制），出网策略（要不要跟随系统代理）也该按地区给。
//! 按两家建模后：各自独立的账号、清单、启停；转发域名由 provider 身份决定，
//! 不再从账号字段反推。
//!
//! ── 与参考实现的对应 ──────────────────────────────────────────
//! 参考的 `REGION_PRESETS`：`cn → https://web.tabbit.com`（遗留 `web.tabbit-ai.com`
//! 同属国内版）、`intl → https://web.tabbit.ai`（国内直连 403
//! "Service Unavailable in Your Region"）。`region_for_base_url` 的域名归位逻辑
//! 在粘贴导入时仍用（用户可能从任一域名拷 Cookie），见 `credentials.rs`。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use super::super::{kind_id, ProviderKind};

/// Tabbit 地区（国内版 / 国际版）。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Region {
    /// 国内版（`tabbit`，`https://web.tabbit.com`）
    Cn,
    /// 国际版（`tabbit-intl`，`https://web.tabbit.ai`；国内直连 403）
    Intl,
}

impl Region {
    /// 全部地区（顺序 = 注册表顺序：国内版在前）
    pub const ALL: [Region; 2] = [Region::Cn, Region::Intl];

    /// 地区 → ProviderKind
    pub const fn kind(self) -> ProviderKind {
        match self {
            Region::Cn => ProviderKind::Tabbit,
            Region::Intl => ProviderKind::TabbitIntl,
        }
    }

    /// 地区 → provider id（从注册表推导，不写字面量）
    pub fn provider_id(self) -> &'static str {
        kind_id(self.kind())
    }

    /// ProviderKind → 地区（非 Tabbit 系返回 None）
    pub fn from_kind(kind: ProviderKind) -> Option<Region> {
        match kind {
            ProviderKind::Tabbit => Some(Region::Cn),
            ProviderKind::TabbitIntl => Some(Region::Intl),
            _ => None,
        }
    }

    /// 上游基址（参考 `REGION_PRESETS`）
    pub const fn base_url(self) -> &'static str {
        match self {
            Region::Cn => "https://web.tabbit.com",
            Region::Intl => "https://web.tabbit.ai",
        }
    }

    /// 登录页（网页登录拉起的地址：主站，用户在官方页面完成登录）
    pub const fn login_url(self) -> &'static str {
        match self {
            Region::Cn => "https://web.tabbit.com/",
            Region::Intl => "https://web.tabbit.ai/",
        }
    }

    /// 给人看的地区名
    pub const fn label(self) -> &'static str {
        match self {
            Region::Cn => "国内版",
            Region::Intl => "国际版",
        }
    }

    /// 由登录域名反推地区（粘贴导入时用户可能从任一域名拷 Cookie）。
    ///
    /// 参考 `region_for_base_url`：国内域名组 / 国际域名组；认不出返回 None
    /// （调用方回落到用户在添加弹窗里选的那一家）。
    pub fn from_base_url(url: &str) -> Option<Region> {
        let lower = url.trim().to_ascii_lowercase();
        // 取 host（容忍带 scheme / 路径的输入）
        let host = lower
            .split("://")
            .last()
            .unwrap_or(&lower)
            .split('/')
            .next()
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("");
        if matches!(
            host,
            "web.tabbit.com"
                | "www.tabbit.com"
                | "tabbit.com"
                | "web.tabbit-ai.com"
                | "www.tabbit-ai.com"
                | "tabbit-ai.com"
        ) {
            Some(Region::Cn)
        } else if matches!(
            host,
            "web.tabbit.ai"
                | "www.tabbit.ai"
                | "tabbit.ai"
                | "web.tabbitbrowser.com"
                | "tabbitbrowser.com"
        ) {
            Some(Region::Intl)
        } else {
            None
        }
    }

    /// 国际版在国内直连固定 403（地区限制）：账号未显式配代理时跟随系统代理。
    ///
    /// 与 Antigravity 同一口径（见 trait 文档）：「上游在多数网络里只有经代理
    /// 才可达的家」。系统没配代理时等价于直连，不引入新的失败面；国内版保持
    /// 直连默认。
    pub const fn system_proxy_when_unset(self) -> bool {
        match self {
            Region::Cn => false,
            Region::Intl => true,
        }
    }
}
