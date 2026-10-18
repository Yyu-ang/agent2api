# Tabbit / TeleAgent 行为变更说明（PR #1：provider-feature-parity-20261014）

这份说明列出本 PR 对外可见的行为变化，合入 beta、写发布日志时按这里核对。
「破坏性」= 已有客户端可能需要改配置或改代码。

## 破坏性变更

| 变更 | 以前 | 现在 | 影响 / 迁移 |
|---|---|---|---|
| 未知模型 | 静默回退到默认模型 | 返回 400 | 客户端必须用 `/v1/models` 里广告的名字。TeleAgent 冷启动目录为空时只认内置兜底模型（`NewApi/chat-pro` 等） |
| Tabbit 模型前缀与地区不一致 | 忽略前缀 | 返回 400 | 国内版 / 国际版的模型名不能混用 |
| TeleAgent 模型目录刷新 | 不需要账号 | `refresh_uses_account = true`，没有账号时刷新失败 | 先添加 TeleAgent 账号（或确保本机能自动提取 key） |
| 上游流提前结束（EOF 前没有结束事件） | 补一个 `[DONE]` 当作成功 | 报错（502） | 半截回答不再被当成完整回答 |
| Tabbit usage 口径 | 按字符数 | 优先用上游 `usage` 事件，否则按 token 粗估（CJK 1 字 ≈ 1 token，其它 4 字符 ≈ 1 token），估算值带 `estimated: true` | 统计页的 Tabbit token 数与旧版不可直接对比 |
| 模型 `available` 字段 | 恒为 true | 跟随账号 / 渠道的 `enabled` | 停用的渠道不再出现在可用列表里 |
| TeleAgent 客户端断开 | 记 502 | 记 499 | 日志 / 统计里断开不再算作上游错误 |

## 新增能力（默认不改变旧行为）

- **跨请求会话复用**：只有**流式**请求带显式会话 ID 时才复用上游会话、只发增量。
  会话 ID 来源（优先级从高到低）：body `conversation_id` → `metadata.conversation_id`
  → `metadata.agent2api_conversation_id` → 请求头 `x-session-id`（与 CatPaw 同口径）。
  - 隔离维度：账号、凭证、模型（TeleAgent 另加工作目录与工具定义）、**网关调用方凭证**
    （API Key 的哈希），以及会话 ID。
  - 非流式请求（标题生成、摘要等辅助请求）不读也不写会话映射，每次独立。
  - 历史被改写、或与上一轮完全相同（重试 / 重新生成）时新建会话发全量。
  - 增量不再回灌上游自己刚生成的 assistant 回复；TeleAgent 增量也不再重发 system 与工具 schema。
  - 复用会话上发送失败时自动作废，用新会话 + 全量历史重试一次；任何失败都会作废会话，
    不会再出现「同一会话 ID 永久卡死」。
  - 同一会话的并发请求排队最多 30 秒，超时返回 **409**；手动终止请求时排队立即结束。
  - `reset_session: true` 强制新建。
- **TeleAgent 人工确认**（permission / question）：**默认关闭，与旧版一致**（立即中断本轮并返回 502）。
  开启方式任选其一：
  - 设置项 `teleagentManualConfirmation: true`；
  - 环境变量 `AGENT2API_TELEAGENT_MANUAL_CONFIRM=1`；
  - 单个请求 `metadata.teleagent_manual_confirmation: true`。
  开启后待确认项进入受保护队列，在 `/api/teleagent/confirmations/view` 页面或
  `GET /api/teleagent/confirmations`、`POST /api/teleagent/confirmations/reply` 处理。
  等待期间流上每 15 秒发一条 SSE 注释（`: waiting for teleagent confirmation`）保活；
  最长等 10 分钟。确认接口拒绝跨源浏览器请求（`Origin` 与 `Host` 不一致）。
- **TeleAgent 工具调用**：开启 tools 时普通文本照常流式输出，只有 `[TOOL_CALL]` 之后的内容缓冲；
  历史里 assistant 的 `tool_calls`（带 id）和 tool 消息的 `tool_call_id` / `name` 会还原给上游；
  `tool_choice` 支持 `none` / `auto` / `required` / 指定函数（逐轮提示）。返回的 tool call id 跨轮唯一。
- **用量上报**：两家流式和非流式都会上报 usage（最后一个流式 chunk 也带 `usage`）。
  TeleAgent 读取 `step-finish.tokens`（input / output / reasoning / cache.read），读不到时估算。
- **超时**：TeleAgent 事件流不再有 10 分钟的总时长上限；建连 30 秒，之后按空闲判定
  （设置里的流空闲超时；等人工确认时放宽到 10 分钟）。上游安静时每 15 秒发 SSE 注释保活。
- 管理接口 `GET /api/provider-conversations`、`POST /api/provider-conversations/reset`
  （只清空闲会话；拒绝跨源浏览器请求）。目前没有面板入口。
