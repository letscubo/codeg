# Slack 独立平台回调

MyClaw 使用 `SLACK_WEB_ORIGIN` 选择 Slack 平台地址，codeg 通过已认证的 HTTP API 接收固定目标。无需新增数据库表或修改 Telegram/其他平台的全局 webhook。

## 协议

- `POST /api/get_relay_capabilities` 返回 `channelCallbackVersion: 1`。
- `POST /api/register_channel_callback` 接收 `{ "channelType": "slack", "origin": "https://preview.example" }`，返回 `{ "targetId": "<64位小写SHA-256>" }`。仅接受无路径、查询、片段及用户凭据的 HTTPS origin。
- `channel_submit.callbackTargetId` 固定这次操作的 `resolve`、`dispatch`、`status`、`complete` 回调目标。操作持久化及重启恢复沿用同一 ID；回调 body 同样包含该字段。
- `acp_prompt.callbackTargetId` / `cli_prompt.callbackTargetId` 固定该轮 `turn_complete` / `error` 目标，包括无 outbound 的隐藏群成员。每轮按 connection、事件序号及进程身份隔离，终态关闭边界；后续普通会话不会继承。
- Slack `outbound.resultCallbackTargetId` 固定投递回执目标，须为已登记 Slack ID，要求 `deliveryVersion >= 2`，与 `resultWebhookUrl` 互斥；同时传顶层 ID 时两者必须一致。

独立回调不注册为全局订阅。该轮平台终态送到指定 `/api/codeg/events`；原普通通知 hook 保留，全局通知筛选不屏蔽独立操作终态。独立 `turn_complete` 带原始 `stop_reason`，包括取消/失败结果。

固定目标只在现有 SQLite `app_metadata` 保存 channel、origin、VM 身份；secret 仍读取原平台凭据，不复制。操作与 outbox 保存目标 ID，重试不随当前环境变化。回调前校验目标属于当前 VM，验证失败不回退到其他地址，独立 HTTP 请求禁止重定向。

未传新字段的旧请求继续旧路径，历史 SQLite 记录兼容读取。先升级 codeg，再部署使用新能力的 MyClaw；MyClaw 必须检查能力版本，旧 codeg 不得静默回退导致跨环境处理。独立目标注册不修改公共 `chat_event_webhooks`。
