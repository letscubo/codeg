# OpenClaw 工具准备期间的“调整方向”

日期：2026-11-04。本文记录准备期 steering 和 client-message proof 修复。此前经用户授权，已将固定源码快照安装到唯一指定预览实例；本次另获授权在 `codex/agent-queue-steering` 功能工作区 checkpoint、合入最新 `origin/main` 并验证。当前操作不发版、不部署、不操作实例、不启动真实 Agent 或模型，也不创建任何数据库。

## 行为约定

- Web 本地 A 在配置解析／发送准备阶段就获得独立 signal 和 UUID。点击 B 后先等待同一 A 的首次 `session_assigned`，再核对 Codeg 状态中的 `clientMessageId`，不把后继 A 或旧连接当作原 A。
- 原 A 已被 Codeg 受理，但 OpenClaw/Gateway 工具准备尚未完成时，B 可以先提交到 Codeg，立即取得原 submission ID 的 `pending` 回执。Codeg 在同一准入锁内建立 host 与 client-message proof，不依赖 ACP 循环先取出 A。
- 准备期后台等待预算为 120 秒，从成功捕获 admission 起计算，包含后续等待就绪、连接锁、状态锁和命令通道容量的时间；不包含此前获取内存锁的时间。超时且未派发时给出明确未注入回执。
- 后台等待绑定的原 A 就绪，页面继续查询同一 submission ID，不保持一个等待工具初始化的 HTTP 请求。
- 准备期请求固定进程 epoch、原连接对象、conversation、Agent、ACP session、host turn 和轮次代数。Codeg 只向原连接的命令通道派发一次。
- Gateway 在原 operation 创建时观察实际 backend attachment，提供真实首个 backend 的证明。状态轮询遗漏某个短暂 backend 不能让后续 backend 被误当首个。
- 仅明确协商准备期能力的 owned OpenClaw 开放等待。普通已就绪 steer 沿用原来的精确 native turn 与消费回执校验；其余 Agent 的准备期行为不扩展。
- A 结束、取消、换轮、会话或连接变更，以及等待超时，若尚未派发 B，返回 `not_injected`。已经可能派发或进程重启时保持 `unconfirmed`，不自动重发。
- Web 复用原 `ChatQueueBar`。`pending` / `unconfirmed` 保持消息归属和操作锁；明确未注入的 B 留待用户操作，不自动作为下一轮普通消息发送。

## 取证与范围

此前测试会话 `codeg:209` 的工具准备阶段耗时 107423 ms，其中 `tool-search` 计时段为 107217 ms。这个计时段包含工具 runtime 获取、目录发现和整理，不能将其当作单个 MCP 请求耗时。

本次修复准备期消息接收与派发，不修改实例里的工具地址、MCP 超时配置或 OpenClaw 安装，不把本地假传输验证当作真实模型验收。

## 验证边界

Rust 仅执行重新审查后列入 `scripts/no-db-tests.json` 的精确测试名称。清单指纹在生产源码和测试调用链冻结后更新。其余 Rust 测试仅编译。

Node 验证使用内存 operation/backend、固定发布版源码片段、假 ACP/Gateway 和私有临时文件，不导入 OpenClaw 运行时入口，不连接数据库或模型。Web 浏览器验证使用独立 headless 浏览器和假传输，不操作用户的浏览器。

## 执行结果

| 验证 | 结果 |
| --- | --- |
| `codeg-server` / `codeg-mcp` 无默认 feature 编译检查 | 通过；保留原有库警告 |
| server / lib 测试编译（`--no-run`） | 通过；未运行数据库测试 |
| 精确 Rust 无数据库清单 | **93/93**；既有 78 项、准备期 12 项、client-message proof 2 项、同 host begin 1 项；4343 项其他测试未执行 |
| OpenClaw Node 回归、固定发布包核对和测试执行器守卫 | **57/57**，0 skip |
| Rust 改动文件格式与差异检查 | 通过 |
| Web 纯队列／独立 headless 浏览器 | **33/33** |
| Web API／连接归属／准入 | **16/16** |
| Web 根类型检查及改动文件 ESLint | 通过 |

已安装源码快照的 Rust 无数据库指纹：`8b335f9323e4ea42d07be882c143ad0f4aadb40a0f99262d379141b1164d41d3`。此前准备期单独验证的 90 项指纹为 `b68b1e37261cec072286dc429873b1bd503c35a27c0d722d5f314a7fdc74de4c`；两次记录不得混为同一版本。

独立审查逐项核对了原连接与原 A 的绑定、取消早于终态、轮询漏掉短暂首个 backend、替换同名连接、命令通道背压、等待超时、重复提交身份及未知回执。无数据库用例只通过已重新审查的精确清单执行，没有直接执行全量 `cargo test`。

固定发布版源码从本机已有 npm 缓存复制到独立审计目录，先核对完整原 tarball SHA-512，再核对所需源码 SHA-256。没有安装依赖、修改共享发布包或执行 OpenClaw 入口。

日志：

- [93 项 Rust 无数据库测试](/private/tmp/steer-client-proof-no-db-20261104.log)
- [此前 90 项准备期测试](/private/tmp/steer-preparing-codeg-no-db-20261104.log)
- [Node 与发布包核对](/private/tmp/steer-preparing-codeg-node-20261104.log)
- [测试编译](/private/tmp/steer-preparing-cargo-test-compile-20261104.log)
- [client-message proof 最终编译检查](/private/tmp/steer-client-proof-cargo-check-final-20261104.log)
- [固定发布包审计来源](/private/tmp/codeg-preparing-openclaw-audit-20261104/restoration-audit.json)

## 已完成的单实例交付

`client-proof-8b335f93` 是打包标签，不是 Git commit。固定源码由真实基线 `ab88c6c12e1af4cccac7e09c96398d44d41e9022` 加 18 个已审查工作区文件组成；源码快照文件 SHA-256 为 `0aeacef48c96c3cf6d9c5d41b5699b29e978451a0be02815399d4cc3f617ef4a`，最终源码 manifest SHA-256 为 `886ab804acefe901c7b46ec423a32e753fa3239ffa533f2d8f26b17e2ba94e8f`。

仅目标 VM `716b08b2-a0d2-445f-814e-52258edb0120` 的本地安装回执记录为 `installed`：server 与实际 worker executable SHA-256 均为 `f91602d4239c1356d35f467c316a34262c7cf607cbcdb9b80c67f103a96aeda0`，MCP SHA-256 为 `40fa95c2e8d8b76b064cec08821c9f93385edc58025569f042be54ae9e4c2a35`。复用 8 项资源的精确 owned 目录，保留 live DB、环境文件和配置字节；本次 checkpoint 前再次逐文件核对，18 项差异均与打包源码相同。

该安装回执证明固定包和服务加载情况，不等于六类 Agent 的真实模型测试全部通过。记录来源：[固定包核验](/private/tmp/codeg-client-proof-8b335f93-linux-build/candidate-package-verification.json)、[源码快照](/private/tmp/codeg-client-proof-8b335f93-linux-build/source-snapshot.json)、[安装回执](/private/tmp/codeg-preview-installed-client-proof-8b335f93-716b08b2.json)。后续 main 整合的验证另行记录，不复用旧指纹冒充合并版本结果。
