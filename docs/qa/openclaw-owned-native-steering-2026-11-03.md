# OpenClaw：Codeg 自带 Gateway 插件与 ACP 严格转向桥

日期：2026-11-03。代码准备于功能分支，未提交、合并、部署、升级实例、改实例配置或重启 Gateway。没有启动 OpenClaw 运行时、真实模型或数据库。本报告不把本地无数据库协议验证等同于真实 Gateway/模型验收。

## 这次实际补了什么

保留原 `openclaw acp`、原 ACP agent、原 Gateway client、原 Gateway 进程中的 A。Codeg 自带两份协作资源：

1. ACP 子进程通过 Node `--import` 加载 `acp-bootstrap.mjs`，对已核固定 bundle 只做内存 load-hook，在原 `new AcpGatewayAgent` 后安装 `acp-bridge.mjs`。它 hook 原 session store 的 `setActiveRun`，获得原 A 的 ACP session → Gateway runId，不查最近会话，不另建 client。
2. `plugin.mjs` 在原 Gateway 内注册三项 Codeg 专属 RPC，`native-gateway.mjs` 取得原 A 的 reply operation 与 embedded backend，调用已有 guarded V2 原生消费函数。它没有调用 `chat.send`、普通 prompt、interrupt、abort 或 next-turn 队列。

只在插件可协商、精确 A 当前可接收时发布能力/readiness。未加载 Gateway 插件、版本不符、bundle 变更、缺 Node hook、非已审 embedded backend 时保留原 ACP，能力关闭。

新增 Rust `acp/openclaw_bootstrap.rs` 只物化 Codeg 自己的资源并准备 ACP 子进程环境，复用共享 `owned_bootstrap::materialize_owned_resources` 的内容寻址目录。它不修改 OpenClaw 安装或实例配置。`NODE_OPTIONS` 原值被保留；`CODEG_OPENCLAW_NATIVE_BOOTSTRAP=off` 可关闭本桥。

公共 Rust ACP 桥与 Npx 子进程加载入口现已接线。整体为 Codex、Pi、Claude Code、DeepSeek、OpenClaw、Hermes **6/6 代码接入，0/6 用户实例验收**；OpenClaw 是否可用仍取决于原 Gateway 后续加载本插件。Hermes 的 npm wrapper 清除 PYTHONPATH 问题已由其独立 Codeg Node 启动垫片解决，见 [Hermes 专项记录](./hermes-native-steering-bootstrap-2026-11-03.md)。这两项交付边界不能混为“仅升级 Web 即可”。

## 版本与官方依据

固定本地已发布 npm 包为：

```
/private/tmp/myclaw-openclaw-multiversion-20261030/v2026.9.4/node_modules/openclaw
OpenClaw: 2026.9.4
其发布包中的 @agentclientprotocol/sdk: 1.4.0
```

没有安装、升级或执行该 OpenClaw 包。只读其源码；真实 SDK 测试只导入 SDK 的纯 schema/JSON-RPC/stream 实现。

| 已核文件 | SHA256 |
| --- | --- |
| `dist/server-DWybsgYT.mjs` | `96113ca44674714f6157b040ecd956f94c226bd983f62ff342ad9ef78963d020` |
| `dist/reply-run-registry.registry-DDgx8cNZ.mjs` | `3d53a6271de56728e8b70c7d7cc1989adc52d6fb2033a7a838c2633e09615f09` |
| `dist/builtin-openclaw-B-H-7lKk.mjs` | `0a8c813e535c92d03f69bc58381518ba0e6ac6e46f3adda54138c5f668340ea8` |

这些内部模块不是稳定公共 API；实现按版本和完整文件 SHA 验证，不猜 export。`compat.mjs` 已核实际 export aliases：registry `h`、upstream signal→operation WeakMap `V`、attached backend getter `A`。

官方公共接口依据：[插件 Gateway 注册](https://docs.openclaw.ai/plugins/sdk-overview/infrastructure)、[本地插件加载和 allowlist](https://docs.openclaw.ai/tools/plugin)、[ACP 扩展合同](https://agentclientprotocol.com/protocol/v1/extensibility)、[Node 24 registerHooks](https://nodejs.org/docs/latest-v24.x/api/module.html#moduleregisterhooksoptions)。固定包 `dist/agent-harness-runtime-CZb40n5o.d.ts:15054` 核对了 `registerGatewayMethod(method,handler,{scope?,profileAccess?})`；本桥使用 `operator.write` 且保留默认 profile 检查，不新增认证 key。

## 为什么不复用旧 chat.send

固定版 `dist/sessions-rlbLHLas.mjs:1749-1781` 的 `chat.send` 没有 expected native run 参数；`chat-send-handler-oS9rX200.mjs:3951` 在远端选择当前 injection target，`:2880-2945` 注入失败后进入普通 dispatch/followup。`sessions.steer` 在 `sessions-messaging-BlF7fVjt.mjs:194-204` 实际指定 interrupt。`talk.client.steer` 也没有 expected run 参数。只在 ACP 一侧预检运行状态，再调用这些方法，无法消除 A 恰好结束的竞态。

ACP 当前内部类并不导出：`server-DWybsgYT.mjs:2479` 为 `AcpGatewayAgent`，`:2885` 只 export `serveAcpGateway`。因此本桥对固定 SHA 的原启动回调插入 hook，而非假设可 import 私有类。原 `serveAcpGateway:2744` 会创建 SQLite ACP replay ledger，本次没有导入/执行整个 server。

## 精确身份、原 authority 与消费证明

Gateway 插件每次都验证：

- `context.chatAbortControllers.get(原ACP生成的Gateway runId)` 仍是原 entry，且 `ownerConnId === 本请求client.connId`，sessionKey 精确相同、signal 未取消。
- 原 entry 的 signal 在真实 `operationsByUpstreamAbortSignal` 中取得原 operation，必须等于真实 registry 的当前同 key 对象，并匹配原 native sessionId、running phase。
- 原 attached backend 必须 `kind=embedded`、`messageInjectionV2.version=2`、`supportsTranscriptCommitWait=true`。拒绝 legacy queueMessage 和其它未审 backend。
- native token 包含 Gateway run、native session、backend run、entry/operation/backend 对象 nonce 和原 tool-authority fingerprint。任何换绑或权限 fingerprint 变化都使旧 token 失效。注入函数内部继续检查同对象与原 runtime authority。

固定真实原生函数位于 `builtin-openclaw-B-H-7lKk.mjs:12611-12779,13217-13245`：第三参数为 `assertCurrent` 断言回调（不是 boolean predicate），原 `composeInjectionGuard` 调用它后返回原 `canInject()`。本桥传递该断言，保留原 runtime registration/tool authority，不构造新 overlay。参数 `isInboundUserMessage=false` 用于绕开 pending ask_user claim 的独立 ACK 捷径；实际仍调用原 runtime 的 `steer`，产生用户输入行。

`waitForTranscriptCommit=true`、独立 `queueIdentity`，只在匹配该 identity 的用户 `message_end` 后成功。queued/accepted 不算消费。原函数在 settle、handoff、timeout 时按 identity 移除未消费队列项。本桥对调用之后的任何错误一律保留 unconfirmed，即使原 handoff 分支允许 replay，也不自动重发。

原生等待预算沿用固定版默认 120000 ms（`:12604`）；ACP 使用同 Gateway client 的无限 request timeout，避免长工具期间先丢弃回执。Codeg 的外层请求可保持 pending 并查账本，Cancel 继续由 Rust select 处理。

submissionId 在 Gateway 插件内先占账本再 await；同 scope/正文重复取同结果，冲突保持 unconfirmed，满额拒绝且不淘汰旧 ID。Codeg 的外层 epoch/幂等账本继续负责跨 connection 重试边界。

ACP prompt 的 `_meta["ai.myclaw/nativeTurn"]` 绑定 hostTurnId；原 `setActiveRun` 提供真实 Gateway run。原 A 完成/断线/shutdown 时先发带原 nativeTurnId 的 ready=false，再清理本地 token，不把 B 当成另一普通 prompt。B 的 Codeg transcript 由公共 Rust 桥在 injected 后记录，原 runtime 已写其用户行；ACP 包装层不另发 user chunk，避免双份。

首次原生状态立即检查，后续活跃会话默认间隔为 500 ms；提交时仍以 Gateway 的严格同对象 guard 为准，不靠轮询时间保证同轮。轮询不承担普通消息的后台执行或持久化。

## 已执行的无数据库验证

`native-steering.test.mjs` 10 项通过：精确 identity 消费与 B 转录、旧 owner/run/session/authority 拒绝、真实 composed guard 竞态、settle/handoff/timeout 撤回且 unknown、同文不同 ID/重复冲突、ACK-shaped 假结果拒绝、A 结束/断线/provider error/账本满、缺插件普通 ACP、原 A 的 promise/工具/usage/终态保留，以及 ACP 丢回执保持未知、stale/结束后不写入、通知异常不能覆盖 A 终态。

该测试执行 `audited-consumer-fixture.json` 内固定发布版的真实 V2 `queueMessage`、`composeInjectionGuard`、`steerAndWaitForTranscriptCommit` 源码。其依赖是内存 fake activeSession、事件订阅、队列、transcript 数组和计时器，无 OpenClaw 初始化、provider、网络或数据库。

`published-package.audit.test.mjs` 2 项通过：三个完整 bundle SHA、上述原生片段与发布包逐字一致；发布版 ACP SDK 1.4 的真实 AgentSideConnection/ClientSideConnection 内存流往返。后者确认 wire `"_ai.myclaw/steer"` 与 `"_ai.myclaw/nativeSteeringState"` 不被 SDK 添加或剥离下划线，不只直接调用 fake extMethod。

```bash
node --test src-tauri/resources/openclaw/native-steering.test.mjs
CODEG_OPENCLAW_AUDIT_PACKAGE=/path/to/already-published/openclaw \
  node --test src-tauri/resources/openclaw/published-package.audit.test.mjs
```

本机执行使用 `/Users/cooper/.nvm/versions/node/v24.15.0/bin/node`，只运行上述无运行时初始化的代码，不启动要求 Node 24.16+ 的 OpenClaw。本轮主代理统一 Node 回归 59/59、Hermes Python 16/16、Web API/队列/精确归属 19/19 及最终 Web 非增量 tsc 已通过；OpenClaw 的上述 12 项包含在 Node 59 项内，不能重复相加。

新增公共 ACP/owned bootstrap 后，已审查无数据库 Rust 测试最终 **68/68 通过**，exit 0、4343 项 filtered out，日志 `/private/tmp/agent-queue-rust-final-20261103.log`。最终 server `cargo check --no-default-features --bin codeg-server --locked` 通过，exit 0、28.56 秒，保留 3 项既有库警告；日志 `/private/tmp/agent-queue-codeg-check-final-20261103.log`。没有执行全量 cargo test，不代表数据库集成用例、真实 OpenClaw Gateway 或模型验收通过。

## 尚未执行的交付步骤与局限

必须先让原 Gateway 受控加载 Codeg 物化目录中的插件，再重建使用此 Codeg 的 ACP 连接。仅发布 Web、仅更新 Codeg 或仅加 ACP loader 都不能让已运行的 Gateway 获得严格 RPC。

后续经授权加载时，应在原 Gateway 配置合并（不能覆盖已有项）：`plugins.load.paths` 加入精确 owned 目录，`plugins.entries.codeg-native-steering.enabled=true`，存在 `plugins.allow` 时加入该 ID，并保留既有 deny/权限政策。该原 Gateway 的重新加载/重启属于之后的实例变更，本次没有执行。ACP 与 Gateway 可以在不同主机，插件文件必须交付到真正的 Gateway 主机，不能误把 ACP 本地目录当作远端目录。

尚未验证：真实 Gateway 的插件发现/加载、完整启动 load-hook、真实用户 authority/provider/tool 的端到端、不同版本/standalone 包。固定 bundle 不符会关闭本桥，不升级上游依赖。真实模型、数据库及实例验证需另行授权并遵守数据库限制。

Web 普通待发队列仍只在当前页面内存，刷新或关闭会丢失未发送项；本轮没有增加持久化、跨设备或离线后台执行。运行时的消费回执与 Codeg 进程账本不能升级解释为 Web 队列已持久化。
