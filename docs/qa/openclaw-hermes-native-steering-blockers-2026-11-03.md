# OpenClaw / Hermes：历史阻塞与 owned bootstrap 适配状态

更新日期：2026-11-03。初轮调查发现原 ACP 缺少精确当前轮和逐条消费回执；续作已通过 Codeg 自带启动桥、Gateway 插件及原生消费 hook 补齐。**此前“必须先构建上游源码补丁”的结论已被替代**，本文保留原接口不能直接复用的依据，并记录当前实现和交付边界。

## 当前结论

普通消息排队继续复用 Web 队列。OpenClaw / Hermes 均已有受版本与实际 readiness 门控的代码适配，保留原聊天传输、原会话和执行中的 A；没有修改上游仓库、升级依赖或另起第二个 Agent。

- **OpenClaw**：Codeg 自带 ACP 内存 load-hook 与原 Gateway 插件。原 ACP agent/client 提供精确 runId；插件在原 Gateway 的原 A 对象与 authority 内调用 guarded V2 消费接口。**后续仍须让原 Gateway 受控加载该插件**，本轮未改实例配置、未加载插件或重启 Gateway。插件缺失时保留普通 ACP，readiness=false。详见 [OpenClaw 实现与验证](./openclaw-owned-native-steering-2026-11-03.md)。
- **Hermes**：Codeg 自带 Python bootstrap，在原 ACP worker 的同一 state.agent 上绑定 hostTurnId 与真实 _current_turn_id；工具后与 pre-API 原生边界逐条消费，结束/取消撤回未消费项。npm 包装器会删除 PYTHONPATH，已由精确版本 Node load-hook 在其原 Python ACP spawn 环境处仅恢复 owned 目录。详见 [Hermes 实现与验证](./hermes-native-steering-bootstrap-2026-11-03.md)。
- Web 复用已有 scoped 三端点与队列状态；subscription-service、数据库 schema 不需要本方案变更。代码准备、本地验证、Gateway 加载、发布部署和真实实例验收分别记录，不能互相替代。

## 固定版本与源码证据

| 对象 | 实际启用依据 | 边界 |
|---|---|---|
| OpenClaw | 固定已发布 npm `openclaw@2026.9.4` 的三个完整 bundle SHA、guarded V2 原生函数及其实际 export aliases；发布包 ACP SDK 1.4.0 的真实内存流验证 | 内部接口不是稳定公共 API；版本、bundle、backend 或插件不匹配即关闭 |
| Hermes runtime | 官方 `v2026.9.14` / peeled commit `345cd2b057a452236de401d3534b8502a7465e8d`；CLI 声明 0.21.3 / release date 2026.9.14；14 个源码文件精确 SHA | 在目标模块执行前读文件校验，不导入未知 runtime 探测版本 |
| Hermes Python ACP SDK | 官方 `agent-client-protocol==0.9.0` wheel 的 uv.lock SHA；router、agent router、agent connection 三文件 SHA 与真实函数签名 | 不推测其它 SDK 版本；未知源码不安装扩展 |
| Hermes npm 包装器 | 社区 `hermes-agent@0.21.3` 原 tarball 的 SHA1/SHA512、三个包装文件 SHA；registry ECDSA 两份签名均 verified=true | registry 内容签名不是 Nous 对社区包装包的发布署名；不执行 postinstall |

实际启动采用解析器找到的已安装命令；registry 的安装 pin 不等于真实实例已经安装该版本。早期 main/raw 调查快照只用于定位问题；Hermes raw 缓存曾与固定 git commit 不同，最终 manifest 和隔离函数测试以固定 git commit 为准。完整 SHA、官方合同链接和许可证记录于上述专属报告及随包审计 fixture。

## 历史问题：旧版为什么能用、新版为什么不能直接复用

### OpenClaw 原 Gateway 接口

MyClaw 旧入口 `web/src/app/api/chat/steer/route.ts` 调 `chat.send({sessionKey,idempotencyKey,queueMode:"steer",...})`，随后用 `chat.history` 输入回执、`chat.message.get` 与 run provenance 判断去向。旧版本闸门为 `web/src/lib/openclaw-version.ts` 的最低 2026.8.2。

该接口允许晚到输入变为 followup；POST ACK 不等于本轮消费。当前官方文档也区分接收、消费与晚到 followup。[官方队列合同](https://docs.openclaw.ai/concepts/queue-steering)

新类型的普通 `session/prompt` 进入 `openclaw acp`；固定版本忙时会先取消旧工作，另生成私有 Gateway runId。第二个普通 prompt 不能冒充 steer，单独 session key 也不能证明 A 仍是当前 native run。[固定版 ACP prompt 源码](https://github.com/openclaw/openclaw/blob/v2026.9.4/src/acp/translator.prompt-stream.ts)、[官方 ACP 桥文档](https://docs.openclaw.ai/cli/acp)

现实现通过原 session store 的 setActiveRun 取得原 A 映射，再由原 Gateway 插件检查相同 entry、operation、embedded backend、native session 与 tool-authority fingerprint，直接调用已核 guarded V2 消费函数。没有绕回 chat.send / interrupt / next-turn 队列。

### Hermes 原 HTTP run 与 ACP slash

旧入口从 `chat_runs.metadata.hermes_run_id` 取旧 `/v1/runs` admission 的真实 run ID，调用 `/v1/runs/<id>/steer`。旧 2xx accepted 语义不能直接解释为逐条消费证明，也没有等价的 ACP run 映射。

固定版 ACP 的 `/steer` 只返回入队文案，闲置时会进入下一轮；原 `steer(text)` 仅将多个输入拼入字符串槽，缺少逐条 ID 与消费 ACK。因此现实现使用原进程内独立 v1 队列，在原生用户行追加点确认，保留 legacy 调用的原行为。[固定版 ACP 命令](https://github.com/NousResearch/hermes-agent/blob/345cd2b057a452236de401d3534b8502a7465e8d/acp_adapter/commands.py)、[原生控制](https://github.com/NousResearch/hermes-agent/blob/345cd2b057a452236de401d3534b8502a7465e8d/agent/interrupt_control.py)

已核实际消费点为固定 git 源码的 `agent/agent_runtime_helpers.py::apply_pending_steer_to_tool_results` 和 `agent/turn_iteration_prep.py::prepare_iteration`；原 finalizer 将 legacy leftover 转下一轮前，v1 已关闭并撤回未消费项。[工具批次源码](https://github.com/NousResearch/hermes-agent/blob/345cd2b057a452236de401d3534b8502a7465e8d/agent/agent_runtime_helpers.py)、[pre-API 源码](https://github.com/NousResearch/hermes-agent/blob/345cd2b057a452236de401d3534b8502a7465e8d/agent/turn_iteration_prep.py)

本轮没有迁移到当前 Hermes Gateway authority，也没有把其 session.steer 的 queued 解释为已消费。[官方当前 Gateway 合同](https://hermes-agent.nousresearch.com/docs/developer-guide/gateway-internals#shared-authority-busy-input-and-corrections)

## 已实现的 Codeg 自有 ACP 合同

这些名称属于 Codeg 扩展，不是宣称上游原版已实现同名 API：

| 位置 | 字段/语义 |
|---|---|
| initialize 顶层 _meta | `"ai.myclaw/nativeSteering": {version:1,activeOnly:true,receipts:"consumed"}` |
| 普通 session/prompt._meta | `"ai.myclaw/nativeTurn": {version:1,hostTurnId}` |
| notification `_ai.myclaw/nativeSteeringState` | `{version,sessionId,hostTurnId,nativeTurnId,ready}`，实际 native turn 变化即换 token |
| request `_ai.myclaw/steer` | `{version,sessionId,hostTurnId,expectedNativeTurnId,submissionId,prompt:[{type:"text",text}]}` |
| runtime 回执 | `{version,submissionId,hostTurnId,nativeTurnId,outcome}`；outcome 为 injected / not_injected / unconfirmed，RPC 可等待实际消费或 A 结束 |
| Web-facing Codeg 三端点 | 沿用 native_steering_state / native_steer / native_steer_result；进程 epoch、全局 submissionId 幂等与精确会话/Agent/turn 检查继续生效 |

旧 submit_session_feedback、旧 native flag 和 MCP pull 不参与新能力判定。新 capability、实际 active readiness、消费回执是三项独立证明；未注入只能在明确未消费时回普通待发，未知/断线/重启不可自动重发。

公共 Rust `acp/native_acp.rs` 与 connection reactor 保持 Cancel/Disconnect 可处理，injected 后记录 B 的 Codeg transcript；A 终态排空回执，ReceiptBatch 极端清算约 4 秒，超出历史关闭边界的回执保持未知，不把 B 追加到后继 C。旧方案中的上游新 RPC / 源码 patch 提案已由该自有桥与插件实现替代。

## 当前文件与服务影响

| 位置/服务 | 已实现内容 | 尚未执行的交付 |
|---|---|---|
| Codeg 公共 ACP | `acp/native_acp.rs`、`connection.rs`、`session_state.rs`；host/native 绑定、扩展请求、终态回执与转录 | Rust 68 项受限用例已通过；构建发布、实例更新未执行 |
| Codeg owned 资源 | `acp/owned_bootstrap.rs`、`openclaw_bootstrap.rs`，物化私有内容寻址目录；原安装目录保留 | 不修改上游源码或自动升级依赖 |
| OpenClaw ACP / Gateway | `resources/openclaw/acp-bootstrap.mjs`、`acp-bridge.mjs`、`plugin.mjs`、`native-gateway.mjs`、`compat.mjs` 及插件 manifest | 原 Gateway 必须后续加载插件；仅发布 Web/Codeg 或只加 ACP loader 不代表 Gateway 已可接收 |
| Hermes 原 ACP 进程 | `resources/hermes/sitecustomize.py`、`codeg_native_steering.py`、`node-bootstrap.mjs`；版本/hash/readiness 不符时原启动继续 | 真实已安装包、完整会话/模型/工具尚未实测 |
| MyClaw Web | 复用已有 scoped 三端点、队列与 pending/unknown 处理，附件完整保留 | Web 与 Codeg 的发布部署均未执行 |
| subscription-service / DB | 本方案不需要代码、schema 或 migration 变更 | 未操作数据库；不新增旧聊天镜像链路 |

初轮检查的 `web/scripts/claude55-runtime-patches/` 仍是 source-only 审查工具；migration 原生启动包装和旧 tarball 安装脚本也未改动。它们未提供通用 ACP patch 分发，不再作为本轮实现的依赖或阻断：当前采用 Codeg 自带资源物化与原进程加载。

## OpenClaw Gateway 的后续加载边界

本轮插件代码已准备，但没有在任何真实 Gateway 配置或进程中加载。后续经授权时须向**真正的 Gateway 主机**交付精确 owned 插件目录，在原配置合并 plugins.load.paths、entries enabled，以及已有 allowlist 中的插件 ID，保留既有 deny、权限和配置。原 Gateway 的加载/重启与 ACP 连接重建是后续实例变更，不能因为 ACP 与 Gateway 地址相同或本地文件存在而推定已完成。

这一步是加载 Codeg 自带插件，不要求本轮修改上游源码仓库。远端 Gateway 与 ACP 可在不同主机；不能把 ACP 本地目录当作远端已安装目录。未加载、未协商或版本不符时继续关闭 readiness。

## 已执行验证

- **Hermes：16 项 Python + 13 项 Node 通过**，其中 Node policy 7、真实 stdio 启动链 5、离线签名 1。真实官方 AST 函数、原 state.agent/executor、工具后/pre-API、generation 替换、取消/provider error、caller loss、50 次线程竞态、双 owned 缓存/用户 sitecustomize 均有覆盖。真实 stdio 链只启动假 Python console，不导入 Hermes/SQLite。
- **OpenClaw：12 项 Node 通过**，包括 10 项固定真实原生 V2 函数隔离用例，2 项已发布 bundle/SHA/真实 ACP SDK 内存流核验。不启动 OpenClaw server，不创建其 ledger。
- 根代理统一回归记录：Node **59/59**（OpenClaw 12 + Hermes 13 + DSH 27 + 执行器守卫 7），Python **16/16**；日志分别为 `/private/tmp/agent-queue-node-final-20261103.log`、`/private/tmp/agent-queue-hermes-python-final-20261103.log`。
- 最新 Rust **68/68 通过**、4343 项 filtered out、exit 0。测试库由主代理审查白名单/指纹后的受限执行器最新重新编译，没有执行全量测试。日志：`/private/tmp/agent-queue-rust-final-20261103.log`。此前 52 项记录保留为历史，不代替本次 68 项结果。
- 服务器最终 `cargo check` **exit 0**，28.56 秒；日志：`/private/tmp/agent-queue-codeg-check-final-20261103.log`。Web 调用方独立回归 **19/19**，不计入 Codeg 用例。
- 没有执行全量数据库测试、真实模型、实例升级、配置加载、Gateway 重启、部署、提交、push 或合并。真实实例验收仍为 0/6，不能把本地协议/启动链测试称为生产全通过。
