# Codeg 原生转向 v1：实现与本地验证记录

日期：2026-11-03（Asia/Shanghai）

后续已在本功能分支合入 main 的每轮推理设置，完成 78 项 Rust、69 项 Node、16 项 Python 无数据库回归；冲突处理和首次失败修复见 [main 合入记录](./main-sync-2026-11-03.md)。以下 68 项为前轮实现验收快照。

工作区：`/Users/cooper/Desktop/work/MyClaw/.worktrees/agent-queue-steering-codeg`
分支：`codex/agent-queue-steering`
基线：`origin/main` 本地 ref `d60226fd`。

## 服务合同

全部端点复用 Codeg 已有 Bearer 鉴权，均为 POST `/api/<name>`。health 新增 `native_steering_v1` 表示合同存在，不表示每个运行时均支持。

- `native_steering_state`：输入 `{connectionId}`；返回 `{serverEpoch,conversationId,agentType,nativeSupported,activeTurnId,attachmentsSupported}`。`activeTurnId` 是服务端 opaque token，客户端不能自行生成。
- `native_steer`：输入 `{connectionId,expectedConversationId,expectedAgentType,serverEpoch,expectedTurnId,submissionId,text,blocks?}`；返回 `{submissionId,state,reason}`。
- `native_steer_result`：输入 `{connectionId,expectedConversationId,expectedAgentType,serverEpoch,submissionId}`；返回同一结果形状。

`state` 为 `pending`、`injected`、`not_injected`、`unconfirmed`。只允许明确的 `not_injected` 回到普通待发；网络错误、查询缺失、进程重启、未知错误或提交 ID 冲突均不能自动重发。账本在进程内按全局 `submissionId` 去重；相同 ID 换 connection、会话或正文会拒绝，不能再次注入。进程 epoch 防止重启后遗忘去重记录。账本最多保留 8192 项，不淘汰已接受 ID；达到上限拒绝新提交，不能静默丢弃去重记录。

提交 admission 与 driver 执行前分别核对作用域和轮次；HTTP 调用者离开后，已接受的提交仍完成派发与结果记录。第一版仅接受纯文本，提供 blocks 时其文本必须与 text 一致；附件完整留在平台待发项，`attachmentsSupported=false`。

## 运行时边界

- Codex：在现有 app-server JSONL pump 接 `turn/steer`，使用实际 thread ID 与 expected native turn ID；确认响应的 turnId 后才记为 injected，并把 B 写入 Codeg transcript。原 turn 保持不变，不发送新 turn/start 或 turn/interrupt。若 turn/completed 先于 steer ACK，最多继续读取 2 秒，保留能确认的回执与 B；未知结果不重发。
- Pi：实际稳定版本至少 1.3.0 才使用其 JSONL RPC；保持现有 CLI transport、每轮进程和会话续接结构。只有观察到初始 prompt 被该进程实际消费后才开放 readiness。steer 的 queued ACK 只代表入队；匹配的 user message 消费事件才确认 injected。轮结束竞态通过已确认的 clear_queue 结果证明未消费；无法证明的保持 unconfirmed。旧版或未知版本继续原 print 路径并保持 readiness=false。
- Claude Code：续作已接入原 CLI stream-json；实际稳定 2.x ≥2.1.287、init capability/session 与初始 A UUID 均核对后才开放。B 使用 next + shouldQuery:false，不自启下一轮；同 session 的结果/回复同时含 A+B UUID 才确认消费。晚到只按 B UUID 撤回 pending queue，明确 cancelled=true 才未消费；原任务不被取消。详见 [Claude 专属记录](./claude-native-steering-2026-11-03.md)。
- DeepSeek：续作扩展 Codeg 已有自带 headless runner，以公开 inject(next-step,false) 进入原 Agent；五个实际包版本严格为 0.1.6-alpha.2 才开放，stock/未知版本关闭。每个真实 native turn 换 token，只有同轮精确 user/message append 确认；owner hooks 精确清理，pre-step 阻止旧输入跨轮，不在 Session.append 回调里写 inbox。详见 [DeepSeek 专属记录](./deepseek-native-steering-2026-11-03.md)。
- OpenClaw：已补 Codeg 自带 ACP 内存 load-hook 和原 Gateway 插件。ACP 保留原 agent、Gateway client 与 A 的 runId；插件在原 Gateway 中核对 entry/operation/backend/authority，调用固定版 guarded V2 原生消费函数，仅精确用户行提交才 injected。**原 Gateway 后续仍须受控加载插件，本轮没有改实例配置、加载插件或重启 Gateway**；未加载时 readiness=false、普通排队保留。详见 [OpenClaw 专属记录](./openclaw-owned-native-steering-2026-11-03.md)。
- Hermes：已补 Codeg 自带 Python bootstrap，绑定原 ACP worker 的同一 state.agent 与真实 _current_turn_id，在工具后/pre-API 原生边界逐条消费 B，结束/取消时撤回未消费项。固定 npm 包的 Node load-hook 仅在原 Python ACP spawn 恢复私有目录，原 venv/argv/stdio 保持。14 个 Hermes 文件、3 个 SDK 文件及 npm 包装文件均核对 SHA；未知版本/源码保持普通 ACP、关闭能力。详见 [Hermes 专属记录](./hermes-native-steering-bootstrap-2026-11-03.md)。
- 公共 ACP ReceiptBatch 在 A 终态等待未决回执后关闭历史写入；极端清算约 4 秒。迟于历史关闭的结果保持 unconfirmed，不把 B 写到后继 C，等待期间原 reactor 继续处理取消。
- [旧阻塞记录](./openclaw-hermes-native-steering-blockers-2026-11-03.md) 已标明被上述 owned bootstrap 实现替代；本轮不需要修改上游仓库或升级依赖。代码适配完成与 Gateway 插件加载、发布部署、真实实例验收分别记录。
- 旧 submit_session_feedback、旧 ACP native flag 与 MCP pull 的业务路径保持原样；新 RPC 仅读取新的 v1 driver readiness，不沿用旧 flag。

官方 Codex 合同：<https://learn.chatgpt.com/docs/app-server#steer-an-active-turn>。Pi 固定版官方合同及源码核对入口记录于 `src/acp/cli/pi_rpc.rs` 文件头。

## 变更文件

- 新合同、账本、状态检查：`src-tauri/src/acp/steering.rs`。
- HTTP 路由与能力注册：`src-tauri/src/web/handlers/steering.rs`、`handlers/mod.rs`、`web/router.rs`。
- 共享内存状态与命令：`acp/manager.rs`、`acp/session_state.rs`、`acp/connection.rs`、`acp/mod.rs`。
- Codex bridge 与协议级测试：`acp/cli/codex_driver.rs`、`codex_steering_tests.rs`。
- Pi bridge、回执与协议级测试：`acp/cli/pi_driver.rs`、`pi_rpc.rs`、`pi_driver_rpc_tests.rs`、`cli/mod.rs`。
- Claude bridge 与协议级测试：`acp/cli/driver.rs`、`claude_steering.rs`、`claude_steering_tests.rs`。
- DeepSeek bridge 与协议级测试：`acp/cli/dsh_driver.rs`、`dsh_steering.rs`、`dsh_steering_tests.rs`、`resources/dsh/codeg-headless-runner.mjs`、`codeg-native-steering.test.mjs`。
- ACP 自有扩展、原生轮次绑定和终态回执排空：`acp/native_acp.rs`、`native_acp_tests.rs`、`native_acp_transport_tests.rs`；共享物化与启动环境：`acp/owned_bootstrap.rs`、`owned_bootstrap_tests.rs`。
- OpenClaw 自带插件、ACP 桥与精确版本核验：`acp/openclaw_bootstrap.rs`、`resources/openclaw/`。
- Hermes 自带 Python/Node 启动桥与专属无数据库测试：`resources/hermes/`。
- 已审查无数据库测试清单、嵌入 runner 指纹与守卫：`scripts/no-db-tests.json`、`test-no-db.mjs`、`test-no-db.test.mjs`。

没有修改核心依赖版本、数据库 schema、默认 Agent 传输分配或旧反馈实现。六类运行时的能力均按已核版本/实际 readiness 开放，继续当前 CLI/ACP/runner 架构；Codeg 仓库本轮没有修改 Web UI。

## 本地验证结果（owned bootstrap 续作）

- `cargo check --no-default-features --bin codeg-server --locked`：主代理最终执行 **exit 0**，28.56 秒。日志：`/private/tmp/agent-queue-codeg-check-final-20261103.log`。
- `node scripts/test-no-db.mjs server`：主代理审查白名单/指纹后，最新重新编译测试库并执行 **68/68 通过**、4343 项 filtered out、exit 0。日志：`/private/tmp/agent-queue-rust-final-20261103.log`。没有执行全量 cargo test，数据库用例仍仅编译。
- Hermes：**16 项 Python + 13 项 Node 通过**。Node 分为 7 项精确包装器 policy、5 项真实 stdio 启动链路、1 项离线 registry 签名校验。同步 hook 与强制异步 loader fallback 均在本机 Node 24.15 通过；没有把它描述为真实 Node 20 或真实 Hermes/模型实测。
- OpenClaw：**12 项 Node 通过**，其中 10 项真实固定原生消费函数的隔离测试，2 项已发布 bundle/SHA 与真实 ACP SDK 内存流审计。没有导入/启动 OpenClaw server 或创建其 replay ledger。
- Hermes 的 stdio 测试真正执行固定 npm bin/launcher → 临时 venv 假 Python console → 生产 sitecustomize/core，仅装 Finder，未导入 Hermes、SessionManager 或 SQLite；保持原 stdio/argv/cwd/venv，ambient PYTHONPATH 仍被包装器移除。
- 主代理最终统一回归：Node **59/59**（OpenClaw 12 + Hermes 13 + DSH 27 + 执行器守卫 7）、Python **16/16**。日志：`/private/tmp/agent-queue-node-final-20261103.log`、`/private/tmp/agent-queue-hermes-python-final-20261103.log`；Web 调用方另有 19/19，通过范围不计入 Codeg 用例。
- `git diff --check`：本次文档收口检查通过。没有执行全量 cargo test、数据库用例、migration 或 seed。

### 前轮已完成结果（历史记录）

- `cargo check --no-default-features --bin codeg-server --locked`：通过。编译保留 deprecated/dead_code warnings，没有为清理它们扩大改动。
- `cargo test --no-default-features --bin codeg-server --lib --no-run --locked`：通过，仅编译，不运行数据库用例。最终库测试由受限执行器重新编译；测试链接器另有 unwind section 大小 warning，无 errors。
- `node scripts/test-no-db.mjs server`：前轮 **52/52 通过**，当时 4343 项未执行。首次 51/52 暴露 Claude 回退只 shutdown 未关闭 pipe 的兼容错误；改为实际 drop 后保留、增强原断言并完整重跑。清单逐条审查调用链，固定 Rust/依赖及嵌入生产 mjs 指纹，没有在该受限套件初始化 SQLite、执行 migration 或 seed。
- `node --test src-tauri/resources/dsh/codeg-headless-runner.test.mjs src-tauri/resources/dsh/codeg-native-steering.test.mjs scripts/test-no-db.test.mjs`：**34/34 通过**，其中 runner 27 项、执行器守卫 7 项。
- `git diff --check`：通过。

52 项组成：既有 Prompt 13 项；native 账本及六运行时/内部换轮保护 6 项；Codex 纯合同 1 项与生产 pump 假 app-server 4 项；Pi 回执/版本 5 项与生产 driver 假进程 6 项；Claude 9 项；DeepSeek helper 4 项与生产 driver 假进程 4 项。

Codex pump 测试使用内存 duplex IO 与假 app-server 响应，实际执行生产 pump；覆盖完整 turn→steer→finish、精确 turn ID、无新 turn/interrupt、B 的临时 transcript、明确 no-active-turn、未知错误、丢 ACK、terminal 先到。Pi driver 测试只运行临时 Python 假进程，覆盖工具/结束状态、精确轮次、旧版本回退、竞态清队列、provider 错误、会话恢复及 B 的 transcript。上述都是协议集成测试，不是真实 Agent/模型实测。

Claude/DeepSeek 新用例同样只用生产 driver + 本地 Python 假进程／内存对象／临时 transcript。独立复核发现并修正 Claude 背景终态提前结束、DeepSeek Session.append 重入撤回两处竞态；前轮回归覆盖修正。早期两次真实 Claude 隔离 loopback 观察未证实无内部 DB，不计入受限测试，边界如专属报告。续作后六类运行时均已有受版本门控的适配代码；**0/6 用户实例验收**，OpenClaw 原 Gateway 插件尚未加载。最新 Rust 68 项已按上节的受限范围通过。

## 复现

本机 Rust 工具链与已缓存依赖位于临时目录，不含凭证：

```sh
cd /Users/cooper/Desktop/work/MyClaw/.worktrees/agent-queue-steering-codeg
export CARGO_HOME=/private/tmp/myclaw-rust-tools/cargo
export RUSTUP_HOME=/private/tmp/myclaw-rust-tools/rustup
export PATH="/private/tmp/myclaw-rust-tools/rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"
cd src-tauri
cargo check --no-default-features --bin codeg-server --locked
cargo test --no-default-features --bin codeg-server --lib --no-run --locked
cd ..
node scripts/test-no-db.mjs server
node --test src-tauri/resources/dsh/codeg-headless-runner.test.mjs src-tauri/resources/dsh/codeg-native-steering.test.mjs scripts/test-no-db.test.mjs
python3 -B -m unittest discover -s src-tauri/resources/hermes -p test_native_steering.py
node --test src-tauri/resources/hermes/node-bootstrap.test.mjs src-tauri/resources/hermes/node-stdio-bootstrap.test.mjs
node --test src-tauri/resources/openclaw/native-steering.test.mjs
# 只指向已取得的已发布包，不安装或启动 OpenClaw：
CODEG_OPENCLAW_AUDIT_PACKAGE=/path/to/already-published/openclaw node --test src-tauri/resources/openclaw/published-package.audit.test.mjs
git diff --check
```

源码或依赖改变后，执行器会拒绝运行；必须先重新审查 allowlist 的调用链，再更新指纹。不能直接跑全量 cargo test。

## 尚未验证与交付状态

- 未连接真实模型 API 或用户实例；实际实例安装版本、真实计费/usage、跨进程崩溃恢复和线上完整流式行为尚未实测。早期两次 Claude 发布 CLI + 假 provider 的版本观察单列，不把它当作真实模型或无数据库验收。
- 六类适配代码不代表六类运行时已在线可用或全部实测通过。未知版本/源码、缺少实际 readiness 或插件未加载时继续关闭原生转向，保留普通队列。
- OpenClaw 需要后续在真正的原 Gateway 主机加载 owned 插件并重建 ACP 连接；已有配置/allowlist/deny/权限必须保留，不能把 ACP 本地目录当作远端 Gateway 已加载的目录。本轮未执行该实例变更。
- 纯文本以外的原生转向不在本合同范围；附件继续正常排队发送。
- 未编译发布包、未升级实例、未部署、未 push、未创建 PR。功能代码与 main 整合已作本地提交，未改远端 main；最新交付状态见 [main 合入记录](./main-sync-2026-11-03.md)。
