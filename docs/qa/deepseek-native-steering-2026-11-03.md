# DeepSeek 当前原生轮 steer 适配（2026-11-03）

## 结论与范围

现有 CLI 架构可以适配，复用 Codeg 已自带的 headless runner，不升级依赖、不切 TUI/SDK、不取消或重发原任务。新增输入在当前 Agent 的 `next-step` 通道进入；只有该消息 ID 在提交绑定的 native turn 中产生 `user/message` append，才确认为 `injected`。

本次修改限定 Codeg 的 dsh runner、dsh driver、私有协议 helper 和测试。普通初始 prompt、背景子 Agent 自动续轮、其它 Agent transport 保留原入口。steer 第一版仅纯文本，最大 16000 Unicode 码点。生产实例、真实模型、数据库、升级部署均未执行。

## 版本与调查证据

项目配置的固定发行版是 `@deepseek-ai/dsh@0.1.6-alpha.2`，不是 npm `latest`。依据 `src-tauri/src/acp/registry.rs:2196-2212`，启动命令依据 `src-tauri/src/acp/cli/dsh_prompt.rs:20-37`：

```text
dsh --profile headless --patch <Codeg patch> --json [--session-id <exact sid>] -- <initial prompt>
```

这说明项目安装目标；本次没有连接实例执行 `dsh --version`，不把它当作已部署版本。官方 npm 精确 tarball 已只读下载、解包核查，未安装运行：

| 官方包（均为 0.1.6-alpha.2） | npm dist SHA-1 | 关键依据 |
| --- | --- | --- |
| `@deepseek-ai/dsh` | `37d635377c9807c47d49d662ca00d6d5ea5792de` | launcher 固定发行版 |
| `@deepseek-ai/dsh-agent` | `f113b4522c1fd8ada720394a5bc2d5562e36bd4c` | `lib/types/runtime-types.d.ts` 的 Agent / Inbox / 事件合同 |
| `@deepseek-ai/dsh-agent-loop` | `e35b69336aceb432475b13be07c4cc8e8f34d212` | `lib/index.js` 的实际 send、claim、turn、step 实现 |

包源分别为官方 [dsh](https://registry.npmjs.org/@deepseek-ai/dsh/-/dsh-0.1.6-alpha.2.tgz)、[agent](https://registry.npmjs.org/@deepseek-ai/dsh-agent/-/dsh-agent-0.1.6-alpha.2.tgz)、[agent-loop](https://registry.npmjs.org/@deepseek-ai/dsh-agent-loop/-/dsh-agent-loop-0.1.6-alpha.2.tgz) tarball。`dsh-llm`、`dsh-session`、`dsh-headless`、`dsh-sdk-protocol`、`dsh-sdk-jsonrpc-server` 同一固定版本也已核对。临时审查源在 `/private/tmp/myclaw-dsh-steer-source-20261103/<package>/package/`，不是项目依赖。

另通过只读 `git ls-remote ... refs/heads/master` 核到调查时 upstream master 为 `ce7073d51254c26cd63b22a60835b8f948deb447`。当前 [Agent API](https://github.com/deepseek-ai/deepseek-harness/blob/ce7073d51254c26cd63b22a60835b8f948deb447/packages/core/agent/src/runtime-types.ts)、[SDK wire 合同](https://github.com/deepseek-ai/deepseek-harness/blob/ce7073d51254c26cd63b22a60835b8f948deb447/packages/sdk/protocol/README.md)、[headless runner](https://github.com/deepseek-ai/deepseek-harness/blob/ce7073d51254c26cd63b22a60835b8f948deb447/packages/bundle/headless/src/index.ts) 已复查。npm 元数据没有 `gitHead`，该 master 与精确发行 tarball 的 commit 对应关系**未证实**；实现依赖核过的发行包，而不是假定 master 与其一致。

## 官方接口与消费证明

固定包 `dsh-agent/lib/types/runtime-types.d.ts`：

- `send(message: UserMessage, target: InboxTarget, wakeup: boolean): void`，第 176-186 行。
- `steer(message: UserMessage): void`，第 193-200 行；空闲时能打开新轮，不能直接作为严格同轮保证。
- `inject(message: UserMessage): void`，第 201-208 行；只进入下一 step，不唤醒新轮。
- `inbox.remove(messageId): boolean`，第 66-72 行；可以精确撤回，不能用 `clear()` 清掉其它系统／子 Agent 消息。
- `agent/inbox/claimed`，第 268-281 行，明确 claim 后 proposed step 仍可能被拒绝；claim 本身不是消费回执。
- `agent/pre-step` 是正式 waterfall，可在 step 准入前替换消息集合；`agent/turn-stopping` 是正式 awaited serial 停轮边界。

固定包 `dsh-agent-loop/lib/index.js`：

| 行 | 源码语义 | 接入约束 |
| --- | --- | --- |
| 781-793 | `steer` 调 `send(next-step, true)`；abort 后 waking input 会改到 next-turn；`inject` 调 `send(next-step, false)` | 用公开 `inject`，并同步检查 phase.running、native turn 和未 abort |
| 90-110、145-151 | claim 将消息从 inbox 移出；remove 按唯一 ID 删除仍 pending 的消息 | 不按文本匹配、不清整个 inbox |
| 887-904 | 先 claim，再 await pre-step waterfall | 已 claim 的旧轮输入仍可在新轮准入前按 ID 排除 |
| 924、949、1026 | append turn/start、step/start；请求准备获准后才 append user/message | 用户输入的原生消费证据是同 ID、同 turn、同 step 所属轮的 user/message |
| 964-972 | await turn-stopping；随后重新读 inbox 决定是否继续当前轮 | 可在此同步精确清理，且不能假定“要停”已经停了 |
| 869-881 | turn settles 后同步 setPhase(idle) 并发 status，随后才检查 latched wake | 错误／abort 路径的 idle 清理先于下一 wake/claim |

`user/message` append 是原生消费／转录事件，**不是磁盘 flush 完成证明**。原 runner 的 `sessions.flush(agent.session)` 仍在整个宿主 run 末尾。Codeg 收到确认后先 `record_prompt(B)` 写入自身现有 transcript，再回 `Injected`，以保持平台历史。

## 为什么不能只打开 stdin、改 TUI 或调用 SDK prompt

stock headless 从 argv 取一个 task，或读 stdin 到 EOF 一次，随后 followup → whenIdle → flush → final → exit；它没有本次控制帧协议。只把 stdin 由 null 改成 pipe，不会自动让文本成为当前轮 steer。

交互 TUI 可通过 Agent API 实现输入交互，但解析终端按键没有可依赖的消息 ID、精确 native turn 和消费回执；切换模式还会扩大当前驱动边界。当前固定 SDK 与调查时 master 的 wire 方法均没有已证实的严格同轮 steer 请求；`session/prompt` 是普通排队的消息回执，不能冒充当前轮消费。

Codeg 本来就通过 `dsh_profile.rs` materialize 并挂载 `codeg-headless-runner.mjs`，处理背景子 Agent 等待和工具图片。因此本次扩展这个已存在的 controller，再让 `dsh_driver.rs` 读写它的私有 JSONL 控制帧，不修改上游 bundle。

## 实现与失败语义

1. 仅自带 runner 使用 pipe；`CODEG_DSH_RUNNER=stock` 保持原 stdin 语义并关闭新能力。
2. 启动时核查 dsh、agent、agent-loop、llm、session 五个实际包版本，全部严格为 `0.1.6-alpha.2` 且 API 存在才启用；未知版本、元数据缺失、未知 phase 关闭。普通 runner 行为仍可使用。
3. 初始 A 已进入原生 user/message 后才宣布 readiness。每个 native turn 生成 `connection:conversation:host-turn:dsh:native-number` token，仅包含 Web 允许的字符；背景自动续轮换 token，旧 token 拒绝。
4. 控制提交包含 request ID、精确 session ID、native turn、纯文本。桥接创建独立原生 message ID，并用 `inject` 放入本 Agent。相同文本的不同 request 不合并；重复 request ID 不再执行。
5. 插入、queued、claimed 均不返回 injected；仅同 native turn 的精确 user/message ID 才确认。错轮消费、claim 后拒绝、输出断线、丢回执等保持 `unconfirmed`，不重发。
6. `turn/end` observer 只关闭 readiness，**不写 inbox**。官方 `dsh-session/lib/index.js:1248-1273` 禁止 Session.append 发布期间重入，原先在此 remove 的草案已经修正。精确撤回位于 awaited turn-stopping／同步 idle hook；pre-step 还按 ID 排除旧轮输入，覆盖宿主 run 中直接进入背景续轮而无 idle 的路径。其它背景／系统消息保留。
7. 确认精确 remove 成功、未 claim 的 discard 或严格准入拒绝，才允许 `not_injected`。无法证明的撤回／消费结果不降级成普通重发。移除 inbox 不是回滚已产生的 user/message，更不是回滚已完成的模型工作。
8. Rust helper 校验控制协议、发行版本、session、request ID、提交时 native turn。收到 `injected` 先写 Codeg transcript，再返回消费结果；结束／取消／断线／错误集中清 readiness 和 token。final 先到、回执丢失不会被 final 替代为成功。

## 验证边界

Node 命令：

```sh
node --test src-tauri/resources/dsh/codeg-native-steering.test.mjs src-tauri/resources/dsh/codeg-headless-runner.test.mjs
```

结果 **27/27 通过**：原 runner 的 17 项与新 bridge 的 10 项。新测试使用纯假 Agent／inbox／事件，加入 Session.append 重入 guard 模拟，覆盖当前轮消费、claim 未消费、轮末精确撤回、无 idle 背景下一轮准入过滤、相同文字不同 ID、重复 ID、abort／错误 scope／超长输入和未知版本关闭。它们不是运行真实官方 Session／真实模型的集成验证。

Rust 新增八项须通过仓库审计后的无 DB allowlist 执行，不能直接运行默认 cargo test：

```sh
node scripts/test-no-db.mjs server
```

四个 pure helper 测试位于 `dsh_driver::native_steering::tests`；四个 driver 测试位于 `dsh_driver::native_steering_tests`（Unix）。driver 走真实 DshDriver + 本地 fake Python JSONL 进程、SessionState、Noop emitter、临时 transcript 文件；不构造 ConnectionManager／AppState，不访问数据库、provider 或真实 Agent。覆盖延迟消费回执、记录 B、两条相同文本不同 ID、final 先于回执／丢回执保持 unknown、stock／未知版本关闭。最终 allowlist **52/52 通过**，其中本模块 8/8；server 编译与库测试编译通过。完整复现与其它模块边界见 [总验证记录](./native-steering-v1-2026-11-03.md)。

未验证：真实固定版 dsh Agent 与真实模型的端到端 steer、运行中崩溃恢复、生产实例版本和发布。该实现需要新 Codeg 二进制携带更新的 runner；仅更新 Web 不会使旧实例自动获得该能力。
