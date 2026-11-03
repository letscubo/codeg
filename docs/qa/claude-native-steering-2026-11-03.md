# Claude Code 原生转向：实现与验证记录

日期：2026-11-03（Asia/Shanghai）。工作区为 `codex/agent-queue-steering` 功能分支。此记录只覆盖 Claude，不代表其它运行时通过。

## 实现范围与支持门槛

保留现有 Codeg `ConnectionTransport::Cli`、`claude -p --input-format stream-json --output-format stream-json`、每轮进程、实例认证、会话与 transcript 路径。没有切换 SDK 或 ACP，没有升级 Claude。

- 启动前 `--version` 必须证明稳定 Claude 2.x 且版本不低于 2.1.287；无法解析、预发布版、旧版或其它主版本继续原来的一次输入后 EOF 路径，readiness=false。
- 实际子进程的 `system/init` 必须再次报告符合门槛的 `claude_code_version`、`msg_lifecycle_v1` capability 和完全匹配的 `session_id`。
- 必须观测初始 A UUID 的 `command_lifecycle: started` 才开放 readiness。缺少实际 capability 时主动关闭 stdin 写侧，恢复普通 EOF，避免旧版发送因保留 stdin 卡住。
- `SessionState.native_steer_v1_turn_id` 绑定该进程初始 A UUID；执行前还使用共享 `matches_turn` 验证当前轮，阻止旧轮命令写入。
- A 的第一份 result 到达即关闭 readiness、清除原生 token。即使宿主仍在等待 background follow-up，也不继续冒用 A 的旧身份。

改动为 `src-tauri/src/acp/cli/driver.rs`，新增 `claude_steering.rs`、`claude_steering_tests.rs`。不修改公共 steering manager、统一测试清单、Claude 旧反馈、默认运行时分流或其它驱动。测试模块只在 `cfg(all(test, unix))` 构建。

## 公开官方合同与固定版本源码

核对日期为本记录日期；公开 SDK 文档会继续更新，不能把新版本字段反推成旧 CLI 已支持。

官方文档：

- [SDKUserMessage](https://code.claude.com/docs/en/agent-sdk/typescript#sdkusermessage)：`priority: "next"` 在当前工具执行结束后进入同一轮；如果轮次先结束，普通消息可能进入下一轮。`shouldQuery: false` 只注入上下文，不启动 assistant turn。`client_composed: true` 保留已组合文本，避免再次执行 slash command 或展开 mentions；文档标注 CLI 2.1.248+ 与 TypeScript SDK 0.3.280+。
- [user_message_uuids](https://code.claude.com/docs/en/agent-sdk/typescript#user_message_uuids)：该列表列出本轮实际回答的输入 UUID；当前轮吸收用户消息时，把其 UUID 加入列表。文档标注 SDK 0.3.259+。单个 `user_message_uuid` 可能只指向一个输入，不能代替列表里的 A+B 同轮证明。
- [SDKSystemMessage](https://code.claude.com/docs/en/agent-sdk/typescript#sdksystemmessage)：对实际进程使用 capabilities 检测，未知 capability 忽略；不能仅靠版本字符串。

固定核对对象为本机 Anthropic 发布的原生 CLI：

```
/Users/cooper/.nvm/versions/node/v24.15.0/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe
version: 2.1.287 (Claude Code)
sha256: 6eab8333fe2121553100d8f40bfada384a3e989b94f947e18ba6677a6fcb41ea
```

原生包包含对应版本的 JS wire/schema 源码。以下字节偏移定位该二进制的源码文本，便于复核，不能用于其它 SHA256：

| 兼容点 | 固定版源码定位 | 证明内容 |
|---|---:|---|
| `msg_lifecycle_v1` | 193633809 | 出现在该版本初始化 capability 列表；不是当前公开 SDK 文档完整承诺的独立能力名称，按内部 wire 兼容点处理 |
| `command_lifecycle` schema | 180099999 | `command_uuid` 为输入 UUID，state 为 queued/started/completed/cancelled/discarded/refused。取消状态也可能表示已消费轮被中止，因此不能仅据 cancelled 事件判定未消费 |
| `cancel_async_message` 请求/响应 schema | 180203447 | 请求 `{subtype:"cancel_async_message",message_uuid:string}`；成功 payload `{cancelled:boolean}`。说明为按 UUID 从待处理队列移除；false 可表示已出队或从未入队 |
| 本地 stdin handler | 204882302 | 实际 print handler 调用按 UUID 的 `Sm({targetUuid,sender:"host",messageQueue,prewaitLatch,io})`，把真实撤销结果放入 cancelled |
| UUID 队列撤销实现 `Sm` | 204570941 | 不撤销 fold-in-flight 输入；对仍可从队列取出的目标 UUID 移除；结果不是撤回界面显示 |
| stdin 输入映射 | 204942337 附近 | 转发 `priority`、`shouldQuery` 和客户端 UUID 到当前命令 |
| 模型启动条件 | 204665046 附近 | 启动模型的条件包含 `De?.shouldQuery !== false`，只注入上下文不会自启新模型轮 |

`msg_lifecycle_v1` 和 `cancel_async_message` 是已核对发布 CLI 源码的内部兼容点，公开网页未给出完整稳定承诺。实现因此同时检查稳定版本、实际 init capability、精确 session 与消费证明；缺失或未知响应关闭能力或保留 unconfirmed，不猜测签名。

## 输入、消费证明与晚到竞态

B 通过同一运行中子进程的 stdin 发送，不启动第二个普通 prompt：

```json
{
  "type": "user",
  "uuid": "<Codeg生成的B_UUID>",
  "session_id": "<实际Claude会话>",
  "parent_tool_use_id": null,
  "origin": { "kind": "human" },
  "client_composed": true,
  "priority": "next",
  "shouldQuery": false,
  "message": { "role": "user", "content": [{ "type": "text", "text": "<B>" }] }
}
```

- queued、started 或 user replay 单独到达时都保持 pending。只有同 session、主线程的 assistant/stream_event/result 里 `user_message_uuids` 同时包含 A_UUID 和 B_UUID，才确认 B 进入原轮。相同文本的两条 B 依靠独立 UUID 区分。
- 确认消费后先写 Codeg transcript，再回复 Injected；不发送 `AcpEvent::UserMessage` 重置 A 的活动回复，不生成第二个宿主 TurnComplete。
- A result 先到、B 尚无同轮证明时，只发送如下按 B UUID 撤销请求，不调用 interrupt，也不取消 A：

```json
{
  "type": "control_request",
  "request_id": "codeg-native-cancel-<B_UUID>",
  "request": { "subtype": "cancel_async_message", "message_uuid": "<B_UUID>" }
}
```

只有匹配 request_id 的 success 回包 `response.cancelled === true` 才返回 PromptRequired/NotInjected。false、报错、超时或掉线均为 Unconfirmed，不自动重发。B 带 `shouldQuery:false`，即使晚到也不能自启另一轮模型调用；不能把它的 context-only result 当作成功 steer 或 A 的最终回复。

背景任务继续沿用现有 StreamMapper 跟踪。背景仍存在时的中间 result 不结束宿主 A；后来 task_notification 清空背景集合也不等于最终 result。新增 `final_terminal_ready` 仅由真正的 result 建立，防止晚到的 B 撤销 ACK/超时提前结束仍在回答的背景续轮。

## 两次早期隔离 CLI 探测的边界

早期曾运行两次发布 CLI 2.1.287 的隔离 loopback 探测。执行前未充分核实该 CLI 是否会初始化内部数据库，因此这两次不计入无数据库验收；之后已停止真实 CLI 探测。

- 脚本：`/private/tmp/claude-native-steer-probe.py`、`/private/tmp/claude-native-steer-probe-context.py`。
- 第一次临时目录：`/private/var/folders/pb/x4ffg5fd0wj1r5zmfxydl3g80000gn/T/claude-native-steer-ri0m7kge`，B 为 priority next。
- 第二次临时目录：`/private/var/folders/pb/x4ffg5fd0wj1r5zmfxydl3g80000gn/T/claude-native-steer-jr1yh9ff`，B 为 priority next + shouldQuery false。
- 使用隔离 CLAUDE_CONFIG_DIR、临时 cwd、固定假凭证、关闭非必要流量；模型 endpoint 显式为 127.0.0.1 的 Python 假 provider。没有真实模型调用。假 provider 只返回一个本地 `sleep 2` 工具调用与最终固定文本。
- 两次均观测两次本地 `/v1/messages` 请求：首次含 A、不含 B，第二次同时含 A+B。最终只有一份 success result，其 `user_message_uuids` 同时包含 A 与 B；`queued_turn_count=0`。这比单纯 replay 更强，证明该实际 CLI 在该场景同轮吸收了 B。
- 只在隔离目录发现 `config/.claude.json` 与一份 JSON backup；未发现 SQLite 文件头、`.db/.sqlite/.sqlite3` 或 `-wal/-shm` 磁盘痕迹。
- **未证明 CLI 没有初始化内存数据库或其它进程内部数据库，因此这两次不能计入已审查的无数据库受限测试。** 它们仅是早期版本协议观察，不能代替后续无数据库回归。脚本不应在未完成数据库审查前再次运行。

## 无数据库回归

以下用例只直接构造生产 `CliDriver`、SessionState、Noop emitter、mpsc/oneshot，运行临时 Python 假 CLI 与临时 CODEG_HOME transcript；不创建 AppState、数据库、真实 Claude 或网络。假 CLI 的 --version 固定输出，源码已做 Python AST 检查。统一执行与指纹清单由主代理维护。

Namespace：`acp::cli::driver::native_steering::tests::`。

1. `myclaw_prompt_claude_native_same_turn_proof_and_transcript`：replay/started 不提前确认；A+B result 才确认，B transcript 只有一份。
2. `myclaw_prompt_claude_native_late_input_requires_real_queue_cancellation`：A result 后按 B UUID 真正取消队列，只有 true 才未消费。
3. `myclaw_prompt_claude_native_late_consumption_or_lost_cancel_stays_unknown`：cancel=false 或丢 ACK 保持未知。
4. `myclaw_prompt_claude_native_stale_turn_never_writes_input`：不匹配轮次不写 B。
5. `myclaw_prompt_claude_native_other_session_cannot_confirm_input`：错误 session 的 A+B 帧不能确认。
6. `myclaw_prompt_claude_native_old_or_unadvertised_cli_preserves_eof`：旧版本、实际 init 缺能力或 session 错配均保留普通 EOF 完成路径，假进程必须显式记录收到 EOF。
7. `myclaw_prompt_claude_native_background_retains_one_host_terminal`：有背景工作时跨 result 保留 A，最终仅一次 TurnComplete。
8. `myclaw_prompt_claude_native_version_and_duplicate_text_keep_identity`：版本 fail-closed，同文不同 UUID 不混淆。
9. `myclaw_prompt_claude_native_receipt_after_background_notification_waits_for_final_result`：中间 A result→背景 notification→B ACK/超时/context-only result 交错不能提前结束宿主。

首次统一受限执行为 51/52 通过，暴露了本实现的真实 EOF 兼容错误：Tokio 1.49.0 的 Unix pipe `poll_shutdown` 只返回成功，未关闭仍持有的写句柄，导致缺能力回退等待。现已改为 `Option<ChildStdin>::take()` 后实际 drop，保留并增强原回归断言。[固定版官方实现](https://github.com/tokio-rs/tokio/blob/tokio-1.49.0/tokio/src/process/unix/mod.rs#L314-L316)

修复后的最终受限执行 **52/52 通过**（其中 Claude 专属 9/9），4343 项未执行；server 编译与库测试编译通过。未执行全量 cargo test，未提交、push、合并、部署或升级实例。
