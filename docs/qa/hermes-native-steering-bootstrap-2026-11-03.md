# Hermes 原进程 ACP steer：Codeg owned bootstrap

2026-11-03。本轮实际实现取代此前“必须先给外部 runtime 打补丁”的 Hermes 方案：Codeg 随包 bootstrap 在原 `hermes acp` Python 进程内接入现有原生消费边界，不修改 runtime 安装目录、不换聊天传输、不创建第二个 Agent。

## 固定依据

- 官方 Hermes `v2026.9.14`，peeled commit `345cd2b057a452236de401d3534b8502a7465e8d`，CLI 版本声明 `0.21.3` / release date `2026.9.14`。通过只读 git clone 到临时目录核验；GitHub raw 缓存曾与此 commit 不同，最终 manifest 与隔离测试均以 git commit 的源码为准。
- 官方 `pyproject.toml` 的 ACP extra 固定 `agent-client-protocol==0.9.0`；SDK 官方 wheel 由 `uv.lock` 的 URL 下载，SHA256 `06911500b51d8cb69112544e2be01fc5e7db39ef88fecbc3848c5c6f194798ee` 匹配，只解包读源码，没有安装。
- [ACP 原入口](https://github.com/NousResearch/hermes-agent/blob/345cd2b057a452236de401d3534b8502a7465e8d/acp_adapter/entry.py) 的 `main` 仍创建原 HermesACPAgent；[server](https://github.com/NousResearch/hermes-agent/blob/345cd2b057a452236de401d3534b8502a7465e8d/acp_adapter/server.py) 的 `_claim_turn_or_queue` 用原 `SessionState.runtime_lock` 认领 A，`prompt` 用 `contextvars.copy_context()` 进入原 executor / `_run_agent_turn`。
- SDK 0.9.0 `agent/router.py:114-119` 将 `_xxx` 扩展交给 `ext_method("xxx",params)`；`agent/connection.py:206-207` 的 `ext_notification` 添加前导 `_`；`router.py:104-107` 将 prompt `_meta` 展开到 kwargs。这些是已核实固定源码的签名，不推测其它 SDK 版本。

## 实现

`src-tauri/resources/hermes/sitecustomize.py` 消费一次性 `CODEG_HERMES_NATIVE_BOOTSTRAP=1`，保留用户既有 sitecustomize；仅原 ACP argv 装 `sys.meta_path` 目标导入钩子。两个 owned 缓存目录同时存在时，按本 startup 已访问的精确路径排除，避免 new→old→new 递归，不按通用目录名过滤用户代码。工具 Python 子进程不再继承启用 marker。

`codeg_native_steering.py` 在执行目标模块前读取 14 个 Hermes 文件、3 个 SDK 文件的精确 SHA。未知版本/源码、缺文件、SDK 不匹配、模块已被既有启动定制加载时均不安装扩展；普通原 ACP 启动继续，Codeg 不得据 launch env 宣称支持。没有额外导入 Hermes 来探测版本，也没有修改源文件。

npm 的官方社区包装 `hermes-agent@0.21.3` 会删除 PYTHONPATH。随包 `node-bootstrap.mjs` 因此只对该固定包的三个已核 SHA 文件启用 Node 官方 module load hook：在原 `runHermes` 的精确 Python ACP spawn 环境处恢复 owned 目录，保留原 venv/命令/argv，不恢复上游有意移除的 ambient PYTHONPATH。版本、源码、入口、命令或最终 env 不匹配时均继续原包装器。没有执行 npm installer。

Controller 绑定原 `SessionState.agent` 和该次 `_run_agent_turn` 调用。hostTurnId 通过原 context copy 跟随 executor。首次原生 `prepare_iteration` 已建立 `agent._current_turn_id` 后才发布 ready；该 ID 变化先关闭旧代并撤回未消费 B，再发布新代。所有 v1 admission、用户行追加、关闭和结果共用一把 RLock。

v1 输入独立保留逐条 ID，不写 legacy `_pending_steer` 拼接字符串槽，也不调用普通 `prompt`、`redirect`、cancel/resend 或 MCP pull。它使用原生 `steer_user_row`，在两个真实执行边界消费：

- `agent.agent_runtime_helpers.apply_pending_steer_to_tool_results` 完成原工具批次处理后；没有当前批次工具结果时保留待确认。
- `agent.turn_iteration_prep.prepare_iteration` 完成原准备/修复后、下一次模型请求前；只允许当前 A 用户行之后的工具结果触发，历史工具消息不能把 B 插到 A 之前。

追加到原生 messages 后才返回 injected。原 `finalize_turn` 进入前、原 ACP worker finally、cancel 前关闭 Controller，未消费 v1 直接 not_injected，不能被原生 finalizer 的 `pending_steer` 分支交给下一轮。原工具、模型请求、结果、usage、会话续接和 legacy steer 行为继续原路径。

公共 wire 由根代理接入：initialize `_meta["ai.myclaw/nativeSteering"]`，prompt `_meta["ai.myclaw/nativeTurn"]`，notification `_ai.myclaw/nativeSteeringState`，request `_ai.myclaw/steer`。RPC 等实际消费或撤回，调用方离开不会取消已接收 B 的 custody。重复 ID/相同正文返回原 future，正文或 scope 冲突保持 unconfirmed。

## 无数据库验证

运行：

```sh
cd src-tauri/resources/hermes
python3 -B -m unittest -v test_native_steering.py
```

16 项通过。覆盖真实官方 SessionState/claim、原生工具后与 pre-API 函数、旧 steer 与 v1 同时存在、旧工具历史、generation 替换、重复/冲突/跨 scope/附件拒绝、50 次 consume/close 线程竞态、取消/provider error、原 worker/context 传递、请求任务离开、原 finish/usage 保留、精确 SDK metadata/notification、未知源码静态拒绝、双 owned 缓存与用户 sitecustomize subprocess。

`native-test-fixtures.json` 只含固定官方源码 AST 隔离函数、来源 SHA 和许可证；没有导入 Hermes 模块、SessionManager 或 DB，没有初始化 SQLite（含内存库），没有真实模型、网络、用户实例或升级操作。标准库测试中的 provider/persistence 与无关的消息修复器为 fake；完整官方运行时打包/真实会话仍未实测。公共 Rust 编译和端到端假 ACP 测试由根代理统一报告。

另运行 `node --test src-tauri/resources/hermes/node-stdio-bootstrap.test.mjs`，6 项通过：1 项离线 registry 签名校验，5 项真实 stdio 启动链路测试。链路为官方固定 npm bin/launcher → 临时 venv 假 Python console → 真实生产 sitecustomize/core（仅装 Finder、不 import Hermes/SQLite）。覆盖 Node 24.15 同步 hook、强制异步官方 loader fallback、版本漂移、launcher 源码漂移和非 ACP 命令。owned 路径包含空格，NODE_OPTIONS 与 Rust 相同使用 `--import="path"`；检查原 stdin/out/argv/cwd/venv 与原 ambient 路径剔除。未把此结果描述为真实 Node 20、真实 Hermes/模型实测。

`node-wrapper-test-fixture.json` 来自官方 npm `hermes-agent-0.21.3.tgz`，原件 SHA1 `fae082e1f76fff3b1959940d9e9cdc1033c89d5f` 与 registry SHA512 integrity 均匹配，保存准确三个包装文件及 MIT 许可证。没有安装包或执行其 postinstall。

按 [npm registry 签名合同](https://docs.npmjs.com/about-registry-signatures/) 对 `${name}@${version}:${dist.integrity}` 验证：metadata 的两份 ECDSA 签名均 verified=true，匹配 registry key `SHA256:DhQ8wR5APBvFHLF/+Tc+AYvPOdTpcIDqOhxsBHRwC7U`。这证明 npm registry 内容签名，不是 Nous 对社区 npm 包的官方发布署名。
