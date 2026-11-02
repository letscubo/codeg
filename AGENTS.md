# AGENTS.md

This file provides guidance to Code Agent when working with code in this repository.

## 项目概述

Codeg（Code Generation）是一个多智能体编码工作台，它将多个智能体（Claude Code、Codex CLI、OpenCode、Gemini CLI、OpenClaw、Cline 等）统一到一个工作区中，支持会话聚合和多智能体协作，支持桌面安装，服务器/Docker 部署。

## 分支工作流（2026-11-03，用户明确要求）

- Codeg 后续开发与迭代直接在 `main` 分支进行，不再默认创建功能分支。
- 开始修改和提交前先同步 `origin/main`，保护并恢复已有工作区改动。只有用户以后明确改变要求时，才改用其他分支。
- commit、push、版本 tag 和 Release 仍分别遵守用户授权范围；“在 main 开发”不代表可以自行推送或发版。

## 技术栈

- **服务器运行时**: 独立 Rust 二进制（Axum HTTP + WebSocket）—— 本 fork 的唯一交付物
- **桌面运行时**: Tauri 2 外壳仍能编译，但没有界面可加载（见下）
- **数据库**: SeaORM + SQLite
- **包管理器**: pnpm（只剩 `@tauri-apps/cli` 一个依赖，用于发版签名）

### ⚠️ 本 fork 没有前端

上游的 Next.js 应用（`src/`、`public/` 及全部前端配置）**已从本 fork 删除**：平台
（MyClaw）有自己的界面，只调用 codeg 的 HTTP API（`/api/*` 与 `/ws`），从不打开
codeg 自带的工作台。因此：

- 不要新增 / 恢复任何 `.ts` / `.tsx` 界面代码，也不要给后端改动"配套改前端"
- release 包里的 `web/` 只有占位页：旧版（< v0.30.10-10）自更新器硬性要求包里有 `web/`，
  删掉它存量实例就升不上来。容器不设 `CODEG_STATIC_DIR`，服务端对非 API 路径一律 404
- `out/index.html` 是**占位文件**，只为让 tauri-build 校验 `frontendDist` 通过；它不是界面
- 合上游时 `src/` / `public/` 下的冲突一律按"删除"解决：
  `git diff --name-only --diff-filter=U | grep -E '^(src|public)/' | xargs -r git rm -q`

## 代码检查与测试（任务完成后进行必要的检查）

### 禁止测试创建数据库（2026-11-02，用户要求）

- 测试不得创建或初始化数据库，包括内存 SQLite、临时 SQLite 文件、测试数据库及任何迁移、种子写入。
- 现有依赖真实数据库的用例仅保留编译检查（`cargo test --no-run`），不得直接执行全量 `cargo test`。这不代表数据库集成测试已通过。
- Rust 实际执行统一通过 `scripts/test-no-db.mjs`：只运行 `scripts/no-db-tests.json` 中已审查的无数据库测试，当前为 19 项 Prompt 测试。它们只使用请求反序列化、内存 SessionState、Noop emitter、通道、纯参数／patch 构造和私有临时 JSON 文件，不创建 SQLite。
- `src-tauri/resources/codeg-effort.test.mjs` 的 8 项 Node 用例也已经逐项审查：只调用纯 wire 校验、mock metadata/selection、只读 settings backend、每模型默认 getter 和 sampling 对象合并，不调用 runner apply、原生 SDK、Agent、Session 或 CLI。可用 `CODEG_PI_EFFORT_PROBE=0 node --test src-tauri/resources/codeg-effort.test.mjs` 执行。生产 MJS 资源与 Rust 源码一起纳入清单指纹。
- 新增或修改测试应使用 mock/fake 等无数据库方式。Rust 源码或依赖变化后，必须重新核对允许执行用例的调用链并更新清单指纹；不能只为通过 CI 自动刷新指纹或扩大匹配条件。
- 按用户要求不修改 `.github/workflows/test.yml`。该工作流仍含建库测试，不得触发；本次发布提交使用 `[skip ci]` 跳过 push/pull_request 自动测试，通过 `gh workflow run release.yml --ref <版本标签>` 手动触发 Release。
- Release 在打包发布前执行保护脚本测试和已审查的 19 项无数据库 Prompt 测试（`node scripts/test-no-db.mjs server --release`）；不会执行全量数据库测试。不得把这一结果描述为全量测试通过。

### Rust（在 `src-tauri/` 目录下执行）

```bash
# 桌面模式（默认 feature）
cargo check
cargo test --features test-utils --no-run
node ../scripts/test-no-db.mjs desktop
cargo clippy --all-targets --features test-utils -- -D warnings

# 服务器模式
cargo check --no-default-features --bin codeg-server
cargo test --no-default-features --bin codeg-server --lib --no-run
node ../scripts/test-no-db.mjs server
cargo clippy --no-default-features --bin codeg-server --lib -- -D warnings

# codeg-mcp 协作伴生进程（多智能体委托）
cargo check --no-default-features --bin codeg-mcp
cargo clippy --no-default-features --bin codeg-mcp -- -D warnings

# 解析器快照评审（输出变化时）
cargo insta review
# 执行快照用例前先完成无数据库审查并纳入清单；禁止全量自动执行。
```

## 架构

### 双模式运行

项目通过 Cargo feature flags 支持三种二进制：

- **`codeg`**（`tauri-runtime`，默认）：完整桌面应用，包含 Tauri 窗口管理、系统通知、自动更新等
- **`codeg-server`**（无 feature，`--no-default-features`）：独立服务器模式，仅编译 Axum HTTP API + WebSocket
- **`codeg-mcp`**（无 feature）：per-launch stdio MCP 伴生进程，被注入到代理 CLI 的 MCP 配置中，向 LLM 暴露**异步**子智能体委托工具。

### 共享核心

- **`app_state.rs`** — `AppState` 共享状态结构，两种模式通过 `EventEmitter` 枚举区分事件发射方式
- **`web/event_bridge.rs`** — `EventEmitter::Tauri(AppHandle)` 或 `EventEmitter::WebOnly(Arc<WebEventBroadcaster>)`
- **`web/router.rs`** — Axum 路由，接受 `Arc<AppState>`
- **`web/handlers/`** — HTTP API 端点，全部使用 `Extension<Arc<AppState>>`

### Rust 后端（`src-tauri/src/`）

后端负责读取和解析本地文件系统上的代理会话文件：

- **`app_state.rs`** — 共享状态（db、连接管理器、终端管理器、事件广播器）
- **`models/`** — 共享数据结构
- **`parsers/`** — 每个智能体一个解析器
- **`commands/`** — 业务逻辑，`_core` 函数供两种模式共用，`#[tauri::command]` 函数仅桌面模式
- **`web/`** — Axum HTTP API + WebSocket + 静态文件服务 + 认证中间件
- **`acp/`** — Agent Client Protocol 连接管理
- **`db/`** — SeaORM + SQLite

### 数据流

平台调用：MyClaw `fetch()` → Axum HTTP API → 业务逻辑 → 返回 JSON
实时通信：后端事件 → EventEmitter（WebSocket 广播）→ 平台
桌面模式（本 fork 不交付）：`invoke()` → Tauri 命令 → 同一套业务逻辑

### 条件编译约定

- `#[cfg(feature = "tauri-runtime")]` — 仅桌面模式编译（Tauri 窗口、通知、`tauri::State` 参数等）
- `#[cfg_attr(feature = "tauri-runtime", tauri::command)]` — 函数始终可用，仅在桌面模式标记为 Tauri 命令
- `_core` 后缀函数 — 接受普通引用参数（`&AppDatabase`、`&EventEmitter`），供 Web handlers 和 Tauri 命令共用

## 关键约束

- **服务器部署**：通过环境变量配置（`CODEG_PORT`、`CODEG_HOST`、`CODEG_TOKEN`、`CODEG_DATA_DIR`）。`CODEG_STATIC_DIR` 仍被读取，但本 fork 没有可指的静态目录，正常部署不设它
- **Docker 支持**：单阶段 Rust 构建，支持 `docker-compose` 一键部署

## 代码风格

- Rust：2021 edition，使用 `thiserror` 定义错误类型
- 仓库里剩余的少量 `.mjs`（sidecar 脚本、dsh 插件）沿用 Prettier 默认：无分号、尾逗号（es5）、2 空格缩进


## 内部提示词参数（v0.30.10-78）

- `/api/cli_prompt`、`/api/acp_prompt` 接受可选字符串 `internalPrompt`；Tauri `acp_prompt` 同义。由调用方提供本轮最终提示词，Codeg 不再调用 `/api/codeg/prompt/snapshot`、读取 agent_config 或判断首轮/续聊。
- 省略、null、空字符串、纯空白都不注入；每轮独立，不缓存、不沿用上一轮，不在 SessionState 保存内容。
- 非空内容只插入执行用 blocks，用户消息回显、clientMessageId、附件、标题与预览仍取原始消息。沿用 v1 `myclaw_internal_prompt` 包装；`phase="existing_session"` 仅兼容已有历史过滤器，不代表 Codeg 对会话年龄作出判断。原生运行时记录仍可能包含包装内容，平台需继续按该标记过滤。
- 限制 UTF-8 文本为 64 KiB，错误不回显提示词；HTTP 入口在连接/会话/relay 副作用前校验。旧内部发送调用默认不提供该参数。
- health 能力标识 `internal_prompt_inline_v1` 用于未来 MyClaw 接入协商。本次 Codeg 发版不部署 MyClaw 调用方、不执行 SQL、不升级实例；未传参数的调用方按约定不注入内部提示词。

请求示例（其余 CLI 启动字段／ACP 连接字段沿用原协议）：

```json
{
  "connectionId": "existing-connection",
  "folderId": 1,
  "conversationId": 137,
  "blocks": [{ "type": "text", "text": "hi" }],
  "internalPrompt": "本轮执行规则"
}
```

本地验证：服务器模式测试代码仅编译（不运行数据库用例）；13 项已审查无数据库 Prompt 测试及 6 项执行器守卫测试通过。codeg-server、codeg-mcp、桌面 codeg 的 cargo check 通过。严格 Clippy 被 7 处既有问题阻断（event_bridge 文档空行、pi_stream/turn_relay 未使用方法、connection 的 future/参数数量、registry match、session_commands if）；未扩展本次任务清理它们。完整仓库 fmt 亦有既有差异，仅对本次重写的 Prompt 实现和测试做了格式检查。

## WhatsApp 独立 relay 地址

- WhatsApp outbound 必须携带 `relayEndpoint`，并同时使用原有短期 `deliveryGrant`。Codeg 只接受与本轮 `turnId` 完全匹配的公开 HTTPS relay URL。
- WhatsApp relay 不读取 `chat_event_webhooks`，也不从普通 Codeg 事件回调推导平台地址；`vm_codeg_config.webhook_origin` 只属于普通 Codeg 事件链。
- health 能力 `whatsapp_relay_endpoint_v1` 用于 MyClaw 协商。未提供该字段的旧调用应在反序列化阶段失败，不能静默回退到事件 webhook。
