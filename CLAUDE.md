# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Codeg（Code Generation）是一个多智能体编码工作台，它将多个智能体（Claude Code、Codex CLI、OpenCode、Gemini CLI、OpenClaw、Cline 等）统一到一个工作区中，支持会话聚合和多智能体协作，支持桌面安装，服务器/Docker 部署。

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
- release 包里不含 `web/` 目录，容器不设 `CODEG_STATIC_DIR`，服务端对非 API 路径一律 404
- `out/index.html` 是**占位文件**，只为让 tauri-build 校验 `frontendDist` 通过；它不是界面
- 合上游时 `src/` / `public/` 下的冲突一律按"删除"解决：
  `git diff --name-only --diff-filter=U | grep -E '^(src|public)/' | xargs -r git rm -q`

## 代码检查与测试（任务完成后进行必要的检查）

### Rust（在 `src-tauri/` 目录下执行）

```bash
# 桌面模式（默认 feature）
cargo check
cargo test --features test-utils
cargo clippy --all-targets --features test-utils -- -D warnings

# 服务器模式
cargo check --no-default-features --bin codeg-server
cargo test --no-default-features --bin codeg-server --lib
cargo clippy --no-default-features --bin codeg-server --lib -- -D warnings

# codeg-mcp 协作伴生进程（多智能体委托）
cargo check --no-default-features --bin codeg-mcp
cargo clippy --no-default-features --bin codeg-mcp -- -D warnings

# 解析器快照评审（输出变化时）
cargo insta review
INSTA_UPDATE=auto cargo test --features test-utils     # 自动写新 .snap
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
