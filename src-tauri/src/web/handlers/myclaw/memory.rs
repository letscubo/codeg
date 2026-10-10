//! `POST /api/myclaw/memory/invalidate` — fork(letscubo)专属:平台改了记忆,让这里的记忆缓存作废。
//!
//! 统一记忆的快照按连接缓存(见 `acp::myclaw_memory`)。平台在用户 / agent 改完记忆后调这里,
//! 每条连接下一轮重新取快照,版本变了就重新注入。调不到也没关系:缓存最长 10 分钟,
//! codeg 重启后缓存本来就是空的。
//!
//! 鉴权与 `/api/myclaw/exec` 同:受保护路由组内(主 token)。请求体 `{ all?: bool }`,
//! 目前只有「全部作废」一种粒度 —— 每条连接重新取一次快照的代价很小。

use axum::Json;
use serde_json::{json, Value};

pub async fn invalidate(body: Option<Json<Value>>) -> Json<Value> {
    let _ = body;
    crate::acp::myclaw_memory::invalidate_all().await;
    Json(json!({ "ok": true }))
}
