use std::sync::Arc;

use axum::{extract::Extension, Json};
use serde::Deserialize;

use crate::app_error::AppCommandError;
use crate::app_state::AppState;
use crate::chat_channel::backends::weixin::{WeixinQrcodeInfo, WeixinQrcodeStatusPublic};
use crate::chat_channel::webhook::WebhookConfig;
use crate::commands::chat_channel as cc_commands;
use crate::models::chat_channel::{ChannelStatusInfo, ChatChannelInfo, ChatChannelMessageLogInfo};

// ---------------------------------------------------------------------------
// Param structs
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateChatChannelParams {
    pub name: String,
    pub channel_type: String,
    pub config_json: String,
    pub enabled: bool,
    pub daily_report_enabled: bool,
    pub daily_report_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateChatChannelParams {
    pub id: i32,
    pub name: Option<String>,
    pub enabled: Option<bool>,
    pub config_json: Option<String>,
    pub event_filter_json: Option<Option<String>>,
    pub daily_report_enabled: Option<bool>,
    pub daily_report_time: Option<Option<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelIdParams {
    pub id: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveTokenParams {
    pub channel_id: i32,
    pub token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelIdOnlyParams {
    pub channel_id: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMessagesParams {
    pub channel_id: i32,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

pub async fn list_chat_channels(
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<Vec<ChatChannelInfo>>, AppCommandError> {
    let result = cc_commands::list_chat_channels_core(&state.db).await?;
    Ok(Json(result))
}

pub async fn create_chat_channel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<CreateChatChannelParams>,
) -> Result<Json<ChatChannelInfo>, AppCommandError> {
    let result = cc_commands::create_chat_channel_core(
        &state.db,
        params.name,
        params.channel_type,
        params.config_json,
        params.enabled,
        params.daily_report_enabled,
        params.daily_report_time,
    )
    .await?;
    Ok(Json(result))
}

pub async fn update_chat_channel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<UpdateChatChannelParams>,
) -> Result<Json<ChatChannelInfo>, AppCommandError> {
    let result = cc_commands::update_chat_channel_core(
        &state.db,
        params.id,
        params.name,
        params.enabled,
        params.config_json,
        params.event_filter_json,
        params.daily_report_enabled,
        params.daily_report_time,
    )
    .await?;
    Ok(Json(result))
}

pub async fn delete_chat_channel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<ChannelIdParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::delete_chat_channel_core(&state.db, &state.chat_channel_manager, params.id)
        .await?;
    Ok(Json(()))
}

pub async fn save_chat_channel_token(
    Json(params): Json<SaveTokenParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::save_chat_channel_token_core(params.channel_id, &params.token)?;
    Ok(Json(()))
}

pub async fn get_chat_channel_has_token(
    Json(params): Json<ChannelIdOnlyParams>,
) -> Result<Json<bool>, AppCommandError> {
    let has = cc_commands::get_chat_channel_has_token_core(params.channel_id)?;
    Ok(Json(has))
}

pub async fn delete_chat_channel_token(
    Json(params): Json<ChannelIdOnlyParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::delete_chat_channel_token_core(params.channel_id)?;
    Ok(Json(()))
}

pub async fn connect_chat_channel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<ChannelIdParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::connect_chat_channel_core(&state.db, &state.chat_channel_manager, params.id)
        .await?;
    Ok(Json(()))
}

pub async fn disconnect_chat_channel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<ChannelIdParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::disconnect_chat_channel_core(&state.chat_channel_manager, params.id).await?;
    Ok(Json(()))
}

pub async fn test_chat_channel(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<ChannelIdParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::test_chat_channel_core(&state.db, params.id).await?;
    Ok(Json(()))
}

pub async fn get_chat_channel_status(
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<Vec<ChannelStatusInfo>>, AppCommandError> {
    let result = cc_commands::get_chat_channel_status_core(&state.chat_channel_manager).await?;
    Ok(Json(result))
}

pub async fn list_chat_channel_messages(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<ListMessagesParams>,
) -> Result<Json<Vec<ChatChannelMessageLogInfo>>, AppCommandError> {
    let result = cc_commands::list_chat_channel_messages_core(
        &state.db,
        params.channel_id,
        params.limit,
        params.offset,
    )
    .await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCommandPrefixParams {
    pub prefix: String,
}

pub async fn get_chat_command_prefix(
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<String>, AppCommandError> {
    let result = cc_commands::get_chat_command_prefix_core(&state.db).await?;
    Ok(Json(result))
}

pub async fn set_chat_command_prefix(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<SetCommandPrefixParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::set_chat_command_prefix_core(&state.db, params.prefix).await?;
    Ok(Json(()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetEventFilterParams {
    pub filter: Option<Vec<String>>,
}

pub async fn get_chat_event_filter(
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<Option<Vec<String>>>, AppCommandError> {
    let result = cc_commands::get_chat_event_filter_core(&state.db).await?;
    Ok(Json(result))
}

pub async fn set_chat_event_filter(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<SetEventFilterParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::set_chat_event_filter_core(&state.db, params.filter).await?;
    Ok(Json(()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetEventWebhooksParams {
    pub webhooks: Vec<WebhookConfig>,
}

pub async fn get_chat_event_webhooks(
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<Vec<WebhookConfig>>, AppCommandError> {
    let result = cc_commands::get_chat_event_webhooks_core(&state.db).await?;
    Ok(Json(result))
}

pub async fn set_chat_event_webhooks(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<SetEventWebhooksParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::set_chat_event_webhooks_core(&state.db, params.webhooks).await?;
    Ok(Json(()))
}

pub async fn get_chat_message_language(
    Extension(state): Extension<Arc<AppState>>,
) -> Result<Json<String>, AppCommandError> {
    let result = cc_commands::get_chat_message_language_core(&state.db).await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetMessageLanguageParams {
    pub language: String,
}

pub async fn set_chat_message_language(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<SetMessageLanguageParams>,
) -> Result<Json<()>, AppCommandError> {
    cc_commands::set_chat_message_language_core(&state.db, params.language).await?;
    Ok(Json(()))
}

// ---------------------------------------------------------------------------
// WeChat QR code auth
// ---------------------------------------------------------------------------

pub async fn weixin_get_qrcode() -> Result<Json<WeixinQrcodeInfo>, AppCommandError> {
    let result = cc_commands::weixin_get_qrcode_core().await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WeixinCheckQrcodeParams {
    pub channel_id: i32,
    pub qrcode: String,
}

pub async fn weixin_check_qrcode(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<WeixinCheckQrcodeParams>,
) -> Result<Json<WeixinQrcodeStatusPublic>, AppCommandError> {
    let result =
        cc_commands::weixin_check_qrcode_core(&state.db, params.channel_id, &params.qrcode).await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InjectChatChannelMessageParams {
    pub conversation_id: i32,
    pub text: String,
}

/// Web-surface send for a CHANNEL conversation — the "one execution channel"
/// contract: the channel connection is the only place channel conversations
/// run; other surfaces (the web workspace) inject their text HERE instead of
/// spawning their own connection.
///
/// Flow: resolve the channel chat that is "on" this conversation → post a
/// "🌐 <text>" marker to the chat (Telegram is the source-of-truth stream, so
/// the web-typed message must appear there too) → if a live session for that
/// chat points at a DIFFERENT conversation, retire it (route repointed, same
/// as a /tasks switch) → enqueue the text through the normal inbound pipeline,
/// exactly as if the user had typed it in the channel. The reply then flows to
/// the channel natively, and the web renders everything via sync + attach_all.
pub async fn inject_chat_channel_message(
    Extension(state): Extension<Arc<AppState>>,
    Json(params): Json<InjectChatChannelMessageParams>,
) -> Result<Json<()>, AppCommandError> {
    use crate::chat_channel::types::{ChannelMessageTarget, ChannelSessionDefaults, IncomingCommand, RichMessage, TelegramConfig};
    use crate::db::service::{
        chat_channel_service, sender_context_service, thread_binding_service,
    };

    let db = &state.db.conn;
    let manager = &state.chat_channel_manager;
    let text = params.text.trim().to_string();
    if text.is_empty() {
        return Err(AppCommandError::task_execution_failed(
            "text is empty".to_string(),
        ));
    }
    if text.starts_with('/') {
        // 注入的是对话文本,不是渠道命令 —— 防止网页输入被当成 /new 之类执行。
        return Err(AppCommandError::task_execution_failed(
            "commands cannot be injected".to_string(),
        ));
    }

    // ---- 路由解析:direct-chat 优先,其次话题线程,最后"唯一发送者"兜底 ----
    let mut route: Option<(i32, String, ChannelMessageTarget)> = None;
    if let Ok(ctxs) =
        sender_context_service::list_by_current_conversation(db, params.conversation_id).await
    {
        if let Some(ctx) = ctxs.into_iter().next() {
            let target = ChannelMessageTarget::telegram_direct(
                ctx.channel_id,
                ctx.sender_id.clone(),
                ctx.sender_id.clone(),
            );
            route = Some((ctx.channel_id, ctx.sender_id, target));
        }
    }
    if route.is_none() {
        if let Ok(bindings) =
            thread_binding_service::list_by_conversation(db, params.conversation_id).await
        {
            if let Some(b) = bindings.into_iter().next() {
                let target = ChannelMessageTarget::telegram_forum_topic(
                    b.channel_id,
                    b.chat_id.clone(),
                    b.thread_key.clone(),
                );
                route = Some((b.channel_id, b.created_by_sender_id, target));
            }
        }
    }
    if route.is_none() {
        // 会话不是任何渠道聊天的"当前会话"(用户在网页上打开了一条历史渠道
        // 会话)。单发送者实例的合理语义:视为在渠道里 /tasks 切换到它。
        let ctxs = sender_context_service::list_all(db).await.unwrap_or_default();
        let mut candidates: Vec<_> = ctxs
            .into_iter()
            .filter(|c| c.current_conversation_id.is_some() || c.current_folder_id.is_some())
            .collect();
        if candidates.len() == 1 {
            let ctx = candidates.remove(0);
            let target = ChannelMessageTarget::telegram_direct(
                ctx.channel_id,
                ctx.sender_id.clone(),
                ctx.sender_id.clone(),
            );
            route = Some((ctx.channel_id, ctx.sender_id, target));
        }
    }
    let Some((channel_id, sender_id, target)) = route else {
        return Err(AppCommandError::task_execution_failed(
            "conversation is not attached to any channel chat".to_string(),
        ));
    };

    // ---- 会话对齐:活动会话指向别的 conversation 就先退掉,并重指路由 ----
    if let Some(bridge) = manager.session_bridge().await {
        let stale_conn = {
            let guard = bridge.lock().await;
            guard
                .find_by_sender(channel_id, &sender_id)
                .filter(|s| s.conversation_id != params.conversation_id)
                .map(|s| s.connection_id.clone())
        };
        if let Some(conn_id) = stale_conn {
            bridge.lock().await.remove(&conn_id);
            let _ = state.connection_manager.disconnect(&conn_id).await;
        }
    }
    let _ = sender_context_service::update_session(
        db,
        channel_id,
        &sender_id,
        Some(params.conversation_id),
        None,
    )
    .await;

    // ---- 🌐 先落渠道流(Telegram 是事实源,网页敲的字也要在那里出现)----
    let marker = RichMessage::info(format!("🌐 {text}"));
    let _ = manager.send_to_target(&target, &marker).await;

    // ---- defaults(新任务路径用;续聊路径忽略)----
    let session_defaults: Option<ChannelSessionDefaults> =
        match chat_channel_service::get_by_id(db, channel_id).await {
            Ok(Some(row)) => serde_json::from_str::<TelegramConfig>(&row.config_json)
                .ok()
                .and_then(|cfg| {
                    match (cfg.default_folder_path, cfg.default_agent_type) {
                        (Some(f), Some(a)) if !f.trim().is_empty() && !a.trim().is_empty() => {
                            Some(ChannelSessionDefaults {
                                folder_path: f,
                                agent_type: a,
                            })
                        }
                        _ => None,
                    }
                }),
            _ => None,
        };

    // ---- 入队:与渠道收到用户消息完全同路 ----
    let cmd = IncomingCommand {
        channel_id,
        sender_id,
        command_text: text,
        callback_data: None,
        target,
        metadata: serde_json::json!({ "source": "web" }),
        session_defaults,
    };
    manager
        .command_sender()
        .send(cmd)
        .await
        .map_err(|e| AppCommandError::task_execution_failed(format!("enqueue failed: {e}")))?;

    Ok(Json(()))
}
