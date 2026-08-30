use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use tokio::sync::Mutex;

use super::i18n::{self, Lang};
use super::manager::ChatChannelManager;
use super::session_bridge::{ActiveSession, SessionBridge};
use super::types::{
    ButtonStyle, ChannelMessageTarget, InteractiveMessage, MessageButton, MessageLevel, RichMessage,
};
use crate::acp::manager::ConnectionManager;
use crate::acp::registry::all_acp_agents;
use crate::acp::types::PromptInputBlock;
use crate::db::entities::{chat_channel_thread_binding, conversation};
use crate::db::service::{
    conversation_service, folder_service, sender_context_service, thread_binding_service,
};
use crate::db::AppDatabase;
use crate::models::agent::AgentType;
use crate::models::conversation::DbConversationSummary;
use crate::models::folder::FolderDetail;
use crate::web::event_bridge::EventEmitter;

pub struct FollowupRequest<'a> {
    pub db: &'a DatabaseConnection,
    pub text: &'a str,
    pub channel_id: i32,
    pub sender_id: &'a str,
    pub target: &'a ChannelMessageTarget,
    pub conn_mgr: &'a ConnectionManager,
    pub emitter: &'a EventEmitter,
    pub bridge: &'a Arc<Mutex<SessionBridge>>,
    pub data_dir: &'a Path,
    pub lang: Lang,
    pub prefix: &'a str,
}

pub struct CommandMessageResult {
    pub message: RichMessage,
    pub response_target: ChannelMessageTarget,
    pub extra_responses: Vec<(RichMessage, ChannelMessageTarget)>,
    pub post_action: Option<CommandPostAction>,
}

impl CommandMessageResult {
    fn current_target(message: RichMessage, target: &ChannelMessageTarget) -> Self {
        Self {
            message,
            response_target: target.clone(),
            extra_responses: Vec::new(),
            post_action: None,
        }
    }
}

pub enum CommandPostAction {
    SendLinkedPrompt {
        connection_id: String,
        folder_id: i32,
        conversation_id: i32,
        text: String,
        channel_id: i32,
        sender_id: String,
        response_target: ChannelMessageTarget,
        lang: Lang,
    },
}

pub enum SessionCommandMessage {
    Rich(RichMessage),
    Interactive(InteractiveMessage),
}

impl From<RichMessage> for SessionCommandMessage {
    fn from(message: RichMessage) -> Self {
        Self::Rich(message)
    }
}

struct CommandSessionRef {
    connection_id: String,
    conversation_id: Option<i32>,
    binding_id: Option<i32>,
}

// ── /folder ──

pub async fn handle_folder(
    db: &DatabaseConnection,
    args: &str,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    if args.is_empty() {
        return list_folders(db, channel_id, sender_id, lang, prefix).await;
    }

    // Try parse as index (1-based)
    if let Ok(idx) = args.parse::<usize>() {
        return select_folder_by_index(db, idx, channel_id, sender_id, lang, prefix).await;
    }

    // Treat as path
    select_folder_by_path(db, args, channel_id, sender_id, lang).await
}

pub async fn handle_folder_picker(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> SessionCommandMessage {
    let folders = match folder_service::list_folders(db).await {
        Ok(f) => f,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_list_folders_label(lang)))
                .into();
        }
    };

    if folders.is_empty() {
        return RichMessage::info(i18n::no_folders_found(lang))
            .with_title(i18n::folder_title(lang))
            .into();
    }

    let ctx = sender_context_service::get_or_create(db, channel_id, sender_id)
        .await
        .ok();

    let mut body = String::new();
    let mut buttons = Vec::new();
    for (i, f) in folders.iter().take(10).enumerate() {
        let current = ctx
            .as_ref()
            .and_then(|c| c.current_folder_id)
            .map(|id| id == f.id)
            .unwrap_or(false);
        let marker = if current { " [*]" } else { "" };
        body.push_str(&format!("{}. {}{} ({})\n", i + 1, f.name, marker, f.path));
        buttons.push(MessageButton {
            id: format!("cfg:folder:{}", f.id),
            label: truncate_button_label(&format!("{}. {}", i + 1, f.name), 40),
            style: ButtonStyle::Default,
        });
    }

    body.push_str(&format!("\n{}", i18n::folder_select_hint(lang, prefix)));

    SessionCommandMessage::Interactive(InteractiveMessage {
        base: RichMessage::info(body.trim_end()).with_title(i18n::folder_title(lang)),
        buttons,
        callback_context: serde_json::json!({ "kind": "folder" }),
    })
}

async fn list_folders(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let folders = match folder_service::list_folders(db).await {
        Ok(f) => f,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_list_folders_label(lang)));
        }
    };

    if folders.is_empty() {
        return RichMessage::info(i18n::no_folders_found(lang))
            .with_title(i18n::folder_title(lang));
    }

    let ctx = sender_context_service::get_or_create(db, channel_id, sender_id)
        .await
        .ok();

    let mut body = String::new();
    for (i, f) in folders.iter().take(10).enumerate() {
        let current = ctx
            .as_ref()
            .and_then(|c| c.current_folder_id)
            .map(|id| id == f.id)
            .unwrap_or(false);
        let marker = if current { " [*]" } else { "" };
        body.push_str(&format!("{}. {}{} ({})\n", i + 1, f.name, marker, f.path));
    }

    body.push_str(&format!("\n{}", i18n::folder_select_hint(lang, prefix)));

    RichMessage::info(body.trim_end()).with_title(i18n::folder_title(lang))
}

async fn select_folder_by_index(
    db: &DatabaseConnection,
    idx: usize,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    if idx == 0 {
        return RichMessage::info(i18n::index_starts_from_one(lang));
    }

    let folders = match folder_service::list_folders(db).await {
        Ok(f) => f,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_list_folders_label(lang)));
        }
    };

    let Some(folder) = folders.get(idx - 1) else {
        return RichMessage::info(i18n::folder_index_out_of_range(lang, prefix));
    };

    let _ = sender_context_service::update_folder(db, channel_id, sender_id, Some(folder.id)).await;

    RichMessage::info(format!("{} ({})", folder.name, folder.path))
        .with_title(i18n::folder_selected_title(lang))
}

async fn select_folder_by_id(
    db: &DatabaseConnection,
    folder_id: i32,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
) -> RichMessage {
    let folder = match folder_service::get_folder_by_id(db, folder_id).await {
        Ok(Some(folder)) => folder,
        _ => {
            return RichMessage::info(i18n::folder_not_found(lang));
        }
    };

    let _ = sender_context_service::update_folder(db, channel_id, sender_id, Some(folder.id)).await;

    RichMessage::info(format!("{} ({})", folder.name, folder.path))
        .with_title(i18n::folder_selected_title(lang))
}

async fn select_folder_by_path(
    db: &DatabaseConnection,
    path: &str,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
) -> RichMessage {
    let entry = match folder_service::add_folder(db, path).await {
        Ok(e) => e,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_add_folder_label(lang)));
        }
    };

    let _ = sender_context_service::update_folder(db, channel_id, sender_id, Some(entry.id)).await;

    RichMessage::info(format!("{} ({})", entry.name, entry.path))
        .with_title(i18n::folder_selected_title(lang))
}

// ── /agent ──

pub async fn handle_agent(
    db: &DatabaseConnection,
    args: &str,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    if args.is_empty() {
        return list_agents(db, channel_id, sender_id, lang, prefix).await;
    }

    // Try parse as index
    if let Ok(idx) = args.parse::<usize>() {
        return select_agent_by_index(db, idx, channel_id, sender_id, lang, prefix).await;
    }

    // Try parse as agent type name
    select_agent_by_name(db, args, channel_id, sender_id, lang).await
}

pub async fn handle_agent_picker(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> SessionCommandMessage {
    let agents = all_acp_agents();
    let ctx = sender_context_service::get_or_create(db, channel_id, sender_id)
        .await
        .ok();

    let mut body = String::new();
    let mut buttons = Vec::new();
    for (i, at) in agents.iter().enumerate() {
        let at_str = agent_type_to_string(*at);
        let current = ctx
            .as_ref()
            .and_then(|c| c.current_agent_type.as_deref())
            .map(|s| s == at_str)
            .unwrap_or(false);
        let marker = if current { " [*]" } else { "" };
        body.push_str(&format!("{}. {}{}\n", i + 1, at, marker));
        buttons.push(MessageButton {
            id: format!("cfg:agent:{at_str}"),
            label: truncate_button_label(&at.to_string(), 40),
            style: ButtonStyle::Default,
        });
    }

    body.push_str(&format!("\n{}", i18n::agent_select_hint(lang, prefix)));

    SessionCommandMessage::Interactive(InteractiveMessage {
        base: RichMessage::info(body.trim_end()).with_title(i18n::agent_title(lang)),
        buttons,
        callback_context: serde_json::json!({ "kind": "agent" }),
    })
}

async fn list_agents(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let agents = all_acp_agents();
    let ctx = sender_context_service::get_or_create(db, channel_id, sender_id)
        .await
        .ok();

    let mut body = String::new();
    for (i, at) in agents.iter().enumerate() {
        let at_str = agent_type_to_string(*at);
        let current = ctx
            .as_ref()
            .and_then(|c| c.current_agent_type.as_deref())
            .map(|s| s == at_str)
            .unwrap_or(false);
        let marker = if current { " [*]" } else { "" };
        body.push_str(&format!("{}. {}{}\n", i + 1, at, marker));
    }

    body.push_str(&format!("\n{}", i18n::agent_select_hint(lang, prefix)));

    RichMessage::info(body.trim_end()).with_title(i18n::agent_title(lang))
}

async fn select_agent_by_index(
    db: &DatabaseConnection,
    idx: usize,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let agents = all_acp_agents();
    if idx == 0 || idx > agents.len() {
        return RichMessage::info(i18n::agent_index_out_of_range(lang, prefix));
    }

    let at = agents[idx - 1];
    let at_str = agent_type_to_string(at);
    let _ = sender_context_service::update_agent(db, channel_id, sender_id, Some(at_str)).await;

    RichMessage::info(at.to_string()).with_title(i18n::agent_selected_title(lang))
}

async fn select_agent_by_name(
    db: &DatabaseConnection,
    name: &str,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
) -> RichMessage {
    let at = match parse_agent_type(name) {
        Some(a) => a,
        None => {
            return RichMessage::info(format!("{}{}", i18n::unknown_agent_label(lang), name));
        }
    };

    let at_str = agent_type_to_string(at);
    let _ = sender_context_service::update_agent(db, channel_id, sender_id, Some(at_str)).await;

    RichMessage::info(at.to_string()).with_title(i18n::agent_selected_title(lang))
}

pub async fn handle_callback(
    db: &DatabaseConnection,
    data: &str,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    // `/tasks` 列表里的会话按钮 —— 点一下切过去,与 `/tasks <编号>` 落到同一处
    // (只改路由,不起进程:下一条消息自会 auto-resume,所以切换是免费的)。
    if let Some(conv_id) = data.strip_prefix("task:") {
        let Ok(conv_id) = conv_id.parse::<i32>() else {
            return RichMessage::info(callback_expired_or_invalid(lang, prefix));
        };
        let Ok(Some(conv)) = conversation::Entity::find_by_id(conv_id).one(db).await else {
            // 会话被删了 / id 不认识 —— 按钮是旧消息上的,按"已失效"处理
            return RichMessage::info(callback_expired_or_invalid(lang, prefix));
        };
        let _ =
            sender_context_service::update_session(db, channel_id, sender_id, Some(conv_id), None)
                .await;
        let raw = conv.title.as_deref().unwrap_or("");
        let cleaned = crate::parsers::openclaw::strip_openclaw_user_prefix(raw);
        let title = if cleaned.is_empty() { raw } else { cleaned.as_str() };
        return RichMessage::info(format!("#{conv_id} {title}")).with_title(match lang {
            Lang::ZhCn | Lang::ZhTw => "已切换会话",
            _ => "Switched conversation",
        });
    }

    if let Some(folder_id) = data.strip_prefix("cfg:folder:") {
        let Ok(folder_id) = folder_id.parse::<i32>() else {
            return RichMessage::info(callback_expired_or_invalid(lang, prefix));
        };
        return select_folder_by_id(db, folder_id, channel_id, sender_id, lang).await;
    }

    if let Some(agent) = data.strip_prefix("cfg:agent:") {
        return select_agent_by_name(db, agent, channel_id, sender_id, lang).await;
    }

    RichMessage::info(callback_expired_or_invalid(lang, prefix))
}

// ── /task ──

pub async fn apply_session_defaults(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    defaults: &crate::chat_channel::types::ChannelSessionDefaults,
) -> Result<(), String> {
    let ctx = sender_context_service::get_or_create(db, channel_id, sender_id)
        .await
        .map_err(|e| e.to_string())?;
    let default_agent = if ctx.current_agent_type.is_none() {
        Some(
            parse_agent_type(&defaults.agent_type)
                .ok_or_else(|| format!("unknown default agent: {}", defaults.agent_type))?,
        )
    } else {
        None
    };

    if ctx.current_folder_id.is_none() {
        let folder_path = defaults.folder_path.trim();
        if folder_path.is_empty() {
            return Err("default folder path is empty".to_string());
        }
        let folder = folder_service::add_folder(db, folder_path)
            .await
            .map_err(|e| e.to_string())?;
        sender_context_service::update_folder(db, channel_id, sender_id, Some(folder.id))
            .await
            .map_err(|e| e.to_string())?;
    }

    if let Some(agent_type) = default_agent {
        sender_context_service::update_agent(
            db,
            channel_id,
            sender_id,
            Some(agent_type_to_string(agent_type)),
        )
        .await
        .map_err(|e| e.to_string())?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_task(
    db: &DatabaseConnection,
    task_description: &str,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    manager: &ChatChannelManager,
    conn_mgr: &ConnectionManager,
    emitter: &EventEmitter,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
    prefix: &str,
    data_dir: &Path,
) -> CommandMessageResult {
    if task_description.is_empty() {
        return CommandMessageResult::current_target(
            RichMessage::info(i18n::task_usage(lang, prefix)),
            target,
        );
    }

    if has_active_topic_session(db, bridge, target).await {
        return CommandMessageResult::current_target(
            RichMessage::info(topic_has_active_session(lang, prefix)),
            target,
        );
    }

    // 1. Load sender context
    let ctx = match sender_context_service::get_or_create(db, channel_id, sender_id).await {
        Ok(c) => c,
        Err(e) => {
            return CommandMessageResult::current_target(
                RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang))),
                target,
            );
        }
    };

    let folder_id = match ctx.current_folder_id {
        Some(id) => id,
        None => {
            return CommandMessageResult::current_target(
                RichMessage::info(i18n::no_folder_selected(lang, prefix)),
                target,
            );
        }
    };

    // 2. Get folder info
    let folder = match folder_service::get_folder_by_id(db, folder_id).await {
        Ok(Some(f)) => f,
        _ => {
            return CommandMessageResult::current_target(
                RichMessage::info(i18n::folder_not_found_with_hint(lang, prefix)),
                target,
            );
        }
    };

    // 3. Resolve agent type
    let agent_type = match resolve_agent_type(&ctx.current_agent_type, &folder.default_agent_type) {
        Some(at) => at,
        None => {
            return CommandMessageResult::current_target(
                RichMessage::info(i18n::no_agent_selected(lang, prefix)),
                target,
            );
        }
    };

    let runtime_env = match build_chat_session_runtime_env(db, agent_type, None, data_dir).await {
        Ok(env) => env,
        Err(e) => {
            return CommandMessageResult::current_target(
                RichMessage::error(format!("{}{e}", i18n::failed_to_start_agent_label(lang))),
                target,
            );
        }
    };

    let mut session_target = target.clone();
    if target.is_telegram_general_topic() {
        match manager
            .create_thread(channel_id, &truncate_topic_title(task_description))
            .await
        {
            Ok(created) => {
                session_target = created;
            }
            Err(e) => {
                return CommandMessageResult::current_target(
                    RichMessage::error(topic_create_failed(lang, &e.to_string())),
                    target,
                );
            }
        }
    }

    // 4. Create conversation record
    let conv = match conversation_service::create(
        db,
        folder_id,
        agent_type,
        Some(truncate_title(task_description)),
        folder.git_branch.clone(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return CommandMessageResult::current_target(
                RichMessage::error(format!(
                    "{}{e}",
                    i18n::failed_to_create_conversation_label(lang)
                )),
                target,
            );
        }
    };

    // 5. Spawn ACP agent
    let owner_label = owner_label_for(channel_id, sender_id, &session_target);
    let connection_id = match conn_mgr
        .spawn_agent(
            agent_type,
            Some(folder.path.clone()),
            None,
            runtime_env,
            owner_label,
            emitter.clone(),
            None,
            BTreeMap::new(),
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            // Clean up the conversation record
            let _ = conversation_service::update_status(
                db,
                conv.id,
                conversation::ConversationStatus::Cancelled,
            )
            .await;
            return CommandMessageResult::current_target(
                RichMessage::error(format!("{}{e}", i18n::failed_to_start_agent_label(lang))),
                target,
            );
        }
    };

    if session_target.is_telegram_forum_topic() {
        if let Err(e) = thread_binding_service::upsert_for_target(
            db,
            &session_target,
            "telegram",
            conv.id,
            Some(connection_id.clone()),
            sender_id,
            conv.title.clone(),
        )
        .await
        {
            let _ = conn_mgr.cancel(db, &connection_id).await;
            let _ = conversation_service::update_status(
                db,
                conv.id,
                conversation::ConversationStatus::Cancelled,
            )
            .await;
            return CommandMessageResult::current_target(
                RichMessage::error(format!("Failed to bind topic: {e}")),
                target,
            );
        }
        if let Some(title) = conv.title.as_deref() {
            manager.sync_conversation_title(db, conv.id, title).await;
        }
    }

    // 6. Register in bridge (prompt will be sent after SessionStarted event)
    {
        let session = ActiveSession {
            channel_id,
            sender_id: sender_id.to_string(),
            target: session_target.clone(),
            conversation_id: conv.id,
            connection_id: connection_id.clone(),
            agent_type,
            content_buffer: String::new(),
            tool_calls: Vec::new(),
            tool_call_inputs: std::collections::HashMap::new(),
            delegation_rendered: std::collections::HashSet::new(),
            last_flushed: Instant::now(),
            pending_prompt: None,
            permission_pending: None,
        };
        bridge.lock().await.register(connection_id.clone(), session);
    }

    // 7. Update sender context only for legacy non-topic routing.
    if !session_target.is_telegram_forum_topic() {
        let _ = sender_context_service::update_session(
            db,
            channel_id,
            sender_id,
            Some(conv.id),
            Some(connection_id.clone()),
        )
        .await;
    }

    let started_message =
        RichMessage::info(format!("[{}] #{} @ {}", agent_type, conv.id, folder.name,))
            .with_title(i18n::task_started_title(lang));
    let extra_responses = if target.is_telegram_general_topic() && session_target != *target {
        vec![(
            general_topic_task_created_message(lang, agent_type, conv.id, &folder.name),
            target.clone(),
        )]
    } else {
        Vec::new()
    };

    CommandMessageResult {
        message: started_message,
        response_target: session_target.clone(),
        extra_responses,
        post_action: Some(CommandPostAction::SendLinkedPrompt {
            connection_id,
            folder_id,
            conversation_id: conv.id,
            text: task_description.to_string(),
            channel_id,
            sender_id: sender_id.to_string(),
            response_target: session_target,
            lang,
        }),
    }
}

pub async fn handle_post_action(
    action: CommandPostAction,
    db: &DatabaseConnection,
    conn_mgr: &ConnectionManager,
    bridge: &Arc<Mutex<SessionBridge>>,
) -> Option<(RichMessage, ChannelMessageTarget)> {
    match action {
        CommandPostAction::SendLinkedPrompt {
            connection_id,
            folder_id,
            conversation_id,
            text,
            channel_id,
            sender_id,
            response_target,
            lang,
        } => {
            if let Err(e) = send_chat_prompt_linked(
                db,
                conn_mgr,
                &connection_id,
                folder_id,
                conversation_id,
                &text,
            )
            .await
            {
                bridge.lock().await.remove(&connection_id);
                if response_target.is_telegram_forum_topic() {
                    if let Ok(Some(binding)) =
                        thread_binding_service::get_by_target(db, &response_target).await
                    {
                        let _ = thread_binding_service::clear_connection(db, binding.id).await;
                    }
                } else {
                    let _ = sender_context_service::clear_session(db, channel_id, &sender_id).await;
                }
                let _ = conn_mgr.cancel(db, &connection_id).await;
                let _ = conversation_service::update_status(
                    db,
                    conversation_id,
                    conversation::ConversationStatus::Cancelled,
                )
                .await;
                return Some((
                    RichMessage::error(format!(
                        "{}{}",
                        i18n::failed_to_send_message_label(lang),
                        e
                    )),
                    response_target,
                ));
            }
            None
        }
    }
}

// ── /sessions ──

pub async fn handle_sessions(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let ctx = match sender_context_service::get_or_create(db, channel_id, sender_id).await {
        Ok(c) => c,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang)));
        }
    };

    let topic_conversation_id = if target.is_telegram_forum_topic() {
        thread_binding_service::get_by_target(db, target)
            .await
            .ok()
            .flatten()
            .map(|b| b.conversation_id)
    } else {
        None
    };
    let current_conversation_id =
        if target.is_telegram_forum_topic() || target.is_telegram_general_topic() {
            topic_conversation_id
        } else {
            ctx.current_conversation_id
        };

    let folder_id = match ctx.current_folder_id {
        Some(id) => id,
        None => {
            return RichMessage::info(i18n::no_folder_selected(lang, prefix));
        }
    };

    let folder = match folder_service::get_folder_by_id(db, folder_id).await {
        Ok(Some(f)) => f,
        _ => {
            return RichMessage::info(i18n::folder_not_found(lang));
        }
    };

    let convs = match conversation_service::list_by_folder(
        db,
        folder_id,
        None,
        None,
        None,
        Some("in_progress".to_string()),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_list_sessions_label(lang)));
        }
    };

    if convs.is_empty() {
        return RichMessage::info(i18n::no_active_sessions_in_folder(lang)).with_title(format!(
            "{} - {}",
            i18n::sessions_title(lang),
            folder.name
        ));
    }

    let mut body = String::new();
    for (i, c) in convs.iter().take(10).enumerate() {
        let title = c.title.as_deref().unwrap_or("(untitled)");
        let current = current_conversation_id
            .map(|id| id == c.id)
            .unwrap_or(false);
        let marker = if current { " [*]" } else { "" };
        body.push_str(&format!(
            "{}. [{}] {} (#{}){}  \n",
            i + 1,
            c.agent_type,
            title,
            c.id,
            marker,
        ));
    }

    body.push_str(&format!("\n{}", i18n::sessions_resume_hint(lang, prefix)));

    RichMessage::info(body.trim_end()).with_title(format!(
        "{} - {}",
        i18n::sessions_title(lang),
        folder.name
    ))
}

// ── /resume ──

#[allow(clippy::too_many_arguments)]
pub async fn handle_resume(
    db: &DatabaseConnection,
    args: &str,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    manager: &ChatChannelManager,
    conn_mgr: &ConnectionManager,
    emitter: &EventEmitter,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
    prefix: &str,
    data_dir: &Path,
) -> RichMessage {
    if args.is_empty() {
        return list_recent_sessions(db, lang, prefix).await;
    }

    let conversation_id: i32 = match args.parse() {
        Ok(id) => id,
        Err(_) => {
            return list_recent_sessions(db, lang, prefix).await;
        }
    };

    if target.is_telegram_general_topic() {
        return RichMessage::info(no_topic_session_use_task_or_resume(lang, prefix));
    }

    let conv = match conversation_service::get_by_id(db, conversation_id).await {
        Ok(c) => c,
        Err(_) => {
            return RichMessage::info(i18n::conversation_not_found(lang));
        }
    };

    if has_active_topic_session(db, bridge, target).await {
        return RichMessage::info(topic_has_active_session(lang, prefix));
    }

    let folder = match folder_service::get_folder_by_id(db, conv.folder_id).await {
        Ok(Some(f)) => f,
        _ => {
            return RichMessage::info(i18n::folder_not_found(lang));
        }
    };

    let runtime_env = match build_chat_session_runtime_env(
        db,
        conv.agent_type,
        conv.external_id.as_deref(),
        data_dir,
    )
    .await
    {
        Ok(env) => env,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_start_agent_label(lang)));
        }
    };

    // Spawn agent with session_id for resume
    let owner_label = owner_label_for(channel_id, sender_id, target);
    let connection_id = match conn_mgr
        .spawn_agent(
            conv.agent_type,
            Some(folder.path.clone()),
            conv.external_id.clone(),
            runtime_env,
            owner_label,
            emitter.clone(),
            None,
            BTreeMap::new(),
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_start_agent_label(lang)));
        }
    };

    // Register in bridge (no pending prompt for resume)
    {
        let session = ActiveSession {
            channel_id,
            sender_id: sender_id.to_string(),
            target: target.clone(),
            conversation_id: conv.id,
            connection_id: connection_id.clone(),
            agent_type: conv.agent_type,
            content_buffer: String::new(),
            tool_calls: Vec::new(),
            tool_call_inputs: std::collections::HashMap::new(),
            delegation_rendered: std::collections::HashSet::new(),
            last_flushed: Instant::now(),
            pending_prompt: None,
            permission_pending: None,
        };
        bridge.lock().await.register(connection_id.clone(), session);
    }

    if target.is_telegram_forum_topic() {
        if let Err(e) = thread_binding_service::upsert_for_target(
            db,
            target,
            "telegram",
            conv.id,
            Some(connection_id.clone()),
            sender_id,
            conv.title.clone(),
        )
        .await
        {
            let _ = conn_mgr.cancel(db, &connection_id).await;
            bridge.lock().await.remove(&connection_id);
            return RichMessage::error(format!("Failed to bind topic: {e}"));
        }
        if let Some(title) = conv.title.as_deref() {
            manager.sync_conversation_title(db, conv.id, title).await;
        }
    }

    // Update sender context only for legacy non-topic routing.
    if !target.is_telegram_forum_topic() {
        let _ = sender_context_service::update_session(
            db,
            channel_id,
            sender_id,
            Some(conv.id),
            Some(connection_id),
        )
        .await;
    }
    let _ = sender_context_service::update_folder(db, channel_id, sender_id, Some(conv.folder_id))
        .await;

    let title = conv.title.as_deref().unwrap_or("(untitled)");
    RichMessage::info(format!(
        "[{}] #{} {} @ {}",
        conv.agent_type, conv.id, title, folder.name,
    ))
    .with_title(i18n::session_resumed_title(lang))
}

// ── /cancel ──

pub async fn handle_cancel(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    conn_mgr: &ConnectionManager,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
) -> RichMessage {
    let session_ref = match command_session_ref(db, bridge, channel_id, sender_id, target).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return RichMessage::info(i18n::no_active_session_to_cancel(lang));
        }
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang)));
        }
    };

    // Cancel the ACP connection (also CAS-updates the row to Cancelled and
    // emits ConversationStatusChanged when the row is still InProgress).
    let _ = conn_mgr.cancel(db, &session_ref.connection_id).await;

    // Remove from bridge
    bridge.lock().await.remove(&session_ref.connection_id);

    // Update conversation status
    if let Some(conv_id) = session_ref.conversation_id {
        let _ = conversation_service::update_status(
            db,
            conv_id,
            conversation::ConversationStatus::Cancelled,
        )
        .await;
    }

    // Clear session from context
    if let Some(binding_id) = session_ref.binding_id {
        let _ = thread_binding_service::clear_connection(db, binding_id).await;
    } else {
        let _ = sender_context_service::clear_session(db, channel_id, sender_id).await;
    }

    RichMessage::info(i18n::task_cancelled_body(lang)).with_title(i18n::task_cancelled_title(lang))
}

// ── /approve, /deny ──

#[allow(clippy::too_many_arguments)]
pub async fn handle_permission_response(
    approve: bool,
    always: bool,
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    conn_mgr: &ConnectionManager,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
) -> RichMessage {
    let session_ref = match command_session_ref(db, bridge, channel_id, sender_id, target).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return RichMessage::info(i18n::no_active_session(lang));
        }
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang)));
        }
    };

    let pending = {
        let mut bridge_guard = bridge.lock().await;
        let session = match bridge_guard.get_mut(&session_ref.connection_id) {
            Some(s) => s,
            None => {
                if let Some(binding_id) = session_ref.binding_id {
                    let _ = thread_binding_service::clear_connection(db, binding_id).await;
                }
                return RichMessage::info(i18n::no_active_session_found(lang));
            }
        };
        session.permission_pending.take()
    };

    let pending = match pending {
        Some(p) => p,
        None => {
            return RichMessage::info(i18n::no_pending_permission(lang));
        }
    };

    // Find the appropriate option_id
    let option_id = if approve {
        pending
            .options
            .iter()
            .find(|o| o.kind == "allow" || o.kind == "allowForSession")
            .or_else(|| pending.options.first())
            .map(|o| o.option_id.clone())
    } else {
        pending
            .options
            .iter()
            .find(|o| o.kind == "deny")
            .or_else(|| pending.options.last())
            .map(|o| o.option_id.clone())
    };

    let Some(option_id) = option_id else {
        return RichMessage::info(i18n::no_valid_permission_option(lang));
    };

    if let Err(e) = conn_mgr
        .respond_permission(&session_ref.connection_id, &pending.request_id, &option_id)
        .await
    {
        return RichMessage::error(format!(
            "{}{e}",
            i18n::failed_permission_response_label(lang)
        ));
    }

    // Update auto_approve if requested
    if always && approve {
        let _ = sender_context_service::update_auto_approve(db, channel_id, sender_id, true).await;
    }

    let action = if approve {
        i18n::approved_label(lang)
    } else {
        i18n::denied_label(lang)
    };

    let mut msg = RichMessage::info(format!("{}: {}", action, pending.tool_description));
    if always && approve {
        msg = msg.with_field("", i18n::auto_approve_enabled(lang));
    }
    msg.with_title(i18n::permission_response_title(lang))
}

// ── follow-up (non-command text) ──

pub async fn handle_followup(req: FollowupRequest<'_>) -> RichMessage {
    if req.target.is_telegram_forum_topic() {
        return handle_topic_followup(req).await;
    }

    let session_ref = match command_session_ref(
        req.db,
        req.bridge,
        req.channel_id,
        req.sender_id,
        req.target,
    )
    .await
    {
        Ok(Some(s)) => s,
        Ok(None) => {
            let body = if req.target.is_telegram_forum_topic() {
                no_topic_session_use_task_or_resume(req.lang, req.prefix)
            } else {
                i18n::no_active_session_use_task(req.lang, req.prefix)
            };
            return RichMessage::info(body);
        }
        Err(e) => {
            return RichMessage::error(format!(
                "{}{e}",
                i18n::failed_to_load_context_label(req.lang)
            ));
        }
    };

    let connection_id = session_ref.connection_id;

    // Check connection exists in bridge
    {
        let bridge_guard = req.bridge.lock().await;
        if bridge_guard.get(&connection_id).is_none() {
            // Connection lost, clear context
            drop(bridge_guard);
            if let Some(binding_id) = session_ref.binding_id {
                let _ = thread_binding_service::clear_connection(req.db, binding_id).await;
            } else {
                let _ =
                    sender_context_service::clear_session(req.db, req.channel_id, req.sender_id)
                        .await;
            }
            return RichMessage::info(i18n::session_connection_lost(req.lang, req.prefix));
        }
    }

    // Send prompt to agent
    if let Err(e) = send_chat_prompt(req.conn_mgr, &connection_id, req.text).await {
        // A turn is already in flight on this (shared) connection — another
        // client, or a previous prompt still running. This is transient: the
        // connection is alive, so do NOT tear down the bridge/session. Tell the
        // user to retry once the current turn finishes.
        if matches!(e, crate::acp::error::AcpError::TurnInProgress) {
            return RichMessage::info(i18n::agent_busy_retry(req.lang).to_string());
        }
        // Otherwise the connection may have died — clean up.
        req.bridge.lock().await.remove(&connection_id);
        if let Some(binding_id) = session_ref.binding_id {
            let _ = thread_binding_service::clear_connection(req.db, binding_id).await;
        } else {
            let _ =
                sender_context_service::clear_session(req.db, req.channel_id, req.sender_id).await;
        }
        return RichMessage::error(format!(
            "{}{e}",
            i18n::failed_to_send_message_label(req.lang)
        ));
    }

    RichMessage::info(i18n::message_sent(req.lang))
}

async fn handle_topic_followup(req: FollowupRequest<'_>) -> RichMessage {
    let binding = match thread_binding_service::get_by_target(req.db, req.target).await {
        Ok(binding) => binding,
        Err(e) => {
            return RichMessage::error(format!(
                "{}{}",
                i18n::failed_to_load_context_label(req.lang),
                e
            ));
        }
    };

    let Some(binding) = binding else {
        return RichMessage::info(no_topic_session_use_task_or_resume(req.lang, req.prefix));
    };

    let bridge_session = {
        let guard = req.bridge.lock().await;
        guard
            .find_by_target(req.target)
            .map(|session| CommandSessionRef {
                connection_id: session.connection_id.clone(),
                conversation_id: Some(session.conversation_id),
                binding_id: Some(binding.id),
            })
    };

    if let Some(session_ref) = bridge_session {
        return send_followup_to_session(req, session_ref).await;
    }

    if let Some(connection_id) = binding.connection_id.as_deref() {
        let bridge_has_connection = {
            let guard = req.bridge.lock().await;
            guard.get(connection_id).is_some()
        };
        if bridge_has_connection {
            return send_followup_to_session(
                req,
                CommandSessionRef {
                    connection_id: connection_id.to_string(),
                    conversation_id: Some(binding.conversation_id),
                    binding_id: Some(binding.id),
                },
            )
            .await;
        }
        let _ = thread_binding_service::clear_connection(req.db, binding.id).await;
    }

    resume_topic_binding_and_send_followup(req, binding).await
}

async fn send_followup_to_session(
    req: FollowupRequest<'_>,
    session_ref: CommandSessionRef,
) -> RichMessage {
    let connection_id = session_ref.connection_id;
    {
        let bridge_guard = req.bridge.lock().await;
        if bridge_guard.get(&connection_id).is_none() {
            drop(bridge_guard);
            if let Some(binding_id) = session_ref.binding_id {
                let _ = thread_binding_service::clear_connection(req.db, binding_id).await;
            } else {
                let _ =
                    sender_context_service::clear_session(req.db, req.channel_id, req.sender_id)
                        .await;
            }
            return RichMessage::info(i18n::session_connection_lost(req.lang, req.prefix));
        }
    }

    if let Err(e) = send_chat_prompt(req.conn_mgr, &connection_id, req.text).await {
        if matches!(e, crate::acp::error::AcpError::TurnInProgress) {
            return RichMessage::info(i18n::agent_busy_retry(req.lang).to_string());
        }
        req.bridge.lock().await.remove(&connection_id);
        if let Some(binding_id) = session_ref.binding_id {
            let _ = thread_binding_service::clear_connection(req.db, binding_id).await;
        } else {
            let _ =
                sender_context_service::clear_session(req.db, req.channel_id, req.sender_id).await;
        }
        return RichMessage::error(format!(
            "{}{}",
            i18n::failed_to_send_message_label(req.lang),
            e
        ));
    }

    RichMessage::info(i18n::message_sent(req.lang))
}

async fn resume_topic_binding_and_send_followup(
    req: FollowupRequest<'_>,
    binding: chat_channel_thread_binding::Model,
) -> RichMessage {
    let conv = match conversation_service::get_by_id(req.db, binding.conversation_id).await {
        Ok(conv) => conv,
        Err(_) => return RichMessage::info(i18n::conversation_not_found(req.lang)),
    };
    let (connection_id, folder) = match spawn_chat_connection_for_conversation(
        req.db,
        &conv,
        req.channel_id,
        req.sender_id,
        req.target,
        req.conn_mgr,
        req.emitter,
        req.data_dir,
    )
    .await
    {
        Ok(started) => started,
        Err(e) => return RichMessage::error(topic_resume_failed(req.lang, conv.id, &e)),
    };

    let session = ActiveSession {
        channel_id: req.channel_id,
        sender_id: req.sender_id.to_string(),
        target: req.target.clone(),
        conversation_id: conv.id,
        connection_id: connection_id.clone(),
        agent_type: conv.agent_type,
        content_buffer: String::new(),
        tool_calls: Vec::new(),
        tool_call_inputs: std::collections::HashMap::new(),
        delegation_rendered: std::collections::HashSet::new(),
        last_flushed: Instant::now(),
        pending_prompt: None,
        permission_pending: None,
    };
    req.bridge
        .lock()
        .await
        .register(connection_id.clone(), session);

    if let Err(e) = thread_binding_service::upsert_for_target(
        req.db,
        req.target,
        "telegram",
        conv.id,
        Some(connection_id.clone()),
        req.sender_id,
        conv.title.clone(),
    )
    .await
    {
        let _ = req.conn_mgr.cancel(req.db, &connection_id).await;
        req.bridge.lock().await.remove(&connection_id);
        return RichMessage::error(format!("Failed to bind topic: {e}"));
    }

    let _ = sender_context_service::update_folder(
        req.db,
        req.channel_id,
        req.sender_id,
        Some(conv.folder_id),
    )
    .await;

    if let Err(e) = send_chat_prompt_linked(
        req.db,
        req.conn_mgr,
        &connection_id,
        folder.id,
        conv.id,
        req.text,
    )
    .await
    {
        req.bridge.lock().await.remove(&connection_id);
        let _ = thread_binding_service::clear_connection(req.db, binding.id).await;
        let _ = req.conn_mgr.cancel(req.db, &connection_id).await;
        return RichMessage::error(format!(
            "{}{}",
            i18n::failed_to_send_message_label(req.lang),
            e
        ));
    }

    RichMessage::info(i18n::message_sent(req.lang))
}

// ── /resume (list recent) ──

async fn list_recent_sessions(db: &DatabaseConnection, lang: Lang, prefix: &str) -> RichMessage {
    let recent = match conversation::Entity::find()
        .filter(conversation::Column::DeletedAt.is_null())
        .order_by_desc(conversation::Column::CreatedAt)
        .limit(10)
        .all(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            return RichMessage {
                title: Some(i18n::query_failed_title(lang).to_string()),
                body: e.to_string(),
                fields: Vec::new(),
                level: MessageLevel::Error,
            };
        }
    };

    if recent.is_empty() {
        return RichMessage::info(i18n::no_conversations_found(lang))
            .with_title(i18n::recent_conversations_title(lang));
    }

    let mut body = String::new();
    for conv in &recent {
        let title = conv.title.as_deref().unwrap_or(i18n::untitled(lang));
        let agent = &conv.agent_type;
        let time = conv.created_at.format("%m-%d %H:%M");
        body.push_str(&format!("#{} [{}] {} ({})\n", conv.id, agent, title, time,));
    }

    body.push_str(&format!("\n{}", i18n::recent_resume_hint(lang, prefix)));

    RichMessage::info(body.trim_end()).with_title(i18n::recent_conversations_title(lang))
}

async fn has_active_topic_session(
    db: &DatabaseConnection,
    bridge: &Arc<Mutex<SessionBridge>>,
    target: &ChannelMessageTarget,
) -> bool {
    if !target.is_telegram_forum_topic() {
        return false;
    }

    let binding = thread_binding_service::get_by_target(db, target)
        .await
        .ok()
        .flatten();

    {
        let guard = bridge.lock().await;
        if guard.find_by_target(target).is_some() {
            return true;
        }
        if let Some(binding) = &binding {
            if let Some(connection_id) = binding.connection_id.as_deref() {
                if guard.get(connection_id).is_some() {
                    return true;
                }
            }
        }
    }

    false
}

async fn command_session_ref(
    db: &DatabaseConnection,
    bridge: &Arc<Mutex<SessionBridge>>,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
) -> Result<Option<CommandSessionRef>, crate::db::error::DbError> {
    if target.is_telegram_forum_topic() {
        let binding = thread_binding_service::get_by_target(db, target).await?;
        let bridge_session = {
            let guard = bridge.lock().await;
            guard
                .find_by_target(target)
                .map(|session| CommandSessionRef {
                    connection_id: session.connection_id.clone(),
                    conversation_id: Some(session.conversation_id),
                    binding_id: binding.as_ref().map(|b| b.id),
                })
        };
        if bridge_session.is_some() {
            return Ok(bridge_session);
        }

        return Ok(binding.and_then(|b| {
            let conversation_id = b.conversation_id;
            let binding_id = b.id;
            b.connection_id.map(|connection_id| CommandSessionRef {
                connection_id,
                conversation_id: Some(conversation_id),
                binding_id: Some(binding_id),
            })
        }));
    }

    let ctx = sender_context_service::get_or_create(db, channel_id, sender_id).await?;
    Ok(ctx
        .current_connection_id
        .map(|connection_id| CommandSessionRef {
            connection_id,
            conversation_id: ctx.current_conversation_id,
            binding_id: None,
        }))
}

fn owner_label_for(channel_id: i32, sender_id: &str, target: &ChannelMessageTarget) -> String {
    if target.is_telegram_forum_topic() {
        let thread_key = target.thread_key.as_deref().unwrap_or_default();
        format!("chat_channel:{channel_id}:{sender_id}:thread:{thread_key}")
    } else {
        format!("chat_channel:{channel_id}:{sender_id}")
    }
}

fn truncate_topic_title(task_description: &str) -> String {
    let title = truncate_title(task_description);
    format!("Codeg: {title}").chars().take(128).collect()
}

async fn build_chat_session_runtime_env(
    db: &DatabaseConnection,
    agent_type: AgentType,
    session_id: Option<&str>,
    data_dir: &Path,
) -> Result<BTreeMap<String, String>, crate::acp::error::AcpError> {
    crate::commands::acp::build_session_runtime_env(
        &AppDatabase { conn: db.clone() },
        agent_type,
        session_id,
        data_dir,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn spawn_chat_connection_for_conversation(
    db: &DatabaseConnection,
    conv: &DbConversationSummary,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    conn_mgr: &ConnectionManager,
    emitter: &EventEmitter,
    data_dir: &Path,
) -> Result<(String, FolderDetail), String> {
    let folder = folder_service::get_folder_by_id(db, conv.folder_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| i18n::folder_not_found(Lang::En).to_string())?;
    let runtime_env =
        build_chat_session_runtime_env(db, conv.agent_type, conv.external_id.as_deref(), data_dir)
            .await
            .map_err(|e| e.to_string())?;
    let owner_label = owner_label_for(channel_id, sender_id, target);
    let connection_id = conn_mgr
        .spawn_agent(
            conv.agent_type,
            Some(folder.path.clone()),
            conv.external_id.clone(),
            runtime_env,
            owner_label,
            emitter.clone(),
            None,
            BTreeMap::new(),
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok((connection_id, folder))
}

async fn send_chat_prompt(
    conn_mgr: &ConnectionManager,
    connection_id: &str,
    text: &str,
) -> Result<(), crate::acp::error::AcpError> {
    conn_mgr
        .send_prompt(
            connection_id,
            vec![PromptInputBlock::Text {
                text: text.to_string(),
            }],
        )
        .await
}

async fn send_chat_prompt_linked(
    db: &DatabaseConnection,
    conn_mgr: &ConnectionManager,
    connection_id: &str,
    folder_id: i32,
    conversation_id: i32,
    text: &str,
) -> Result<(), crate::acp::error::AcpError> {
    conn_mgr
        .send_prompt_linked(
            &AppDatabase { conn: db.clone() },
            connection_id,
            vec![PromptInputBlock::Text {
                text: text.to_string(),
            }],
            Some(folder_id),
            Some(conversation_id),
            None,
        )
        .await
        .map(|_| ())
}

fn topic_has_active_session(lang: Lang, prefix: &str) -> String {
    match lang {
        Lang::ZhCn | Lang::ZhTw => {
            format!("当前 topic 已有活跃会话。请继续发送 follow-up，或先使用 {prefix}cancel。")
        }
        _ => format!(
            "This topic already has an active session. Send a follow-up or use {prefix}cancel first."
        ),
    }
}

fn no_topic_session_use_task_or_resume(lang: Lang, prefix: &str) -> String {
    match lang {
        Lang::ZhCn | Lang::ZhTw => {
            format!("当前 topic 尚未绑定会话。使用 {prefix}task <描述> 开始，或 {prefix}resume <id> 恢复。")
        }
        _ => format!(
            "This topic is not bound to a session. Use {prefix}task <description> or {prefix}resume <id>."
        ),
    }
}

fn topic_create_failed(lang: Lang, detail: &str) -> String {
    match lang {
        Lang::ZhCn | Lang::ZhTw => format!(
            "创建 Telegram topic 失败：{detail}\n请确认当前 chat 是 forum supergroup，且 bot 拥有管理 topics 权限。"
        ),
        _ => format!(
            "Failed to create Telegram topic: {detail}\nMake sure this chat is a forum supergroup and the bot can manage topics."
        ),
    }
}

fn topic_resume_failed(lang: Lang, conversation_id: i32, detail: &str) -> String {
    match lang {
        Lang::ZhCn | Lang::ZhTw => {
            format!("当前 topic 已绑定会话 #{conversation_id}，但恢复 agent 失败：{detail}")
        }
        _ => format!(
            "This topic is bound to conversation #{conversation_id}, but failed to resume the agent: {detail}"
        ),
    }
}

fn general_topic_task_created_message(
    lang: Lang,
    agent_type: AgentType,
    conversation_id: i32,
    folder_name: &str,
) -> RichMessage {
    let body = match lang {
        Lang::ZhCn | Lang::ZhTw => {
            format!(
                "已创建新 topic 并启动任务：[{}] #{} @ {}",
                agent_type, conversation_id, folder_name
            )
        }
        _ => format!(
            "Created a new topic and started task: [{}] #{} @ {}",
            agent_type, conversation_id, folder_name
        ),
    };
    RichMessage::info(body).with_title(i18n::task_started_title(lang))
}

fn callback_expired_or_invalid(lang: Lang, prefix: &str) -> String {
    match lang {
        Lang::ZhCn | Lang::ZhTw => {
            format!("这个按钮已失效。请重新发送 {prefix}folder 或 {prefix}agent。")
        }
        _ => format!("This button is no longer valid. Send {prefix}folder or {prefix}agent again."),
    }
}

fn truncate_button_label(label: &str, max_chars: usize) -> String {
    if label.chars().count() <= max_chars {
        label.to_string()
    } else {
        let mut truncated: String = label.chars().take(max_chars.saturating_sub(3)).collect();
        truncated.push_str("...");
        truncated
    }
}

// ── Helpers ──

fn agent_type_to_string(at: AgentType) -> String {
    serde_json::to_value(at)
        .ok()
        .and_then(|v| v.as_str().map(String::from))
        .unwrap_or_default()
}

fn parse_agent_type(name: &str) -> Option<AgentType> {
    let trimmed = name.trim();
    // Custom agents are `custom:<registry-id>` and their ids legitimately
    // contain `-` (`custom:qwen-code`), which the built-in normalization below
    // would mangle. Accept both the full wire form and the bare id shown in the
    // agent list — but only for ids the user has actually registered:
    // `AgentType::from_wire` validates the slug's SHAPE, not its existence, so
    // without this gate any typo would resolve to a phantom agent.
    let custom_candidate = trimmed
        .strip_prefix(crate::models::agent::CUSTOM_AGENT_WIRE_PREFIX)
        .unwrap_or(trimmed);
    if crate::acp::custom_registry::is_registered(custom_candidate) {
        if let Some(custom) = AgentType::custom(custom_candidate) {
            return Some(custom);
        }
    }
    // Built-in names are snake_case; accept the spaced/dashed spellings a user
    // is likely to type ("Claude Code", "claude-code").
    let normalized = trimmed.to_lowercase().replace([' ', '-'], "_");
    match AgentType::from_wire(&normalized) {
        Some(agent) if !agent.is_custom() => Some(agent),
        _ => None,
    }
}

fn resolve_agent_type(
    sender_agent: &Option<String>,
    folder_default: &Option<AgentType>,
) -> Option<AgentType> {
    if let Some(ref at_str) = sender_agent {
        if let Some(at) = parse_agent_type(at_str) {
            return Some(at);
        }
    }
    folder_default.as_ref().copied()
}

fn truncate_title(s: &str) -> String {
    if s.chars().count() <= 80 {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(77).collect();
        format!("{truncated}...")
    }
}

// ── Persistent-session channel UX ──────────────────────────────────────────
//
// Direct-chat (non-topic) senders keep ONE current conversation across process
// reclaims: the idle sweep may kill the agent CLI at any time, but the
// conversation row and its transcript survive, so a later message respawns the
// agent with the conversation's external_id (`session/load` restores context)
// and continues where it left off. `/new` is the only way a sender abandons
// the current conversation; `/tasks` lists and switches; `/status` inspects;
// `/models` + `/model` read/set the model.

/// Auto-resume the sender's persisted conversation for a plain-text message
/// that arrived with no live bridge session.
///
/// Returns `None` when the route is unusable (conversation/folder deleted) —
/// the caller falls through to the new-task path after the stale route was
/// cleared here. Non-topic targets only; topic threads keep their own binding
/// flow.
#[allow(clippy::too_many_arguments)]
pub async fn auto_resume_with_prompt(
    db: &DatabaseConnection,
    text: &str,
    conversation_id: i32,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    conn_mgr: &ConnectionManager,
    emitter: &EventEmitter,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
    data_dir: &Path,
) -> Option<RichMessage> {
    let conv = match conversation_service::get_by_id(db, conversation_id).await {
        Ok(c) => c,
        Err(_) => {
            // Conversation is gone — clear the stale route and start fresh.
            let _ = sender_context_service::clear_session(db, channel_id, sender_id).await;
            return None;
        }
    };
    let folder = match folder_service::get_folder_by_id(db, conv.folder_id).await {
        Ok(Some(f)) => f,
        _ => {
            let _ = sender_context_service::clear_session(db, channel_id, sender_id).await;
            return None;
        }
    };

    let runtime_env = match build_chat_session_runtime_env(
        db,
        conv.agent_type,
        conv.external_id.as_deref(),
        data_dir,
    )
    .await
    {
        Ok(env) => env,
        Err(e) => {
            return Some(RichMessage::error(format!(
                "{}{e}",
                i18n::failed_to_start_agent_label(lang)
            )));
        }
    };

    let owner_label = owner_label_for(channel_id, sender_id, target);
    let connection_id = match conn_mgr
        .spawn_agent(
            conv.agent_type,
            Some(folder.path.clone()),
            conv.external_id.clone(),
            runtime_env,
            owner_label,
            emitter.clone(),
            None,
            BTreeMap::new(),
        )
        .await
    {
        Ok(id) => id,
        Err(e) => {
            return Some(RichMessage::error(format!(
                "{}{e}",
                i18n::failed_to_start_agent_label(lang)
            )));
        }
    };

    {
        let session = ActiveSession {
            channel_id,
            sender_id: sender_id.to_string(),
            target: target.clone(),
            conversation_id: conv.id,
            connection_id: connection_id.clone(),
            agent_type: conv.agent_type,
            content_buffer: String::new(),
            tool_calls: Vec::new(),
            tool_call_inputs: std::collections::HashMap::new(),
            delegation_rendered: std::collections::HashSet::new(),
            last_flushed: Instant::now(),
            pending_prompt: None,
            permission_pending: None,
        };
        bridge.lock().await.register(connection_id.clone(), session);
    }

    let _ = sender_context_service::update_session(
        db,
        channel_id,
        sender_id,
        Some(conv.id),
        Some(connection_id.clone()),
    )
    .await;

    // Send the text DIRECTLY, not via `pending_prompt`: `spawn_agent` already
    // waited for SessionStarted internally (its dedup gate), so the
    // SessionStarted event was consumed BEFORE the bridge entry above existed
    // — a prompt parked on `pending_prompt` here would sleep forever
    // (observed on D6: resume spawned, session loaded, message never sent).
    // `pending_prompt` remains only as the busy fallback below, where the
    // TurnComplete arm retries it.
    if let Err(e) = send_chat_prompt(conn_mgr, &connection_id, text).await {
        if matches!(e, crate::acp::error::AcpError::TurnInProgress) {
            if let Some(s) = bridge.lock().await.get_mut(&connection_id) {
                s.pending_prompt = Some(text.to_string());
            }
            return Some(RichMessage::info(i18n::agent_busy_retry(lang).to_string()));
        }
        bridge.lock().await.remove(&connection_id);
        let _ = conn_mgr.cancel(db, &connection_id).await;
        let _ = sender_context_service::clear_connection(db, channel_id, sender_id).await;
        return Some(RichMessage::error(format!(
            "{}{e}",
            i18n::failed_to_send_message_label(lang)
        )));
    }

    Some(RichMessage::info(i18n::message_sent(lang)))
}

/// `/tasks` — list the sender's recent conversations in the current folder,
/// or switch to one by list number (`/tasks 2`).
#[allow(clippy::too_many_arguments)]
pub async fn handle_tasks(
    db: &DatabaseConnection,
    args: &str,
    channel_id: i32,
    sender_id: &str,
    target: &ChannelMessageTarget,
    manager: &ChatChannelManager,
    conn_mgr: &ConnectionManager,
    emitter: &EventEmitter,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
    prefix: &str,
    data_dir: &Path,
) -> SessionCommandMessage {
    let ctx = match sender_context_service::get_or_create(db, channel_id, sender_id).await {
        Ok(c) => c,
        Err(e) => {
            return SessionCommandMessage::Rich(RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang))));
        }
    };
    let Some(folder_id) = ctx.current_folder_id else {
        return SessionCommandMessage::Rich(RichMessage::info(i18n::no_folder_selected(lang, prefix)));
    };

    let mut convs = match conversation_service::list_by_folder(db, folder_id, None, None, None, None)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return SessionCommandMessage::Rich(RichMessage::error(format!("{}{e}", i18n::failed_to_list_sessions_label(lang))));
        }
    };
    convs.truncate(10);

    if convs.is_empty() {
        return SessionCommandMessage::Rich(RichMessage::info(match lang {
            Lang::ZhCn | Lang::ZhTw => "还没有会话。直接发消息即可开始新会话。".to_string(),
            _ => "No conversations yet. Just send a message to start one.".to_string(),
        }));
    }

    // `/tasks N` → switch to the Nth entry of the SAME list rendered below.
    if !args.is_empty() {
        let Ok(n) = args.trim().parse::<usize>() else {
            return SessionCommandMessage::Rich(RichMessage::info(match lang {
                Lang::ZhCn | Lang::ZhTw => format!("用法:{prefix}tasks 或 {prefix}tasks <编号>"),
                _ => format!("Usage: {prefix}tasks or {prefix}tasks <number>"),
            }));
        };
        let Some(conv) = convs.get(n.wrapping_sub(1)) else {
            return SessionCommandMessage::Rich(RichMessage::info(match lang {
                Lang::ZhCn | Lang::ZhTw => format!("编号超出范围(1-{})", convs.len()),
                _ => format!("Number out of range (1-{})", convs.len()),
            }));
        };
        // Point the route at the chosen conversation. No process is spawned
        // here — the next message auto-resumes it, so switching is free.
        let _ = sender_context_service::update_session(db, channel_id, sender_id, Some(conv.id), None)
            .await;
        let title = conv.title.as_deref().unwrap_or("(untitled)");
        let _ = manager; // reserved: title sync is topic-only today
        // 标题剥掉 OpenClaw 注入的前缀 —— 用户看的是自己说过的话,不是工作目录
        let switched_title = crate::parsers::openclaw::strip_openclaw_user_prefix(title);
        let switched_title = if switched_title.is_empty() { title } else { switched_title.as_str() };
        return SessionCommandMessage::Rich(RichMessage::info(format!("#{} {}", conv.id, switched_title))
            .with_title(match lang {
                Lang::ZhCn | Lang::ZhTw => "已切换会话",
                _ => "Switched conversation",
            }));
    }

    let _ = (conn_mgr, emitter, bridge, data_dir, target); // switching spawns nothing

    /*
     * 会话列表出成**可点按钮**,而不是一串还要照编号打字的文本。
     *
     * 按钮文案只留标题 —— agent / 模型这些在列表里是噪音(每行都一样长、把真正
     * 说过的话挤没),要看用 `/status`。标题还得剥掉 OpenClaw 注入的
     * `[Working directory: ~]`/时间戳前缀,否则十行里八行顶着同一段开头。
     *
     * 编号那条老路(`/tasks 3`)照旧可用:按钮是加法,不是替换 —— 客户端渲染不了
     * 键盘时 `to_rich_fallback()` 会把按钮摊回文本。
     */
    let buttons: Vec<MessageButton> = convs
        .iter()
        .map(|c| {
            let raw_title = c.title.as_deref().unwrap_or("");
            let cleaned = crate::parsers::openclaw::strip_openclaw_user_prefix(raw_title);
            let title = if cleaned.is_empty() {
                match lang {
                    Lang::ZhCn | Lang::ZhTw => "(未命名)".to_string(),
                    _ => "(untitled)".to_string(),
                }
            } else {
                cleaned
            };
            // 按钮文字过长会被 Telegram 挤成一坨,截短到能一眼扫完
            // 复用既有的按钮文案截断(picker 那边同一套观感)
            let label = truncate_button_label(&title, 28);
            MessageButton {
                id: format!("task:{}", c.id),
                label: if ctx.current_conversation_id == Some(c.id) {
                    format!("✓ {label}")
                } else {
                    label
                },
                style: ButtonStyle::Default,
            }
        })
        .collect();

    let hint = match lang {
        Lang::ZhCn | Lang::ZhTw => format!("点按切换 · {prefix}new 新会话"),
        _ => format!("Tap to switch · {prefix}new for a fresh one"),
    };
    let base = RichMessage::info(hint).with_title(match lang {
        Lang::ZhCn | Lang::ZhTw => "会话列表",
        _ => "Conversations",
    });
    SessionCommandMessage::Interactive(InteractiveMessage {
        base,
        buttons,
        callback_context: serde_json::json!({}),
    })
}


/// `/status` — the sender's current conversation, agent, model and whether a
/// live process is attached right now (a reclaimed one auto-revives on the
/// next message, so "idle" is informational, not an error).
pub async fn handle_session_status(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    bridge: &Arc<Mutex<SessionBridge>>,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let ctx = match sender_context_service::get_or_create(db, channel_id, sender_id).await {
        Ok(c) => c,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang)));
        }
    };
    let Some(conv_id) = ctx.current_conversation_id else {
        return RichMessage::info(match lang {
            Lang::ZhCn | Lang::ZhTw => {
                format!("当前没有进行中的会话。直接发消息开始,或 {prefix}tasks 查看历史。")
            }
            _ => format!(
                "No current conversation. Send a message to start one, or {prefix}tasks to browse."
            ),
        });
    };
    let conv = match conversation_service::get_by_id(db, conv_id).await {
        Ok(c) => c,
        Err(_) => {
            return RichMessage::info(i18n::conversation_not_found(lang));
        }
    };
    let live = {
        let guard = bridge.lock().await;
        guard.find_by_sender(channel_id, sender_id).is_some()
    };
    let title = conv.title.as_deref().unwrap_or("(untitled)");
    let model = current_model_label(db, conv.agent_type, conv.external_id.as_deref()).await;
    let state_label = match (live, lang) {
        (true, Lang::ZhCn | Lang::ZhTw) => "运行中",
        (true, _) => "live",
        (false, Lang::ZhCn | Lang::ZhTw) => "待机(下条消息自动唤醒)",
        (false, _) => "idle (auto-revives on next message)",
    };
    RichMessage::info(format!("#{} {}", conv.id, title))
        .with_title(match lang {
            Lang::ZhCn | Lang::ZhTw => "当前会话",
            _ => "Current conversation",
        })
        .with_field("Agent", conv.agent_type.to_string())
        .with_field(
            match lang {
                Lang::ZhCn | Lang::ZhTw => "模型",
                _ => "Model",
            },
            model,
        )
        .with_field(
            match lang {
                Lang::ZhCn | Lang::ZhTw => "状态",
                _ => "State",
            },
            state_label.to_string(),
        )
}

/// `/new` — abandon the current conversation route. The next message starts a
/// brand-new conversation; the old one stays in `/tasks` and can be switched
/// back to at any time.
pub async fn handle_new_session(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    bridge: &Arc<Mutex<SessionBridge>>,
    conn_mgr: &ConnectionManager,
    lang: Lang,
) -> RichMessage {
    // Drop the live bridge session (if any) so the old connection can't hijack
    // the next message; the process itself is left to the idle sweep.
    let dropped = {
        let mut guard = bridge.lock().await;
        let conn_id = guard
            .find_by_sender(channel_id, sender_id)
            .map(|s| s.connection_id.clone());
        if let Some(ref id) = conn_id {
            guard.remove(id);
        }
        conn_id
    };
    if let Some(conn_id) = dropped {
        let _ = conn_mgr.disconnect(&conn_id).await;
    }
    let _ = sender_context_service::clear_session(db, channel_id, sender_id).await;
    RichMessage::info(match lang {
        Lang::ZhCn | Lang::ZhTw => "已开启新会话,下一条消息生效。",
        _ => "Fresh conversation started — your next message begins it.",
    })
}

// ── /models · /model ───────────────────────────────────────────────────────

/// The openclaw gateway config on disk. Providers carry the model catalog;
/// `agents.defaults.model.primary` is the agent-level default.
fn read_openclaw_config() -> Option<serde_json::Value> {
    let path = dirs::home_dir()?.join(".openclaw").join("openclaw.json");
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// All `provider/model` ids from the gateway's provider blocks.
fn openclaw_catalog(cfg: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(provs) = cfg
        .get("models")
        .and_then(|m| m.get("providers"))
        .and_then(|p| p.as_object())
    {
        for (pid, blk) in provs {
            if let Some(models) = blk.get("models").and_then(|m| m.as_array()) {
                for m in models {
                    if let Some(id) = m.get("id").and_then(|i| i.as_str()) {
                        out.push(format!("{pid}/{id}"));
                    }
                }
            }
        }
    }
    out
}

fn openclaw_default_model(cfg: &serde_json::Value) -> Option<String> {
    cfg.get("agents")?
        .get("defaults")?
        .get("model")?
        .get("primary")?
        .as_str()
        .map(|s| s.to_string())
}

/// The gateway session key an openclaw conversation's bridge is attached to:
/// an explicit `OPENCLAW_SESSION_KEY` in the agent env wins (the bridge was
/// spawned with `--session <key>`); otherwise the bridge auto-mints
/// `agent:main:acp-bridge:<acp-session-id>` (verified on-disk in the
/// gateway's sessions.json).
async fn openclaw_session_key_for(db: &DatabaseConnection, external_id: &str) -> String {
    if let Ok(Some(setting)) =
        crate::db::service::agent_setting_service::get_by_agent_type(db, AgentType::OpenClaw).await
    {
        if let Some(env_json) = setting.env_json.as_deref() {
            if let Ok(env) = serde_json::from_str::<BTreeMap<String, String>>(env_json) {
                if let Some(key) = env.get("OPENCLAW_SESSION_KEY").filter(|v| !v.is_empty()) {
                    return key.clone();
                }
            }
        }
    }
    format!("agent:main:acp-bridge:{external_id}")
}

/// Run `openclaw <args...>`, optionally feeding stdin, and capture stdout.
async fn run_openclaw_cli(args: &[&str], stdin: Option<&str>) -> Result<String, String> {
    let bin = crate::commands::acp::resolve_npx_command("openclaw")
        .await
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "openclaw".to_string());
    let mut cmd = tokio::process::Command::new(bin);
    cmd.args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("spawn openclaw: {e}"))?;
    if let Some(input) = stdin {
        use tokio::io::AsyncWriteExt;
        if let Some(mut pipe) = child.stdin.take() {
            let _ = pipe.write_all(input.as_bytes()).await;
        }
    } else {
        drop(child.stdin.take());
    }
    let out = tokio::time::timeout(std::time::Duration::from_secs(20), child.wait_with_output())
        .await
        .map_err(|_| "openclaw CLI timed out".to_string())?
        .map_err(|e| format!("openclaw CLI: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr)
            .trim()
            .chars()
            .take(300)
            .collect());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// One `sessions.patch` against the gateway. `gateway call` exits 0 even when
/// the method fails — the truth is the JSON `ok` field.
async fn openclaw_sessions_patch(session_key: &str, model: &str) -> Result<(), String> {
    let params = serde_json::json!({ "key": session_key, "model": model }).to_string();
    let stdout = run_openclaw_cli(
        &["gateway", "call", "sessions.patch", "--params", &params, "--json"],
        None,
    )
    .await?;
    let parsed: serde_json::Value =
        serde_json::from_str(stdout.trim()).map_err(|_| format!("unparseable: {stdout}"))?;
    if parsed.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        return Ok(());
    }
    Err(parsed
        .get("error")
        .and_then(|e| e.get("message").or_else(|| e.get("code")))
        .and_then(|v| v.as_str())
        .unwrap_or("gateway returned ok=false")
        .to_string())
}

/// Register every provider-block model into `agents.defaults.models` (the
/// session-model whitelist). Stock instances only whitelist the primary, so
/// `sessions.patch` for anything else fails "model not allowed" until healed.
async fn openclaw_heal_whitelist() -> Result<(), String> {
    let cfg = read_openclaw_config().ok_or("cannot read openclaw.json")?;
    let ids = openclaw_catalog(&cfg);
    if ids.is_empty() {
        return Err("no provider models found".into());
    }
    let mut wl = serde_json::Map::new();
    for id in ids {
        wl.insert(id, serde_json::json!({}));
    }
    let patch =
        serde_json::json!({ "agents": { "defaults": { "models": serde_json::Value::Object(wl) } } });
    run_openclaw_cli(&["config", "patch", "--stdin"], Some(&patch.to_string()))
        .await
        .map(|_| ())
}

/// Best-effort "which model is this conversation on" label for `/status` and
/// `/models`. openclaw: agent default (a session override set via `/model` is
/// reported when we can read it back; absent one, the default applies).
/// Other agents: the sole `*MODEL*` env entry, if any.
async fn current_model_label(
    db: &DatabaseConnection,
    agent_type: AgentType,
    _external_id: Option<&str>,
) -> String {
    if agent_type == AgentType::OpenClaw {
        return read_openclaw_config()
            .and_then(|cfg| openclaw_default_model(&cfg))
            .unwrap_or_else(|| "default".into());
    }
    if let Ok(Some(setting)) =
        crate::db::service::agent_setting_service::get_by_agent_type(db, agent_type).await
    {
        if let Some(env_json) = setting.env_json.as_deref() {
            if let Ok(env) = serde_json::from_str::<BTreeMap<String, String>>(env_json) {
                let models: Vec<_> = env
                    .iter()
                    .filter(|(k, _)| k.contains("MODEL"))
                    .map(|(_, v)| v.clone())
                    .collect();
                if let [one] = models.as_slice() {
                    return one.clone();
                }
            }
        }
    }
    "default".into()
}

/// `/models` — the current conversation's agent, its active model, and (for
/// openclaw) the catalog of switchable models.
pub async fn handle_models(
    db: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let ctx = match sender_context_service::get_or_create(db, channel_id, sender_id).await {
        Ok(c) => c,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang)));
        }
    };
    let fallback_agent = ctx
        .current_agent_type
        .as_deref()
        .and_then(parse_agent_type)
        .unwrap_or(AgentType::OpenClaw);
    let agent_type = match ctx.current_conversation_id {
        Some(conv_id) => match conversation_service::get_by_id(db, conv_id).await {
            Ok(c) => c.agent_type,
            Err(_) => fallback_agent,
        },
        None => fallback_agent,
    };

    let current = current_model_label(db, agent_type, None).await;
    let mut body = format!(
        "{}: {}\n",
        match lang {
            Lang::ZhCn | Lang::ZhTw => "当前模型",
            _ => "Current model",
        },
        current
    );

    if agent_type == AgentType::OpenClaw {
        match read_openclaw_config() {
            Some(cfg) => {
                let catalog = openclaw_catalog(&cfg);
                if catalog.is_empty() {
                    body.push_str(match lang {
                        Lang::ZhCn | Lang::ZhTw => "(未发现可用模型目录)",
                        _ => "(no model catalog found)",
                    });
                } else {
                    body.push('\n');
                    for id in &catalog {
                        // Bare model name is what sessions.patch accepts.
                        let bare = id.split('/').next_back().unwrap_or(id);
                        body.push_str(&format!("· {bare}\n"));
                    }
                    body.push_str(&match lang {
                        Lang::ZhCn | Lang::ZhTw => format!("\n{prefix}model <名称> 切换本会话模型"),
                        _ => format!("\n{prefix}model <name> to switch this conversation"),
                    });
                }
            }
            None => body.push_str(match lang {
                Lang::ZhCn | Lang::ZhTw => "(读取 openclaw 配置失败)",
                _ => "(failed to read openclaw config)",
            }),
        }
    } else {
        body.push_str(&match lang {
            Lang::ZhCn | Lang::ZhTw => {
                format!("{prefix}model <名称> 修改该 Agent 的模型(下一条消息生效)")
            }
            _ => format!("{prefix}model <name> sets this agent's model (takes effect next message)"),
        });
    }

    RichMessage::info(body.trim_end())
        .with_title(match lang {
            Lang::ZhCn | Lang::ZhTw => "模型",
            _ => "Models",
        })
        .with_field("Agent", agent_type.to_string())
}

/// `/model <name>` — set the model. openclaw: per-conversation, via the
/// gateway's `sessions.patch` (immediate, with a one-shot whitelist heal on
/// "model not allowed"). Other agents: rewrite the sole `*MODEL*` env entry
/// and drop the live connection so the next message respawns with it.
#[allow(clippy::too_many_arguments)]
pub async fn handle_model_set(
    db: &DatabaseConnection,
    args: &str,
    channel_id: i32,
    sender_id: &str,
    bridge: &Arc<Mutex<SessionBridge>>,
    conn_mgr: &ConnectionManager,
    lang: Lang,
    prefix: &str,
) -> RichMessage {
    let model = args.trim();
    if model.is_empty() {
        return handle_models(db, channel_id, sender_id, lang, prefix).await;
    }
    let ctx = match sender_context_service::get_or_create(db, channel_id, sender_id).await {
        Ok(c) => c,
        Err(e) => {
            return RichMessage::error(format!("{}{e}", i18n::failed_to_load_context_label(lang)));
        }
    };

    let fallback_agent = ctx
        .current_agent_type
        .as_deref()
        .and_then(parse_agent_type)
        .unwrap_or(AgentType::OpenClaw);
    let (agent_type, external_id) = match ctx.current_conversation_id {
        Some(conv_id) => match conversation_service::get_by_id(db, conv_id).await {
            Ok(c) => (c.agent_type, c.external_id),
            Err(_) => (fallback_agent, None),
        },
        None => (fallback_agent, None),
    };

    if agent_type == AgentType::OpenClaw {
        let Some(ext) = external_id.as_deref() else {
            return RichMessage::info(match lang {
                Lang::ZhCn | Lang::ZhTw => "当前会话还没有底层 agent 会话,先发一条消息再切模型。",
                _ => "This conversation has no agent session yet — send a message first.",
            });
        };
        let key = openclaw_session_key_for(db, ext).await;
        let mut result = openclaw_sessions_patch(&key, model).await;
        if let Err(ref e) = result {
            if e.to_lowercase().contains("model not allowed") {
                if openclaw_heal_whitelist().await.is_ok() {
                    result = openclaw_sessions_patch(&key, model).await;
                }
            }
        }
        return match result {
            Ok(()) => RichMessage::info(format!(
                "{} → {model}",
                match lang {
                    Lang::ZhCn | Lang::ZhTw => "本会话模型已切换",
                    _ => "Conversation model switched",
                }
            )),
            Err(e) => RichMessage::error(format!(
                "{}: {e}",
                match lang {
                    Lang::ZhCn | Lang::ZhTw => "切换失败",
                    _ => "Switch failed",
                }
            )),
        };
    }

    // Generic agents: rewrite the single *MODEL* env entry.
    let Ok(Some(setting)) =
        crate::db::service::agent_setting_service::get_by_agent_type(db, agent_type).await
    else {
        return RichMessage::error("agent setting not found".to_string());
    };
    let mut env: BTreeMap<String, String> = setting
        .env_json
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();
    let model_keys: Vec<String> = env
        .keys()
        .filter(|k| k.contains("MODEL"))
        .cloned()
        .collect();
    let [key] = model_keys.as_slice() else {
        return RichMessage::info(match lang {
            Lang::ZhCn | Lang::ZhTw => format!("该 Agent({agent_type})暂不支持渠道内切模型。"),
            _ => format!("Model switching isn't supported for this agent ({agent_type}) here."),
        });
    };
    env.insert(key.clone(), model.to_string());
    let env_json = serde_json::to_string(&env).ok();
    if let Err(e) = crate::db::service::agent_setting_service::update(
        db,
        agent_type,
        crate::db::service::agent_setting_service::AgentSettingsUpdate {
            enabled: setting.enabled,
            env_json,
            model_provider_id: setting.model_provider_id,
        },
    )
    .await
    {
        return RichMessage::error(format!("save failed: {e}"));
    }

    // Drop the live connection so the next message respawns with the new env
    // (auto-resume keeps the conversation and its context).
    let conn_id = {
        let mut guard = bridge.lock().await;
        let id = guard
            .find_by_sender(channel_id, sender_id)
            .map(|s| s.connection_id.clone());
        if let Some(ref id) = id {
            guard.remove(id);
        }
        id
    };
    if let Some(id) = conn_id {
        let _ = conn_mgr.disconnect(&id).await;
    }

    RichMessage::info(format!(
        "{} → {model}",
        match lang {
            Lang::ZhCn | Lang::ZhTw => "模型已更新,下一条消息生效",
            _ => "Model updated — takes effect on your next message",
        }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acp::connection::ConnectionCommand;
    use crate::db::service::{agent_setting_service, chat_channel_service, sender_context_service};
    use crate::db::test_helpers::{fresh_in_memory_db, seed_conversation, seed_folder};

    async fn seed_chat_channel(db: &crate::db::AppDatabase) -> i32 {
        chat_channel_service::create(
            &db.conn,
            "Telegram test".to_string(),
            "telegram".to_string(),
            serde_json::json!({ "chat_id": "-100123", "topic_mode": true }).to_string(),
            true,
            false,
            None,
        )
        .await
        .expect("seed chat channel")
        .id
    }

    #[tokio::test]
    async fn callback_folder_selection_updates_sender_context() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-callback-folder").await;

        let message = handle_callback(
            &db.conn,
            &format!("cfg:folder:{folder_id}"),
            channel_id,
            "sender-1",
            Lang::En,
            "/",
        )
        .await;
        let ctx = sender_context_service::get_or_create(&db.conn, channel_id, "sender-1")
            .await
            .expect("context");

        assert_eq!(ctx.current_folder_id, Some(folder_id));
        assert_eq!(message.title.as_deref(), Some("Folder Selected"));
    }

    #[tokio::test]
    async fn callback_agent_selection_updates_sender_context() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;

        let message = handle_callback(
            &db.conn,
            "cfg:agent:codex",
            channel_id,
            "sender-1",
            Lang::En,
            "/",
        )
        .await;
        let ctx = sender_context_service::get_or_create(&db.conn, channel_id, "sender-1")
            .await
            .expect("context");

        assert_eq!(ctx.current_agent_type.as_deref(), Some("codex"));
        assert_eq!(message.title.as_deref(), Some("Agent Selected"));
    }

    #[tokio::test]
    async fn direct_chat_defaults_fill_missing_sender_context() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let defaults = crate::chat_channel::types::ChannelSessionDefaults {
            folder_path: "/tmp/codeg-direct-default".to_string(),
            agent_type: "codex".to_string(),
        };

        apply_session_defaults(&db.conn, channel_id, "sender-1", &defaults)
            .await
            .expect("apply defaults");

        let ctx = sender_context_service::get_or_create(&db.conn, channel_id, "sender-1")
            .await
            .expect("context");
        assert!(ctx.current_folder_id.is_some());
        assert_eq!(ctx.current_agent_type.as_deref(), Some("codex"));
    }

    #[tokio::test]
    async fn direct_chat_defaults_do_not_override_explicit_sender_choices() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let selected_folder = seed_folder(&db, "/tmp/codeg-selected-folder").await;
        sender_context_service::update_folder(
            &db.conn,
            channel_id,
            "sender-1",
            Some(selected_folder),
        )
        .await
        .expect("select folder");
        sender_context_service::update_agent(
            &db.conn,
            channel_id,
            "sender-1",
            Some("claude_code".to_string()),
        )
        .await
        .expect("select agent");
        let defaults = crate::chat_channel::types::ChannelSessionDefaults {
            folder_path: "/tmp/codeg-direct-default".to_string(),
            agent_type: "codex".to_string(),
        };

        apply_session_defaults(&db.conn, channel_id, "sender-1", &defaults)
            .await
            .expect("apply defaults");

        let ctx = sender_context_service::get_or_create(&db.conn, channel_id, "sender-1")
            .await
            .expect("context");
        assert_eq!(ctx.current_folder_id, Some(selected_folder));
        assert_eq!(ctx.current_agent_type.as_deref(), Some("claude_code"));
    }

    #[tokio::test]
    async fn folder_picker_button_label_separates_index_and_name() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        seed_folder(&db, "/tmp/codeg-picker-label").await;

        let message = handle_folder_picker(&db.conn, channel_id, "sender-1", Lang::En, "/").await;
        let SessionCommandMessage::Interactive(message) = message else {
            panic!("folder picker should return interactive message");
        };

        assert_eq!(message.buttons[0].label, "1. codeg-picker-label");
    }

    #[tokio::test]
    async fn sessions_in_general_topic_do_not_mark_sender_context_session_current() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-general").await;
        let legacy_conv = seed_conversation(&db, folder_id, AgentType::Codex).await;
        let _other_conv = seed_conversation(&db, folder_id, AgentType::OpenCode).await;
        sender_context_service::update_folder(&db.conn, channel_id, "sender-1", Some(folder_id))
            .await
            .expect("folder context");
        sender_context_service::update_session(
            &db.conn,
            channel_id,
            "sender-1",
            Some(legacy_conv),
            Some("legacy-connection".to_string()),
        )
        .await
        .expect("session context");

        let message = handle_sessions(
            &db.conn,
            channel_id,
            "sender-1",
            &ChannelMessageTarget::telegram_general(channel_id, "-100123"),
            Lang::En,
            "/",
        )
        .await;

        assert!(!message.body.contains("[*]"));
    }

    #[tokio::test]
    async fn sessions_in_forum_topic_mark_bound_conversation_not_sender_context() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-bound").await;
        let legacy_conv = seed_conversation(&db, folder_id, AgentType::Codex).await;
        let topic_conv = seed_conversation(&db, folder_id, AgentType::OpenCode).await;
        sender_context_service::update_folder(&db.conn, channel_id, "sender-1", Some(folder_id))
            .await
            .expect("folder context");
        sender_context_service::update_session(
            &db.conn,
            channel_id,
            "sender-1",
            Some(legacy_conv),
            Some("legacy-connection".to_string()),
        )
        .await
        .expect("session context");
        let target = ChannelMessageTarget::telegram_forum_topic(channel_id, "-100123", "2");
        thread_binding_service::upsert_for_target(
            &db.conn,
            &target,
            "telegram",
            topic_conv,
            Some("topic-connection".to_string()),
            "sender-1",
            Some("Topic session".to_string()),
        )
        .await
        .expect("thread binding");

        let message =
            handle_sessions(&db.conn, channel_id, "sender-1", &target, Lang::En, "/").await;

        assert!(message.body.contains(&format!("(#{topic_conv}) [*]")));
        assert!(!message.body.contains(&format!("(#{legacy_conv}) [*]")));
    }

    #[tokio::test]
    async fn resume_rejects_active_topic_even_for_same_conversation() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-resume-active").await;
        let conv_id = seed_conversation(&db, folder_id, AgentType::Codex).await;
        let target = ChannelMessageTarget::telegram_forum_topic(channel_id, "-100123", "2");
        let bridge = Arc::new(Mutex::new(SessionBridge::new()));
        bridge.lock().await.register(
            "active-connection".to_string(),
            ActiveSession {
                channel_id,
                sender_id: "sender-1".to_string(),
                target: target.clone(),
                conversation_id: conv_id,
                connection_id: "active-connection".to_string(),
                agent_type: AgentType::Codex,
                content_buffer: String::new(),
                tool_calls: Vec::new(),
                tool_call_inputs: std::collections::HashMap::new(),
                delegation_rendered: std::collections::HashSet::new(),
                last_flushed: Instant::now(),
                pending_prompt: None,
                permission_pending: None,
            },
        );

        let message = handle_resume(
            &db.conn,
            &conv_id.to_string(),
            channel_id,
            "sender-1",
            &target,
            &ChatChannelManager::new(),
            &ConnectionManager::new(),
            &EventEmitter::Noop,
            &bridge,
            Lang::En,
            "/",
            std::path::Path::new("/tmp/codeg-topic-resume-data"),
        )
        .await;

        assert!(message.body.contains("already has an active session"));
        assert_eq!(bridge.lock().await.all_sessions().count(), 1);
    }

    #[tokio::test]
    async fn permission_response_clears_stale_topic_binding_connection() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-stale-permission").await;
        let conv_id = seed_conversation(&db, folder_id, AgentType::Codex).await;
        let target = ChannelMessageTarget::telegram_forum_topic(channel_id, "-100123", "2");
        let binding = thread_binding_service::upsert_for_target(
            &db.conn,
            &target,
            "telegram",
            conv_id,
            Some("missing-connection".to_string()),
            "sender-1",
            Some("Topic session".to_string()),
        )
        .await
        .expect("thread binding");
        let bridge = Arc::new(Mutex::new(SessionBridge::new()));

        let message = handle_permission_response(
            true,
            false,
            &db.conn,
            channel_id,
            "sender-1",
            &target,
            &ConnectionManager::new(),
            &bridge,
            Lang::En,
        )
        .await;
        let refreshed = thread_binding_service::get_by_target(&db.conn, &target)
            .await
            .expect("load binding")
            .expect("binding exists");

        assert_eq!(refreshed.id, binding.id);
        assert!(refreshed.connection_id.is_none());
        assert!(message.body.contains("No active session"));
    }

    #[tokio::test]
    async fn task_uses_agent_settings_before_spawning() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-disabled-agent").await;
        sender_context_service::update_folder(&db.conn, channel_id, "sender-1", Some(folder_id))
            .await
            .expect("folder context");
        sender_context_service::update_agent(
            &db.conn,
            channel_id,
            "sender-1",
            Some("codex".to_string()),
        )
        .await
        .expect("agent context");
        agent_setting_service::ensure_defaults(
            &db.conn,
            &[agent_setting_service::AgentDefaultInput {
                agent_type: AgentType::Codex,
                registry_id: "codex".to_string(),
                default_sort_order: 0,
            }],
        )
        .await
        .expect("agent defaults");
        agent_setting_service::update(
            &db.conn,
            AgentType::Codex,
            agent_setting_service::AgentSettingsUpdate {
                enabled: false,
                env_json: None,
                model_provider_id: None,
            },
        )
        .await
        .expect("disable agent");
        let bridge = Arc::new(Mutex::new(SessionBridge::new()));
        let target = ChannelMessageTarget::telegram_general(channel_id, "-100123");

        let result = handle_task(
            &db.conn,
            "use saved model config",
            channel_id,
            "sender-1",
            &target,
            &ChatChannelManager::new(),
            &ConnectionManager::new(),
            &EventEmitter::Noop,
            &bridge,
            Lang::En,
            "/",
            std::path::Path::new("/tmp/codeg-topic-disabled-agent-data"),
        )
        .await;

        assert!(result.message.body.contains("disabled in settings"));
        assert_eq!(bridge.lock().await.all_sessions().count(), 0);
    }

    #[tokio::test]
    async fn linked_chat_prompt_enqueues_initial_task_prompt() {
        let db = fresh_in_memory_db().await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-linked-prompt").await;
        let conv_id = seed_conversation(&db, folder_id, AgentType::OpenCode).await;
        let conn_mgr = ConnectionManager::new();
        let mut rx = conn_mgr
            .insert_test_connection_live(
                "conn-linked",
                AgentType::OpenCode,
                Some(std::path::PathBuf::from("/tmp/codeg-topic-linked-prompt")),
                EventEmitter::Noop,
            )
            .await;

        send_chat_prompt_linked(
            &db.conn,
            &conn_mgr,
            "conn-linked",
            folder_id,
            conv_id,
            "first task prompt",
        )
        .await
        .expect("linked prompt send");

        let command = rx.recv().await.expect("prompt command");
        let ConnectionCommand::Prompt { blocks, .. } = command else {
            panic!("expected prompt command");
        };
        assert!(matches!(
            &blocks[0],
            PromptInputBlock::Text { text } if text == "first task prompt"
        ));
    }

    #[tokio::test]
    async fn topic_followup_uses_active_bound_session() {
        let db = fresh_in_memory_db().await;
        let channel_id = seed_chat_channel(&db).await;
        let folder_id = seed_folder(&db, "/tmp/codeg-topic-followup-active").await;
        let conv_id = seed_conversation(&db, folder_id, AgentType::OpenCode).await;
        let target = ChannelMessageTarget::telegram_forum_topic(channel_id, "-100123", "2");
        thread_binding_service::upsert_for_target(
            &db.conn,
            &target,
            "telegram",
            conv_id,
            Some("conn-followup".to_string()),
            "sender-1",
            Some("Topic session".to_string()),
        )
        .await
        .expect("thread binding");
        let conn_mgr = ConnectionManager::new();
        let mut rx = conn_mgr
            .insert_test_connection_live(
                "conn-followup",
                AgentType::OpenCode,
                Some(std::path::PathBuf::from("/tmp/codeg-topic-followup-active")),
                EventEmitter::Noop,
            )
            .await;
        let bridge = Arc::new(Mutex::new(SessionBridge::new()));
        bridge.lock().await.register(
            "conn-followup".to_string(),
            ActiveSession {
                channel_id,
                sender_id: "sender-1".to_string(),
                target: target.clone(),
                conversation_id: conv_id,
                connection_id: "conn-followup".to_string(),
                agent_type: AgentType::OpenCode,
                content_buffer: String::new(),
                tool_calls: Vec::new(),
                tool_call_inputs: std::collections::HashMap::new(),
                delegation_rendered: std::collections::HashSet::new(),
                last_flushed: Instant::now(),
                pending_prompt: None,
                permission_pending: None,
            },
        );

        let message = handle_followup(FollowupRequest {
            db: &db.conn,
            text: "continue task",
            channel_id,
            sender_id: "sender-1",
            target: &target,
            conn_mgr: &conn_mgr,
            emitter: &EventEmitter::Noop,
            bridge: &bridge,
            data_dir: std::path::Path::new("/tmp/codeg-topic-followup-data"),
            lang: Lang::En,
            prefix: "/",
        })
        .await;

        assert_eq!(message.body, i18n::message_sent(Lang::En));
        let command = rx.recv().await.expect("prompt command");
        let ConnectionCommand::Prompt { blocks, .. } = command else {
            panic!("expected prompt command");
        };
        assert!(matches!(
            &blocks[0],
            PromptInputBlock::Text { text } if text == "continue task"
        ));
    }

    #[test]
    fn parses_builtin_agent_names_in_the_spellings_users_type() {
        assert_eq!(parse_agent_type("claude_code"), Some(AgentType::ClaudeCode));
        assert_eq!(parse_agent_type("claude-code"), Some(AgentType::ClaudeCode));
        assert_eq!(parse_agent_type("Claude Code"), Some(AgentType::ClaudeCode));
        assert_eq!(parse_agent_type("  codex  "), Some(AgentType::Codex));
        assert_eq!(parse_agent_type("not-an-agent"), None);
    }

    #[test]
    fn parses_custom_agents_without_mangling_dashes_in_their_ids() {
        // `hydrate` publishes a process-global map; share the registry's own
        // lock so this can't race the custom-registry tests.
        let _guard = crate::acp::custom_registry::hydrate_test_guard();
        let def = crate::acp::custom_registry::CustomAgentDef {
            registry_id: "chat-cmd-qwen-code".into(),
            name: "Qwen Code".into(),
            description: String::new(),
            version: "0.21.0".into(),
            distribution_kind: crate::acp::custom_registry::CustomDistributionKind::Npx,
            spec: crate::acp::custom_registry::CustomAgentSpec {
                npx: Some(crate::acp::custom_registry::NpxSpec {
                    package: "@qwen-code/qwen-code@0.21.0".into(),
                    cmd: Some("qwen".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            icon_url: None,
            skills_shared_store: false,
            skills_dir: None,
            source: Default::default(),
            version_probe: None,
            supports_mcp: true,
        };
        crate::acp::custom_registry::hydrate(std::slice::from_ref(&def));

        let expected = AgentType::custom("chat-cmd-qwen-code").unwrap();
        // Full wire form, and the bare id as shown in the agent list. The old
        // `-` → `_` normalization would have produced an unregistered id.
        assert_eq!(
            parse_agent_type("custom:chat-cmd-qwen-code"),
            Some(expected)
        );
        assert_eq!(parse_agent_type("chat-cmd-qwen-code"), Some(expected));
        // Underscore spelling is NOT the registered id, and the built-in
        // normalization must not invent a phantom custom agent from it.
        assert_eq!(parse_agent_type("chat_cmd_qwen_code"), None);
        // An unregistered id is unknown, not a silently-accepted agent —
        // `AgentType::custom` validates a slug's shape, never its existence.
        assert_eq!(parse_agent_type("custom:never-registered"), None);
        assert_eq!(parse_agent_type("never-registered"), None);

        crate::acp::custom_registry::hydrate(&[]);
        assert_eq!(parse_agent_type("chat-cmd-qwen-code"), None);
    }
}
