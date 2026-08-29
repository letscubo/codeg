use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ActiveValue::NotSet, ColumnTrait, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, Set,
};

use crate::db::entities::chat_channel_sender_context;
use crate::db::error::DbError;

pub async fn get_or_create(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
) -> Result<chat_channel_sender_context::Model, DbError> {
    let existing = chat_channel_sender_context::Entity::find()
        .filter(chat_channel_sender_context::Column::ChannelId.eq(channel_id))
        .filter(chat_channel_sender_context::Column::SenderId.eq(sender_id))
        .one(conn)
        .await?;

    if let Some(model) = existing {
        return Ok(model);
    }

    let now = Utc::now();
    let active = chat_channel_sender_context::ActiveModel {
        id: NotSet,
        channel_id: Set(channel_id),
        sender_id: Set(sender_id.to_string()),
        current_folder_id: Set(None),
        current_agent_type: Set(None),
        current_conversation_id: Set(None),
        current_connection_id: Set(None),
        auto_approve: Set(false),
        created_at: Set(now),
        updated_at: Set(now),
    };
    Ok(active.insert(conn).await?)
}

pub async fn update_folder(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    folder_id: Option<i32>,
) -> Result<chat_channel_sender_context::Model, DbError> {
    let model = get_or_create(conn, channel_id, sender_id).await?;
    let mut active = model.into_active_model();
    active.current_folder_id = Set(folder_id);
    active.updated_at = Set(Utc::now());
    Ok(active.update(conn).await?)
}

pub async fn update_agent(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    agent_type: Option<String>,
) -> Result<chat_channel_sender_context::Model, DbError> {
    let model = get_or_create(conn, channel_id, sender_id).await?;
    let mut active = model.into_active_model();
    active.current_agent_type = Set(agent_type);
    active.updated_at = Set(Utc::now());
    Ok(active.update(conn).await?)
}

pub async fn update_session(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    conversation_id: Option<i32>,
    connection_id: Option<String>,
) -> Result<chat_channel_sender_context::Model, DbError> {
    let model = get_or_create(conn, channel_id, sender_id).await?;
    let mut active = model.into_active_model();
    active.current_conversation_id = Set(conversation_id);
    active.current_connection_id = Set(connection_id);
    active.updated_at = Set(Utc::now());
    Ok(active.update(conn).await?)
}

pub async fn clear_session(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
) -> Result<chat_channel_sender_context::Model, DbError> {
    update_session(conn, channel_id, sender_id, None, None).await
}

/// Drop only the live-connection handle, keeping `current_conversation_id`.
///
/// This is the teardown flavour for "the process went away but the
/// conversation didn't": idle-sweep reclaims and crashes kill the agent
/// process, yet the conversation row and its transcript survive on disk, so
/// the sender's next message can respawn the agent and `session/load` the
/// same conversation instead of silently starting a new one. Only an explicit
/// `/new` (or a route that proved unusable, e.g. the SessionStarted conflict
/// teardown) should clear the conversation itself via `clear_session`.
pub async fn clear_connection(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
) -> Result<chat_channel_sender_context::Model, DbError> {
    let model = get_or_create(conn, channel_id, sender_id).await?;
    let mut active = model.into_active_model();
    active.current_connection_id = Set(None);
    active.updated_at = Set(Utc::now());
    Ok(active.update(conn).await?)
}

pub async fn update_auto_approve(
    conn: &DatabaseConnection,
    channel_id: i32,
    sender_id: &str,
    auto_approve: bool,
) -> Result<chat_channel_sender_context::Model, DbError> {
    let model = get_or_create(conn, channel_id, sender_id).await?;
    let mut active = model.into_active_model();
    active.auto_approve = Set(auto_approve);
    active.updated_at = Set(Utc::now());
    Ok(active.update(conn).await?)
}
