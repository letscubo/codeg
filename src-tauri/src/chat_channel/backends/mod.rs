pub mod lark;
pub mod telegram;
pub mod weixin;

use super::error::ChatChannelError;
use super::traits::ChatChannelBackend;
use super::types::*;

/// Factory function to create a backend instance from channel type, config, and token.
/// Eliminates duplicated match blocks across connect, test, and auto-connect paths.
pub fn create_backend(
    channel_id: i32,
    channel_type: ChannelType,
    config: &serde_json::Value,
    token: String,
) -> Result<Box<dyn ChatChannelBackend>, ChatChannelError> {
    match channel_type {
        ChannelType::Telegram => {
            let cfg: TelegramConfig = serde_json::from_value(config.clone()).map_err(|e| {
                ChatChannelError::ConfigurationInvalid(format!("Invalid Telegram config: {e}"))
            })?;
            if cfg.chat_id.is_empty() {
                return Err(ChatChannelError::ConfigurationInvalid(
                    "chat_id is required".into(),
                ));
            }
            let session_defaults = if cfg.direct_chat_enabled {
                let folder_path = cfg
                    .default_folder_path
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        ChatChannelError::ConfigurationInvalid(
                            "default_folder_path is required when direct_chat_enabled is true"
                                .into(),
                        )
                    })?;
                let agent_type = cfg
                    .default_agent_type
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        ChatChannelError::ConfigurationInvalid(
                            "default_agent_type is required when direct_chat_enabled is true"
                                .into(),
                        )
                    })?;
                Some(ChannelSessionDefaults {
                    folder_path,
                    agent_type,
                })
            } else {
                None
            };
            Ok(Box::new(telegram::TelegramBackend::new(
                channel_id,
                token,
                cfg.chat_id,
                cfg.topic_mode,
                session_defaults,
            )))
        }
        ChannelType::Weixin => {
            let cfg: WeixinConfig = serde_json::from_value(config.clone()).map_err(|e| {
                ChatChannelError::ConfigurationInvalid(format!("Invalid Weixin config: {e}"))
            })?;
            if cfg.base_url.is_empty() {
                return Err(ChatChannelError::ConfigurationInvalid(
                    "base_url is required".into(),
                ));
            }
            Ok(Box::new(weixin::WeixinBackend::new(
                channel_id,
                token,
                cfg.base_url,
            )))
        }
        ChannelType::Lark => {
            let cfg: LarkConfig = serde_json::from_value(config.clone()).map_err(|e| {
                ChatChannelError::ConfigurationInvalid(format!("Invalid Lark config: {e}"))
            })?;
            if cfg.app_id.is_empty() || cfg.chat_id.is_empty() {
                return Err(ChatChannelError::ConfigurationInvalid(
                    "app_id and chat_id are required".into(),
                ));
            }
            Ok(Box::new(lark::LarkBackend::new(
                channel_id,
                cfg.app_id,
                token,
                cfg.chat_id,
            )))
        }
    }
}
