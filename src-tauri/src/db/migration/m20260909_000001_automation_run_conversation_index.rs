use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Reverse lookup conversation → the run that produced it, and through it
        // the owning automation. The chat-event webhook needs it on every event
        // to stamp the automation's timezone onto the payload, so it runs on the
        // hot path — and `automation_run` grows one row per fire (a 5-minute
        // schedule is ~100k rows a year), which makes an unindexed scan there a
        // steadily worsening per-event cost.
        //
        // The three indexes this table shipped with (automation+created, status,
        // one-active) all lead with `automation_id`; none can serve a
        // conversation-first probe.
        //
        // Not unique: `conversation_id` is nullable (runs that never produced one,
        // plus SET NULL when a conversation is deleted) and a resumed run reuses
        // the previous run's conversation, so one conversation legitimately maps
        // to many runs. Callers take the newest.
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_automation_run_conversation")
                    .table(AutomationRun::Table)
                    .col(AutomationRun::ConversationId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_automation_run_conversation")
                    .table(AutomationRun::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum AutomationRun {
    Table,
    ConversationId,
}
