use std::sync::Arc;

use super::{log::Entry, message::*};

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type ApplyResponse = Arc<dyn std::any::Any + Send + Sync + 'static>;

pub type ApplyResponseReceiver = tokio::sync::oneshot::Receiver<Option<ApplyResponse>>;
pub type ApplyResponseSender = tokio::sync::oneshot::Sender<Option<ApplyResponse>>;

#[async_trait::async_trait]
pub trait StateMachine {
    /// Apply a new entry to the state machine.
    ///
    /// This method will never be called concurrently.
    // TODO: note that the apply shouldn't return StateMachineError if it's an
    // application error. For example, let's say a state machine only accept
    // each key once. If the same key already exists in the state machine, the
    // entry should successfully be applied as "the duplicated key cannot be
    // written twice" application error and persist last_applied_index.
    //
    // StateMachineError can only be returned when it failed to persistently
    // apply the entry due to, for example, storage failure such as no space left.
    //
    // To return an application error, have ApplyResponse contain Result.
    async fn apply(&self, entry: Entry) -> Result<Option<ApplyResponse>, StateMachineError>;
    async fn last_applied_index(&self) -> Result<Index, StateMachineError>;

    // TODO: add a method to perform compaction.

    // TODO: add a method to load a snapshot to be sent to stale followers which
    // doesn't have truncated log entries.
}

#[derive(Debug, thiserror::Error)]
pub enum StateMachineError {
    #[error("state machine internal error: {0}")]
    InternalError(#[source] BoxedError),
}
