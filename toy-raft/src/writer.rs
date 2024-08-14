use std::{cmp, collections::HashMap, sync::Arc};

use tracing::{error, info, trace};

use super::{
    message::*,
    state_machine::{ApplyResponse, StateMachine, StateMachineError},
};
use crate::config::SharedStorage;

pub type ResponseChannelMap =
    HashMap<Index, (Term, tokio::sync::oneshot::Sender<Option<ApplyResponse>>)>;

pub struct Writer {
    response_channels: Arc<tokio::sync::Mutex<ResponseChannelMap>>,
}

pub struct Args {
    pub cancel: tokio::sync::watch::Receiver<()>,
    pub storage: SharedStorage,
    pub state_machine: Arc<dyn StateMachine + Sync + Send>,
    pub commit_index: tokio::sync::watch::Receiver<Index>,
}

impl Writer {
    pub async fn start(args: Args) -> Result<Self, StateMachineError> {
        // This is the only actor that writes to the state machine, so it's safe
        // to assume that last_applied_index obtained here will not be changed
        // by any other actors.
        let mut next_index = match args.state_machine.last_applied_index().await {
            Ok(index) => index,
            Err(err) => {
                error!(
                    error = err.to_string(),
                    "Failed to get last applied index from the state machine"
                );
                return Err(err);
            }
        };
        next_index.inc();

        let response_channels = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        let process = WriterProcess {
            commit_index: args.commit_index,
            storage: args.storage,
            cancel: args.cancel,
            state_machine: args.state_machine,
            next_index,
            response_channels: response_channels.clone(),
        };
        tokio::spawn(process.run());

        Ok(Writer { response_channels })
    }

    pub async fn suspend_apply(&self) -> ResponseChannelRegistry {
        ResponseChannelRegistry {
            channels: self.response_channels.lock().await,
        }
    }
}

pub struct ResponseChannelRegistry<'a> {
    channels: tokio::sync::MutexGuard<'a, ResponseChannelMap>,
}

impl<'a> ResponseChannelRegistry<'a> {
    pub fn register_response_channel(
        &mut self,
        index: Index,
        term: Term,
    ) -> tokio::sync::oneshot::Receiver<Option<ApplyResponse>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.channels.insert(index, (term, tx));
        rx
    }
}

struct WriterProcess {
    commit_index: tokio::sync::watch::Receiver<Index>,
    storage: SharedStorage,
    cancel: tokio::sync::watch::Receiver<()>,
    state_machine: Arc<dyn StateMachine + Sync + Send>,
    next_index: Index,
    response_channels: Arc<tokio::sync::Mutex<ResponseChannelMap>>,
}

impl WriterProcess {
    async fn run(mut self) {
        loop {
            let mut backoff = tokio::time::Duration::from_millis(100);
            let commit_index = *self.commit_index.borrow();
            if commit_index.get() > 0 {
                while self.next_index <= commit_index {
                    if self.apply(self.next_index).await.is_ok() {
                        self.next_index.inc();
                        break;
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = cmp::min(backoff * 2, tokio::time::Duration::from_secs(60));
                }
            }

            // TODO: garbage collect response channels
            tokio::select! {
                _ = self.cancel.changed() => break,
                _ = self.commit_index.changed() => (),
            }
        }
    }

    async fn apply(&self, index: Index) -> Result<(), ()> {
        let entry = match self.storage.get_entry(index).await {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                error!(
                    index = index.get(),
                    "The entry does not exist in the storage"
                );
                return Err(());
            }
            Err(err) => {
                error!(
                    error = err.to_string(),
                    index = index.get(),
                    "Failed to get the entry from the storage"
                );
                return Err(());
            }
        };

        let entry_term = entry.term();
        let key = entry.index();
        match self.state_machine.apply(entry).await {
            Ok(response) => {
                let ch = {
                    let mut channels = self.response_channels.lock().await;
                    channels.remove(&key)
                };
                if let Some((term, tx)) = ch {
                    if term < entry_term {
                        // The entry has been overwritten by a new leader. This
                        // shouldn't happen a lot because append_entry is only
                        // called by the leader process, and thus it should have
                        // mostly update to date log entries.
                        trace!(
                            index = index.get(),
                            "The response is discarded because the entry has been overwritten by a new leader"
                        );
                        return Ok(());
                    }

                    if let Err(_) = tx.send(response) {
                        trace!(
                            index = index.get(),
                            "Failed to send the response to the client"
                        );
                        return Err(());
                    }
                }
                Ok(())
            }
            Err(err) => {
                // TODO: finer-grained error handling
                error!(
                    error = err.to_string(),
                    index = index.get(),
                    "Failed to apply the entry to the state machine"
                );
                Err(())
            }
        }
    }
}
