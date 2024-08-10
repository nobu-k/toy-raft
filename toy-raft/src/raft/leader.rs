use tracing::error;

use super::{log, message::*};
use crate::grpc;
use std::sync::Arc;

pub struct Leader {
    _cancel: tokio::sync::watch::Sender<()>,
    // TODO: add ack to wait until the previous leader process is finished for sure.
}

pub struct Args {
    pub id: String,
    pub current_term: Term,
    pub peers: Arc<Vec<PeerClient>>,
    pub msg_queue: tokio::sync::mpsc::Sender<Message>,
    pub storage: crate::config::SharedStorage,
}

impl Leader {
    pub fn start(args: Args) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());

        for peer in args.peers.iter() {
            let follower = Follower {
                leader_id: args.id.clone(),
                current_term: args.current_term,
                cli: peer.clone(),
                next_index: Index::new(1),
                match_index: Index::new(0),
                storage: args.storage.clone(),
                cancel: cancel_rx.clone(),
                msg_queue: args.msg_queue.clone(),
            };
            tokio::spawn(follower.run());
        }

        Leader { _cancel: cancel_tx }
    }
}

struct Follower {
    leader_id: String,
    current_term: Term,
    cli: PeerClient,

    next_index: Index,
    match_index: Index,

    storage: crate::config::SharedStorage,
    // TODO: use watch to notify that there's new log entry. The value should be the latest index.
    cancel: tokio::sync::watch::Receiver<()>,
    msg_queue: tokio::sync::mpsc::Sender<Message>,
}

impl Follower {
    async fn run(mut self) {
        loop {
            // TODO: wait for the connection

            // loop {
            // TODO: locate the latest match index
            // TODO: break the loop if matched
            // }

            // TODO: initiate install snapshot if needed

            // This loop assumes that followers reset the heartbeat timeout
            // every time they receive a message right before they return the
            // response.
            loop {
                let heartbeat = tokio::time::sleep(tokio::time::Duration::from_millis(50));
                tokio::select! {
                    _ = heartbeat => match self.send_heartbeat().await {
                        Ok(()) => {},
                        Err(HeartbeatError::ReceiveFailure) => break, // Back to the outer loop.
                        Err(_) => return,
                    },
                    _ = self.cancel.changed() => {
                        return;
                    }
                }
            }
        }
    }

    async fn send_heartbeat(&mut self) -> Result<(), HeartbeatError> {
        let prev_log_term = match self.storage.get_entry(self.next_index.prev()).await {
            Ok(Some(entry)) => entry.term(),
            Ok(None) => Term::new(0),
            Err(e) => {
                error!(
                    error = e.to_string(),
                    "Failed to get the previous log term from the storage"
                );
                let _ = self.back_to_follower(self.current_term).await;
                return Err(HeartbeatError::StorageError(
                    "failed to get the previous log term from the storage",
                    e,
                ));
            }
        };
        let mut request = tonic::Request::new(grpc::AppendEntriesRequest {
            leader_id: self.leader_id.clone(),
            term: self.current_term.get(),
            entries: vec![], // TODO: zero copy
            leader_commit: 0,
            prev_log_index: self.next_index.prev().get(),
            prev_log_term: prev_log_term.get(),
        });
        request.set_timeout(tokio::time::Duration::from_millis(50)); // TODO: customize

        tokio::select! {
            _ = self.cancel.changed() => return Err(HeartbeatError::Canceled),
            res = self.cli.client.append_entries(request) => match res {
                Ok(res) => self.handle_heartbeat_response(res).await,
                Err(e) => {
                    super::metrics::inc_peer_receive_failure(&self.cli.id, e.code());
                    Err(HeartbeatError::ReceiveFailure)
                }
            }
        }
    }

    async fn handle_heartbeat_response(
        &mut self,
        res: tonic::Response<grpc::AppendEntriesResponse>,
    ) -> Result<(), HeartbeatError> {
        let res = res.into_inner();
        if res.success {
            self.match_index = self.next_index.prev();
            return Ok(());
        }

        if res.term > self.current_term.get() {
            return self.back_to_follower(Term::new(res.term)).await;
        }

        if self.next_index.get() == 1 {
            error!(
                id = *self.cli.id,
                "The peer does not implement the protocol correctly"
            );
            return Err(HeartbeatError::ReceiveFailure);
        } else {
            // TODO: optimize by having a peer return the latest possible log index.
            self.next_index.dec();
        }
        Ok(())
    }

    async fn back_to_follower(&mut self, term: Term) -> Result<(), HeartbeatError> {
        tokio::select! {
            _ = self.cancel.changed() => return Err(HeartbeatError::Canceled),
            _ = self.msg_queue.send(Message::BackToFollower { term: term }) => return Err(HeartbeatError::BackToFollower),
        };
    }
}

#[derive(Debug, thiserror::Error)]
enum HeartbeatError {
    #[error("failed to receive a heartbeat response")]
    ReceiveFailure,

    #[error("{0}: {1}")]
    StorageError(&'static str, #[source] log::StorageError),

    #[error("operation canceled")]
    Canceled,

    #[error("need to back to follower")]
    BackToFollower,
}
