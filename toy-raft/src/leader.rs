use tracing::{error, info};

use super::{
    log::{self, StorageError},
    message::*,
    state_machine::ApplyResponseReceiver,
};
use crate::grpc;
use std::sync::Arc;

pub struct Leader {
    current_term: Term,
    _cancel: tokio::sync::watch::Sender<()>,

    storage: crate::config::SharedStorage,
    storage_updated: tokio::sync::watch::Sender<()>,
    // TODO: add ack to wait until the previous leader process is finished for sure.
}

pub struct Args {
    pub id: String,
    pub current_term: Term,
    pub peers: Arc<Vec<PeerClient>>,
    pub msg_queue: tokio::sync::mpsc::Sender<Message>,
    pub commit_index: tokio::sync::watch::Sender<Index>,
    pub storage: crate::config::SharedStorage,
}

impl Leader {
    pub fn start(args: Args) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let (storage_tx, storage_rx) = tokio::sync::watch::channel(());
        let mut watchers = Vec::with_capacity(args.peers.len());

        for peer in args.peers.iter() {
            let (tx, rx) = tokio::sync::watch::channel(Index::new(0));
            watchers.push(rx);

            let follower = Follower {
                leader_id: args.id.clone(),
                current_term: args.current_term,
                storage_updated: storage_rx.clone(),
                cli: peer.clone(),
                next_index: Index::new(1),
                match_index: tx,
                storage: args.storage.clone(),
                cancel: cancel_rx.clone(),
                msg_queue: args.msg_queue.clone(),
            };
            tokio::spawn(follower.run());
        }

        let syncer = CommitIndexSyncer {
            match_indexes_watcher: watchers,
            match_indexes: vec![Index::new(0); args.peers.len()],
            commit_index: args.commit_index,
            cancel: cancel_rx,
        };
        tokio::spawn(syncer.run());
        Leader {
            current_term: args.current_term,
            _cancel: cancel_tx,

            storage: args.storage.clone(),
            storage_updated: storage_tx,
        }
    }

    pub async fn append_entry(
        &self,
        entry: Arc<Vec<u8>>,
        require_response: bool,
    ) -> Result<Option<ApplyResponseReceiver>, StorageError> {
        let (_, rx) = self
            .storage
            .append_entry(self.current_term, entry, require_response)
            .await?;
        let _ = self.storage_updated.send(());
        Ok(rx)
    }
}

struct CommitIndexSyncer {
    match_indexes_watcher: Vec<tokio::sync::watch::Receiver<Index>>,
    match_indexes: Vec<Index>,
    commit_index: tokio::sync::watch::Sender<Index>,
    cancel: tokio::sync::watch::Receiver<()>,
}

impl CommitIndexSyncer {
    async fn run(mut self) {
        let build_future = |i: usize, mut rx: tokio::sync::watch::Receiver<Index>| {
            Box::pin(async move {
                let res = rx.changed().await;
                match res {
                    Ok(_) => Ok(i),
                    Err(e) => Err(e),
                }
            })
        };

        let mut waiters: Vec<_> = self
            .match_indexes_watcher
            .iter()
            .enumerate()
            .map(|(i, rx)| build_future(i, rx.clone()))
            .collect();

        let mut sorted: Vec<usize> = (0..self.match_indexes.len()).collect();
        loop {
            let res = tokio::select! {
                res = futures::future::select_all(waiters.into_iter()) => res,
                _ = self.cancel.changed() => return,
            };

            let (i, remaining) = match res {
                (Ok(i), _, remaining) => (i, remaining),
                (Err(e), _, _) => {
                    info!(
                        error = e.to_string(),
                        "Failed to receive a match index, canceling"
                    );
                    return;
                }
            };
            // Push the selected future back to the waiters.
            waiters = remaining;
            waiters.push(build_future(i, self.match_indexes_watcher[i].clone()));

            // Sort the current match indexes and find the median, which means
            // the majority of the followers have at least that index. The
            // leader's match_index is always the maximum.
            self.match_indexes[i] = *self.match_indexes_watcher[i].borrow();
            sorted.sort_unstable_by_key(|&i| self.match_indexes[i]);
            let new_commit_index = self.match_indexes[sorted[sorted.len() / 2]];
            if new_commit_index > *self.commit_index.borrow() {
                if let Err(_) = self.commit_index.send(new_commit_index) {
                    // No receiver exists. This situation is the same as cancel.
                    return;
                }
            }
        }
    }
}

struct Follower {
    leader_id: String,
    current_term: Term,
    cli: PeerClient,
    storage_updated: tokio::sync::watch::Receiver<()>,

    next_index: Index,
    match_index: tokio::sync::watch::Sender<Index>,

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
                    _ = self.storage_updated.changed() => {
                        self.send_log_entries().await;
                    }
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
            let matched = self.next_index.prev();
            if *self.match_index.borrow() != matched {
                if let Err(_) = self.match_index.send(matched) {
                    // The leader is already gone.
                    return Err(HeartbeatError::Canceled);
                }
            }
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

    async fn send_log_entries(&mut self) {
        // TODO: when reading from the storage takes time, it can happen that
        // the follower times out and becomes a candidate. However, having a
        // separate actor to send heartbeats can result in log truncation.
        // Introducing a lock to call append_entries would be a solution?

        // TODO: implement
        // TODO: load entries after next_index.

        // TODO: think of what to do if append_entries failed. Retry with backoff will be necessary with heartbeat.
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
