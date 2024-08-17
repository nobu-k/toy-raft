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
    pub commit_index_tx: tokio::sync::watch::Sender<Index>,
    pub commit_index_rx: tokio::sync::watch::Receiver<Index>,
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
                commit_index: args.commit_index_rx.clone(),
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
            commit_index: args.commit_index_tx.clone(),
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
    commit_index: tokio::sync::watch::Receiver<Index>,

    storage: crate::config::SharedStorage,
    // TODO: use watch to notify that there's new log entry. The value should be the latest index.
    cancel: tokio::sync::watch::Receiver<()>,
    msg_queue: tokio::sync::mpsc::Sender<Message>,
}

impl Follower {
    async fn run(mut self) {
        loop {
            if let Err(e) = self.wait_for_connection().await {
                error!(
                    error = e.to_string(),
                    id = *self.cli.id,
                    "Failed to establish a connection to the follower, canceling the leader process"
                );

                if let AppendEntriesError::BackToFollower(term) = e {
                    // Going back to the follower as soon as possible. This
                    // operation is idempotent, so multiple followers can send
                    // the same message.
                    tokio::select! {
                        _ = self.cancel.changed() => {},

                        // The leader will eventually go back to the follower
                        // even if sending this message fails because the target
                        // follower will have an election timeout.
                        _ = self.msg_queue.send(Message::BackToFollower { term: term }) => {},
                    };
                }
                return;
            }

            if let Err(e) = self.find_latest_match_index().await {
                error!(
                    error = e.to_string(),
                    id = *self.cli.id,
                    "Failed to find the latest match index"
                );

                // Going back to the beginning of the loop for error handling on
                // the next heartbeat.
                continue;
            }

            info!(
                follower_id = *self.cli.id,
                match_index = self.match_index.borrow().get(),
                "Found the latest match index of the follower",
            );

            // TODO: initiate install snapshot if needed

            // This loop assumes that followers reset the heartbeat timeout
            // every time they receive a message right before they return the
            // response.
            loop {
                if let Err(e) = self.send_all_log_entries().await {
                    error!(
                        error = e.to_string(),
                        id = *self.cli.id,
                        "Failed to send log entries to the follower, trying to recover"
                    );
                    break;
                }

                let heartbeat = tokio::time::sleep(tokio::time::Duration::from_millis(50));
                tokio::select! {
                    _ = heartbeat => match self.send_heartbeat().await {
                        Ok(()) => {},
                        Err(_) => break, // Let wait_for_connection take care of the error.
                    },
                    _ = self.storage_updated.changed() => {}, // Continue the loop.
                    _ = self.cancel.changed() => {
                        return;
                    }
                }
            }
        }
    }

    async fn wait_for_connection(&mut self) -> Result<(), AppendEntriesError> {
        loop {
            match self.send_heartbeat().await {
                Ok(()) => return Ok(()),
                Err(AppendEntriesError::ReceiveFailure) => {}

                // The connection itself is fine. Call find_latest_match_index
                // in the next step to align the log index with the follower.
                Err(AppendEntriesError::LastIndexMismatch) => return Ok(()),
                Err(e) => return Err(e),
            }

            // DO NOT perform exponential backoff that can become longer than
            // the election timeout. Otherwise, a node that just came back from
            // a failure will immediately take over the leader if the log is not
            // update.
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(50)) => {},
                _ = self.cancel.changed() => return Err(AppendEntriesError::Canceled),
            }
        }
    }

    async fn find_latest_match_index(&mut self) -> Result<(), AppendEntriesError> {
        // TODO: optimize the match index discovery.
        loop {
            // TODO: need some sleep?
            match self.send_heartbeat().await {
                Ok(()) => return Ok(()),
                Err(AppendEntriesError::LastIndexMismatch) => {} // Try next heartbeat.
                Err(e) => return Err(e),
            }
        }
    }

    async fn send_heartbeat(&mut self) -> Result<(), AppendEntriesError> {
        let prev_log_term = match self.storage.get_entry(self.next_index.prev()).await {
            Ok(Some(entry)) => entry.term(),
            Ok(None) => Term::new(0),
            // TODO: handle the case that the corresponding entry is compacted
            // and missing. In such a case, the leader should install the
            // snapshot to the follower first.
            Err(e) => {
                error!(
                    error = e.to_string(),
                    "Failed to get the previous log term from the storage"
                );
                return Err(AppendEntriesError::BackToFollower(self.current_term));
            }
        };
        let mut request = tonic::Request::new(grpc::AppendEntriesRequest {
            leader_id: self.leader_id.clone(),
            term: self.current_term.get(),
            entries: vec![], // TODO: zero copy
            leader_commit: self.commit_index.borrow().get(),
            prev_log_index: self.next_index.prev().get(),
            prev_log_term: prev_log_term.get(),
        });
        request.set_timeout(tokio::time::Duration::from_millis(50)); // TODO: customize

        tokio::select! {
            _ = self.cancel.changed() => return Err(AppendEntriesError::Canceled),
            res = self.cli.client.append_entries(request) => match res {
                Ok(res) => self.handle_heartbeat_response(res).await,
                Err(e) => {
                    super::metrics::inc_peer_receive_failure(&self.cli.id, e.code());
                    Err(AppendEntriesError::ReceiveFailure)
                }
            }
        }
    }

    async fn handle_heartbeat_response(
        &mut self,
        res: tonic::Response<grpc::AppendEntriesResponse>,
    ) -> Result<(), AppendEntriesError> {
        let res = res.into_inner();
        if res.success {
            let matched = self.next_index.prev();
            if *self.match_index.borrow() != matched {
                if let Err(_) = self.match_index.send(matched) {
                    // The leader is already gone.
                    return Err(AppendEntriesError::Canceled);
                }
            }
            return Ok(());
        }

        if res.term > self.current_term.get() {
            return Err(AppendEntriesError::BackToFollower(Term::new(res.term)));
        }

        if self.next_index.get() == 1 {
            error!(
                error = "the first index does not match",
                id = *self.cli.id,
                "The peer does not implement the protocol correctly"
            );
            return Err(AppendEntriesError::InvalidProtocol);
        }

        // TODO: optimize by having a peer return the latest possible log index.
        self.next_index.dec();
        Err(AppendEntriesError::LastIndexMismatch)
    }

    async fn send_all_log_entries(&mut self) -> Result<(), AppendEntriesError> {
        let last_index = match self.storage.get_last_entry().await {
            Ok(Some(last)) => last.index(),
            Ok(None) => {
                return Ok(());
            }
            Err(e) => {
                error!(
                    error = e.to_string(),
                    "Failed to get the last entry from the storage"
                );
                return Err(AppendEntriesError::StorageError(
                    "failed to get the last entry from the storage",
                    e,
                ));
            }
        };

        // send_log_entry_batch might not be able to send all the entries
        // missing on the follower at once. So we need to call it multiple
        // times.
        //
        // The storage might also get new entries while sending the entries, but
        // this loop doesn't have to take care of it because it'll be processed
        // in the next iteration since the storage_updated watch will be
        // triggered.
        while self.next_index < last_index {
            match self.send_log_entry_batch().await {
                Ok(()) => {}
                Err(AppendEntriesError::NoEntryToSend) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn send_log_entry_batch(&mut self) -> Result<(), AppendEntriesError> {
        // TODO: this code has an issue that a follower times out if it takes
        // too long to read entries from the log. We might need to send
        // heartbeats in parallel.

        // TODO: might be good to send a heartbeat to reset the timeout.

        // TODO: limit the time and size of entries.
        let entries = match self.storage.get_entries_after(self.next_index.prev()).await {
            Ok(entries) => entries,
            Err(e) => {
                error!(
                    error = e.to_string(),
                    index = self.next_index.prev().get(),
                    "Failed to get entries from the storage"
                );
                // In this failure case, this actor goes back to the outer loop
                // of the run method. If it gets the same error again, it goes
                // back to the follower. send_all_log_entries will be retried
                // after the initial condition check.
                return Err(AppendEntriesError::StorageError("failed to get entries", e));
            }
        };
        if entries.is_empty() {
            return Err(AppendEntriesError::NoEntryToSend);
        }

        let mut request = tonic::Request::new(grpc::AppendEntriesRequest {
            leader_id: self.leader_id.clone(),
            term: self.current_term.get(),
            entries: entries
                .iter()
                .map(|e| grpc::LogEntry {
                    index: e.index().get(),
                    term: e.term().get(),
                    data: (*e.data()).clone(), // TODO: zero-copy
                })
                .collect(),
            leader_commit: self.commit_index.borrow().get(),
            prev_log_index: self.next_index.prev().get(),
            prev_log_term: entries.first().map_or(0, |e| e.term().get()),
        });

        // Timeout can be long because the follower's election timeout will not
        // happen while it's processing the append_entries request.
        request.set_timeout(tokio::time::Duration::from_secs(3));

        let res = tokio::select! {
            _ = self.cancel.changed() => return Err(AppendEntriesError::Canceled),
            res = self.cli.client.append_entries(request) => match res {
                Ok(res) => res.into_inner(),
                Err(e) => {
                    super::metrics::inc_peer_receive_failure(&self.cli.id, e.code());
                    // The follower might have been disconnected, so going back
                    // to the outer loop.

                    // TODO: take care of other failures.
                    return Err(AppendEntriesError::ReceiveFailure);
                },
            }
        };

        if res.success {
            let matched = entries.last().unwrap().index();
            self.next_index = matched.next();
            if let Err(_) = self.match_index.send(matched) {
                // The leader is already gone.
                return Err(AppendEntriesError::Canceled);
            }
            return Ok(());
        }

        if res.term > self.current_term.get() {
            return Err(AppendEntriesError::BackToFollower(Term::new(res.term)));
        }

        error!(
            error = "previously matched index does not match now",
            id = *self.cli.id,
            "The peer does not implement the protocol correctly"
        );
        Err(AppendEntriesError::InvalidProtocol)
    }
}

#[derive(Debug, thiserror::Error)]
enum AppendEntriesError {
    #[error("failed to receive a heartbeat response")]
    ReceiveFailure,

    #[error("the follower implements an invalid Raft protocol")]
    InvalidProtocol,

    #[error("{0}: {1}")]
    StorageError(&'static str, #[source] log::StorageError),

    #[error("operation canceled")]
    Canceled,

    #[error("need to back to follower")]
    BackToFollower(Term),

    #[error("no entry to send")]
    NoEntryToSend,

    #[error("last index mismatch")]
    LastIndexMismatch,
}
