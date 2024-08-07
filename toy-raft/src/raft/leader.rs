use tracing::error;

use super::message::*;
use crate::grpc;
use std::sync::Arc;

pub struct Leader {
    _cancel: tokio::sync::watch::Sender<()>,
    // TODO: add ack to wait until the previous leader process is finished for sure.
}

pub struct Args {
    pub id: String,
    pub current_term: u64,
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
                next_index: 1,
                match_index: 0,
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
    current_term: u64,
    cli: PeerClient,

    next_index: u64,
    match_index: u64,

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
        let mut request = tonic::Request::new(grpc::AppendEntriesRequest {
            leader_id: self.leader_id.clone(),
            term: self.current_term,
            entries: vec![], // TODO: zero copy
            leader_commit: 0,
            prev_log_index: self.next_index - 1,
            prev_log_term: 0, // TODO: get the corresponding term from log storage.
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
            return Ok(());
        }

        if res.term > self.current_term {
            tokio::select! {
                _ = self.cancel.changed() => return Err(HeartbeatError::Canceled),
                _ = self.msg_queue.send(Message::BackToFollower { term: res.term }) => return Err(HeartbeatError::BackToFollower),
            };
        }

        if self.next_index == 1 {
            error!(
                id = *self.cli.id,
                "The peer does not implement the protocol correctly"
            );
            return Err(HeartbeatError::ReceiveFailure);
        } else {
            // TODO: optimize by having a peer return the latest possible log index.
            self.next_index -= 1;
        }
        Ok(())
    }
}

enum HeartbeatError {
    ReceiveFailure,
    Canceled,
    BackToFollower,
}
