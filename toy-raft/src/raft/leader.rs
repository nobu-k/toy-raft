use tracing::warn;

use super::message::*;
use crate::{grpc, Peer};
use std::sync::Arc;

pub struct Leader {
    cancel: tokio::sync::watch::Sender<()>,
    // TODO: add ack to wait until the previous leader process is finished for sure.
}

pub struct Args {
    pub id: String,
    pub current_term: u64,
    pub peers: Arc<Vec<PeerClient>>,
    pub msg_queue: tokio::sync::mpsc::Sender<Message>,
}

impl Leader {
    pub fn start(args: Args) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());

        let process = LeaderProcess {
            args,
            cancel: cancel_rx,
        };
        tokio::spawn(process.run());

        Leader { cancel: cancel_tx }
    }
}

struct LeaderProcess {
    args: Args,
    cancel: tokio::sync::watch::Receiver<()>,
}

impl LeaderProcess {
    async fn run(mut self) {
        loop {
            let heartbeat = tokio::time::sleep(tokio::time::Duration::from_millis(50));
            tokio::select! {
                _ = heartbeat => self.send_heartbeat().await,
                _ = self.cancel.changed() => {
                    break;
                }
            }
        }
    }

    async fn send_heartbeat(&mut self) {
        let mut set = tokio::task::JoinSet::new();
        for p in self.args.peers.iter() {
            let mut request = tonic::Request::new(grpc::AppendEntriesRequest {
                leader_id: self.args.id.clone(),
                term: self.args.current_term,
                entries: vec![], // TODO: zero copy
                leader_commit: 0,
                prev_log_index: 0,
            });
            request.set_timeout(tokio::time::Duration::from_millis(50)); // TODO: customize

            let mut p = p.clone();
            set.spawn(async move {
                PeerJoinResponse {
                    id: p.id,
                    result: p.client.append_entries(request).await,
                }
            });
        }

        loop {
            tokio::select! {
                _ = self.cancel.changed() => break,
                next = set.join_next() => match next {
                    Some(res) => self.handle_heartbeat_response(res).await,
                    None => break,
                }
            }
        }
    }

    async fn handle_heartbeat_response(
        &mut self,
        res: PeerJoinResult<grpc::AppendEntriesResponse>,
    ) {
        let join_result = match res {
            Ok(res) => res,
            Err(e) => {
                warn!(error = e.to_string(), "Failed to join heartbeat responses");
                return;
            }
        };

        let res = match join_result.result {
            Ok(res) => res,
            Err(e) => {
                super::metrics::inc_peer_receive_failure(&join_result.id, e.code());
                return;
            }
        };

        let res = res.into_inner();
        if res.success {
            return;
        }

        tokio::select! {
            _ = self.cancel.changed() => {},
            _ = self.args.msg_queue.send(Message::BackToFollower { term: res.term }) => {},
        };
    }
}
