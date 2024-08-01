use tracing::warn;

use super::message::*;
use crate::grpc;
use std::sync::Arc;

pub struct Leader {
    cancel: tokio::sync::watch::Sender<()>,
    // TODO: add ack to wait until the previous leader process is finished for sure.
}

pub struct Args {
    pub id: String,
    pub current_term: u64,
    pub clients: Arc<Vec<grpc::raft_client::RaftClient<tonic::transport::Channel>>>,
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
        for c in self.args.clients.iter() {
            let mut request = tonic::Request::new(grpc::AppendEntriesRequest {
                leader_id: self.args.id.clone(),
                term: self.args.current_term,
                entries: vec![], // TODO: zero copy
                leader_commit: 0,
                prev_log_index: 0,
            });
            request.set_timeout(tokio::time::Duration::from_millis(50)); // TODO: customize

            let mut c = c.clone();
            set.spawn(async move {
                // TODO: return the client information together with the response.
                c.append_entries(request).await
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
        res: Result<
            Result<tonic::Response<grpc::AppendEntriesResponse>, tonic::Status>,
            tokio::task::JoinError,
        >,
    ) {
        let res = match res {
            Ok(res) => res,
            Err(e) => {
                warn!(
                    error = e.to_string(),
                    "Failed to receive a heartbeat response"
                );
                return;
            }
        };

        let res = match res {
            Ok(res) => res,
            Err(e) => {
                // TODO: add peer information
                warn!(
                    error = e.to_string(),
                    "Failed to receive a heartbeat response"
                );
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
