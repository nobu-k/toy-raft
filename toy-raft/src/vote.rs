use super::message::*;
use crate::{grpc, metrics};
use std::sync::Arc;
use tracing::{info, info_span, warn};

pub struct Vote {
    vote_term: Term,
    cancel: tokio::sync::watch::Sender<()>,
    ack: tokio::sync::oneshot::Receiver<()>,
}

pub struct Args {
    pub id: String,
    pub current_term: Term,
    pub last_log_index: Index,
    pub last_log_term: Term,
    pub msg_queue: tokio::sync::mpsc::Sender<Message>,
    pub peers: Arc<Vec<PeerClient>>,
}

impl Vote {
    /// Initiates a new voting process. In order to cancel the process, drop the
    /// returned value or call `cancel` method.
    pub fn start(args: Args) -> Self {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

        let vote_term = args.current_term;
        let process = VoteProcess {
            args,
            cancel: cancel_rx,
            _ack: ack_tx,
        };
        tokio::spawn(process.run());

        Vote {
            vote_term: vote_term,
            cancel: cancel_tx,
            ack: ack_rx,
        }
    }

    pub fn vote_term(&self) -> Term {
        self.vote_term
    }

    pub fn is_active(&mut self) -> bool {
        match self.ack.try_recv() {
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => true,
            _ => false,
        }
    }

    pub async fn cancel(self) {
        std::mem::drop(self.cancel);
        let _ = self.ack.await;
    }
}

struct VoteProcess {
    args: Args,
    cancel: tokio::sync::watch::Receiver<()>,

    // _ask responds to the caller when the process is cancelled. No explicit
    // send is required as the receiver reacts to an error when the process is
    // dropped.
    _ack: tokio::sync::oneshot::Sender<()>,
}

impl VoteProcess {
    async fn run(mut self) {
        // TODO: this span doesn't seem to be dropped properly
        /*
        let span = info_span!("Requesting votes", term = args.current_term);
        let _enter = span.enter();
        */
        info!(
            term = self.args.current_term.get(),
            peers = self.args.peers.len(),
            "Initiating RequestVote RPC",
        );

        let nodes = self.args.peers.len() + 1; // including self
        let mut set = self.send_requests();

        let mut granted = 1; // vote for self
        loop {
            tokio::select! {
                _ = self.cancel.changed() => {
                    info!("Vote process has been cancelled");
                    break;
                }
                next = set.join_next() => {
                    match next {
                        Some(res) => {
                            if self.vote_granted(res).await {
                                granted += 1;
                            }
                            // Don't break here to make sure all responses are processed.
                        },
                        None => break,
                    }
                }
            }
        }

        if granted > nodes / 2 {
            info!(
                term = self.args.current_term.get(),
                peers = self.args.peers.len(),
                vote_granted = granted,
                "Vote granted"
            );
            tokio::select! {
                _ = self
                .args
                .msg_queue
                .send(Message::VoteCompleted(VoteResult::Granted {
                    vote_term: self.args.current_term,
                })) => {},
                _ = self.cancel.changed() => {},
            }
        } else {
            info!(
                term = self.args.current_term.get(),
                peers = self.args.peers.len(),
                vote_granted = granted,
                "Vote not granted"
            );
            tokio::select! {
                _ = self
                .args
                .msg_queue
                .send(Message::VoteCompleted(VoteResult::NotGranted {
                    vote_term: self.args.current_term,
                })) => {},
                _ = self.cancel.changed() => {},
            }
        }
    }

    async fn vote_granted(&mut self, res: PeerJoinResult<grpc::RequestVoteResponse>) -> bool {
        let join_result = match res {
            Ok(res) => res,
            Err(e) => {
                warn!(error = e.to_string(), "Failed to join vote responses");
                return false;
            }
        };

        match join_result.result {
            Ok(res) => {
                let granted = res.get_ref().vote_granted;
                if !granted {
                    if res.get_ref().term > self.args.current_term.get() {
                        tokio::select! {
                            _ = self
                            .args
                            .msg_queue
                            .send(Message::VoteCompleted(VoteResult::NewerTermFound {
                                vote_term: self.args.current_term,
                                response: res.into_inner(),
                            })) => {},
                            _ = self.cancel.changed() => {},
                        };
                    }
                }
                granted
            }
            Err(e) => {
                metrics::inc_peer_receive_failure(&join_result.id, e.code());
                false
            }
        }
    }

    fn send_requests(&self) -> PeerJoinSet<grpc::RequestVoteResponse> {
        let args = &self.args;

        let mut set = tokio::task::JoinSet::new();
        for p in args.peers.iter() {
            let mut request = tonic::Request::new(grpc::RequestVoteRequest {
                term: args.current_term.get(),
                candidate_id: args.id.clone(),
                last_log_index: args.last_log_index.get(),
                last_log_term: args.last_log_term.get(),
            });

            let mut p = p.clone();
            request.set_timeout(tokio::time::Duration::from_millis(100)); // TODO: make this configurable
            set.spawn(async move {
                PeerJoinResponse {
                    id: p.id,
                    result: p.client.request_vote(request).await,
                }
            });
        }

        set
    }
}
