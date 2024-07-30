use crate::grpc;

use std::sync::Arc;
use tracing::{info, info_span, warn};

pub struct Actor {
    /// _cancel is to trigger the cancellation of the actor process when the
    /// Actor is dropped.
    _cancel: tokio::sync::watch::Sender<()>,
    message_queue: tokio::sync::mpsc::Sender<Message>,
}

#[derive(Debug, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

impl Actor {
    pub fn new(config: crate::config::Config) -> Actor {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(32);

        let clients: Vec<_> = config
            .peers
            .iter()
            .map(|peer| {
                // Config has already been validated.
                peer.parse::<http::Uri>().unwrap()
            })
            .map(|url| tonic::transport::Channel::builder(url))
            .map(|ch| grpc::raft_client::RaftClient::new(ch.connect_lazy()))
            .collect();
        let clients = Arc::new(clients);

        let actor = ActorProcess {
            state: ActorState {
                current_term: 0,
                voted_for: None,
                state: NodeState::Follower,
                heartbeat_deadline: tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(150), // TODO: randomize
            },
            config,
            clients,
            cancel: cancel_rx,
            tx: msg_tx.clone(),
            rx: msg_rx,
        };
        tokio::spawn(actor.run());

        Actor {
            _cancel: cancel_tx,
            message_queue: msg_tx,
        }
    }

    pub async fn state(&self) -> Result<ActorState, MessageError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_queue.send(Message::GetState(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn grant_vote(
        &self,
        msg: grpc::RequestVoteRequest,
    ) -> Result<(ActorState, bool), MessageError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_queue
            .send(Message::GrantVote {
                request: msg,
                result: tx,
            })
            .await?;
        Ok(rx.await?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("failed to send a message: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("failed to receive a message: {0}")]
    ReceiveError(#[from] tokio::sync::oneshot::error::RecvError),
}

struct ActorProcess {
    state: ActorState,

    config: crate::config::Config,
    clients: Arc<Vec<grpc::raft_client::RaftClient<tonic::transport::Channel>>>,

    cancel: tokio::sync::watch::Receiver<()>,
    tx: tokio::sync::mpsc::Sender<Message>,

    /// Receiver for messages. This channel also acts as a trigger to stop the
    /// actor (i.e. cancellation) that happens when Raft is dropped.
    rx: tokio::sync::mpsc::Receiver<Message>,
}

// TODO: rename ActorState and state because there are too many "state"s
#[derive(Debug, Clone)]
pub struct ActorState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub state: NodeState,
    pub heartbeat_deadline: tokio::time::Instant,
}

enum Message {
    GetState(tokio::sync::oneshot::Sender<ActorState>),
    GrantVote {
        request: grpc::RequestVoteRequest,
        result: tokio::sync::oneshot::Sender<(ActorState, bool)>,
    },
    VoteCompleted(VoteResult),
}

impl ActorProcess {
    /// Main loop of the Raft actor. This actor process is canceled when the
    /// corresponding Actor is dropped.
    pub async fn run(mut self) {
        info!("Starting Raft actor");
        loop {
            let heartbeat_timeout = tokio::time::sleep_until(self.state.heartbeat_deadline);
            tokio::select! {
                _ = heartbeat_timeout => {
                    info!("Heartbeat timeout");
                    self.request_vote();
                },
                _ = self.cancel.changed() => {
                    info!("Raft actor is canceled, shutting down the Actor process");
                    break;
                },
                msg = self.rx.recv() => {
                    match msg {
                        None => {
                            info!(reason = "The message channel has been closed", "Raft actor stopped");
                            break;
                        },
                        Some(msg) => self.handle_message(msg),
                    }
                }
            }
        }
    }

    fn reset_heartbeat_timeout(&mut self) {
        self.state.heartbeat_deadline =
            tokio::time::Instant::now() + tokio::time::Duration::from_millis(150);
        // TODO: randomize
    }

    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::GetState(result) => {
                let _ = result.send(self.state.clone());
            }
            Message::GrantVote { request, result } => {
                let granted = self.grant_vote(request);
                if result.send((self.state.clone(), granted)).is_err() {
                    info!("Returning the result of GrantVote failed");
                }
                self.reset_heartbeat_timeout();
            }
            Message::VoteCompleted(result) => match result {
                VoteResult::Granted => {
                    self.state.state = NodeState::Leader;
                    info!(
                        term = self.state.current_term,
                        "Vote granted, becoming a leader"
                    );

                    // TODO: safely set an infinitely far deadline
                    self.state.heartbeat_deadline =
                        tokio::time::Instant::now() + tokio::time::Duration::from_secs(86400);
                }
                VoteResult::NotGranted(res) => {
                    self.state.current_term = res.term;
                    self.state.state = NodeState::Follower;
                    info!(term = res.term, "Vote not granted, back to follower");
                    self.reset_heartbeat_timeout();
                }
            },
        }
    }

    fn grant_vote(&mut self, request: grpc::RequestVoteRequest) -> bool {
        if request.term < self.state.current_term {
            return false;
        }

        // TODO: check log terms

        self.state.voted_for = Some(request.candidate_id.clone());
        match self.state.state {
            NodeState::Follower => {}
            NodeState::Candidate | NodeState::Leader => {
                self.state.state = NodeState::Follower;
                self.state.voted_for = Some(request.candidate_id.clone());
            }
        };
        true
    }

    fn request_vote(&mut self) {
        self.state.current_term += 1;
        self.state.state = NodeState::Candidate;
        self.reset_heartbeat_timeout();

        let clients = self.clients.clone();
        let state = self.state.clone();
        let id = self.config.id.clone();
        let tx = self.tx.clone();

        let fut = async move {
            // TODO: reduce the amount of logs during the election process
            let span = info_span!("Requesting votes", term = state.current_term);
            let _enter = span.enter();
            info!(
                term = state.current_term,
                peers = clients.len(),
                "Initiating RequestVote RPC"
            );

            let nodes = clients.len() + 1; // including self
            let mut set = tokio::task::JoinSet::new();
            for c in clients.iter() {
                let mut request = tonic::Request::new(grpc::RequestVoteRequest {
                    term: state.current_term,
                    candidate_id: id.clone(),
                    last_log_index: 0,
                    last_log_term: 0,
                });

                let mut c = c.clone();
                request.set_timeout(tokio::time::Duration::from_millis(100)); // TODO: make this configurable
                set.spawn(async move { c.request_vote(request).await });
            }

            let mut granted = 1; // vote for self
            while let Some(res) = set.join_next().await {
                // TODO: select! { set.join_next(), cancel when a leader was found } // Timeout case is covered above
                let res = match res {
                    Ok(res) => res,
                    Err(e) => {
                        warn!(error = e.to_string(), "Failed to receive a vote");
                        continue;
                    }
                };

                match res {
                    Ok(res) => match res.get_ref().vote_granted {
                        true => granted += 1,
                        false => {
                            // Another node has a later term.
                            if res.get_ref().term > state.current_term {
                                let _ = tx
                                    .send(Message::VoteCompleted(VoteResult::NotGranted(
                                        res.into_inner(),
                                    )))
                                    .await;
                                return;
                            }
                        }
                    },
                    Err(e) => {
                        // TODO: print status
                        // TODO: print peer information
                        warn!(error = e.to_string(), "The peer has failed to respond");
                    }
                }
            }

            if granted > nodes / 2 {
                info!("Vote granted");
                let _ = tx.send(Message::VoteCompleted(VoteResult::Granted)).await;
            } else {
                info!("Vote not granted");
            }
        };
        tokio::spawn(fut);
    }
}

enum VoteResult {
    Granted,
    NotGranted(grpc::RequestVoteResponse),
}
