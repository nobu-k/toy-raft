use crate::grpc;

use super::{leader, log, message::*, vote};
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use tracing::{error, info, trace};

pub struct Actor {
    /// _cancel is to trigger the cancellation of the actor process when the
    /// Actor is dropped.
    _cancel: tokio::sync::watch::Sender<()>,
    message_queue: tokio::sync::mpsc::Sender<Message>,
}

impl Actor {
    pub fn new(config: crate::config::Config) -> Actor {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(32);

        let peers: Vec<_> = config
            .peers
            .iter()
            .map(|peer| {
                // Config has already been validated.
                let url = peer.addr.parse::<http::Uri>().unwrap();
                PeerClient {
                    id: Arc::new(peer.id.clone()),
                    client: grpc::raft_client::RaftClient::new(
                        tonic::transport::Channel::builder(url).connect_lazy(),
                    ),
                }
            })
            .collect();
        let peers = Arc::new(peers);

        let actor = ActorProcess {
            state: ActorState {
                current_term: 0,
                voted_for: None,
                state: NodeState::Follower,
                heartbeat_deadline: tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(150), // TODO: randomize
            },
            config,
            peers,
            rng: rand::rngs::StdRng::from_entropy(),
            leader: None,
            vote: None,
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

    pub async fn append_entries(
        &self,
        msg: grpc::AppendEntriesRequest,
    ) -> Result<Result<grpc::AppendEntriesResponse, tonic::Status>, MessageError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_queue
            .send(Message::AppendEntries {
                request: msg,
                result: tx,
            })
            .await?;
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

struct ActorProcess {
    state: ActorState,

    config: crate::config::Config,

    // TODO: rename this to peers. A peer should be a struct that contains the ID and gRPC client.
    peers: Arc<Vec<PeerClient>>,

    /// Used to variate the heartbeat timeout.
    rng: rand::rngs::StdRng,

    leader: Option<leader::Leader>,
    vote: Option<vote::Vote>,

    cancel: tokio::sync::watch::Receiver<()>,
    tx: tokio::sync::mpsc::Sender<Message>,

    /// Receiver for messages. This channel also acts as a trigger to stop the
    /// actor (i.e. cancellation) that happens when Raft is dropped.
    rx: tokio::sync::mpsc::Receiver<Message>,
}

macro_rules! reset_timeout {
    ($self:ident, $guard:ident, $instant:expr, $base_msec:expr) => {
        let jitter: u64 = $self.rng.gen_range(0..150);
        $guard = scopeguard::guard($instant, move |i| {
            *i = tokio::time::Instant::now()
                + tokio::time::Duration::from_millis($base_msec + jitter);
        });
    };
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
                    // TODO: suppress this log right after the startup because
                    // other nodes haven't started up yet, either, and a lot of
                    // this log are written.
                    info!("Heartbeat timeout");
                    self.request_vote().await;
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
                        Some(msg) => self.handle_message(msg).await,
                    }
                }
            }
        }
    }

    fn reset_heartbeat_timeout(&mut self) {
        let _guard;
        reset_timeout!(self, _guard, &mut self.state.heartbeat_deadline, 150);
    }

    async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::GetState(result) => {
                let _ = result.send(self.state.clone());
            }
            Message::AppendEntries { request, result } => {
                let response = self.append_entries(request).await;
                if result.send(response).is_err() {
                    info!("Returning the result of AppendEntries failed");
                }
            }
            Message::GrantVote { request, result } => {
                let granted = self.grant_vote(request).await;
                if result.send((self.state.clone(), granted)).is_err() {
                    info!("Returning the result of GrantVote failed");
                }
                self.reset_heartbeat_timeout();
            }
            Message::VoteCompleted(res) => self.vote_completed(res),
            Message::BackToFollower { term } => self.back_to_follower(term),
        }
    }

    async fn append_entries(
        &mut self,
        request: grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse, tonic::Status> {
        if request.term < self.state.current_term {
            // The term is too old, so don't reset the heartbeat timeout here.
            return Ok(grpc::AppendEntriesResponse {
                term: self.state.current_term,
                success: false,
            });
        }

        match self.state.state {
            NodeState::Follower => {}
            NodeState::Leader if request.term == self.state.current_term => {
                error!(
                    duplicated_leader_id = request.leader_id,
                    "Detected a duplicated leader"
                );
                return Ok(grpc::AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                });
            }
            NodeState::Candidate | NodeState::Leader => {
                self.back_to_follower(request.term);
            }
        };

        self.state.current_term = request.term;

        // This redundant implementation of reset_heartbeat_timeout is to avoid
        // the borrow checker error.
        let _guard;
        reset_timeout!(self, _guard, &mut self.state.heartbeat_deadline, 150);

        match self
            .config
            .storage
            .append_entries(
                request.prev_log_index,
                request.prev_log_term,
                request.entries.into_iter().map(Into::into).collect(),
            )
            .await
        {
            Ok(()) => Ok(grpc::AppendEntriesResponse {
                term: self.state.current_term,
                success: true,
            }),
            Err(log::StorageError::InconsistentPreviousEntry {
                expected_term,
                actual_term,
            }) => {
                // TODO: return a hint to the leader for quicker recovery.
                Ok(grpc::AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                })
            }
            Err(e) => {
                error!(error = e.to_string(), "Failed to append entries");
                // TODO: should return tonic::Status

                let mut status = tonic::Status::internal("failed to append entries");
                status.set_source(Arc::new(e));
                Err(status)
            }
        }
    }

    async fn grant_vote(&mut self, request: grpc::RequestVoteRequest) -> bool {
        if request.term <= self.state.current_term {
            return false;
        }

        // TODO: check log terms

        if let Some(vote) = self.vote.take() {
            vote.cancel().await;
        }
        self.leader.take();

        info!(
            current_term = self.state.current_term,
            new_term = request.term,
            candidate_id = request.candidate_id,
            "Granting vote"
        );

        self.state.current_term = request.term;
        self.state.voted_for = Some(request.candidate_id.clone());

        match self.state.state {
            NodeState::Follower => {}
            NodeState::Candidate | NodeState::Leader => {
                self.state.state = NodeState::Follower;
            }
        };
        true
    }

    fn vote_completed(&mut self, res: VoteResult) {
        let ignore_old = |vote_term| {
            if vote_term != self.state.current_term {
                trace!(
                    current_term = self.state.current_term,
                    vote_term,
                    "Ignoring the old vote result"
                );
                return true;
            }
            false
        };

        match res {
            VoteResult::Granted { vote_term } => {
                if ignore_old(vote_term) {
                    return;
                }
                self.state.state = NodeState::Leader;
                info!(
                    term = self.state.current_term,
                    "Vote granted, becoming a leader"
                );

                // TODO: safely set an infinitely far deadline
                self.state.heartbeat_deadline =
                    tokio::time::Instant::now() + tokio::time::Duration::from_secs(86400);

                self.leader = Some(leader::Leader::start(leader::Args {
                    id: self.config.id.clone(),
                    current_term: self.state.current_term,
                    msg_queue: self.tx.clone(),
                    peers: self.peers.clone(),
                    storage: self.config.storage.clone(),
                }));
            }
            VoteResult::NotGranted {
                vote_term,
                response: res,
            } => {
                if ignore_old(vote_term) {
                    return;
                }

                self.state.current_term = res.term;
                self.state.state = NodeState::Follower;
                info!(term = res.term, "Vote not granted, back to a follower");
                self.reset_heartbeat_timeout();
            }
        }
    }

    async fn request_vote(&mut self) {
        self.state.current_term += 1;
        self.state.state = NodeState::Candidate;
        self.reset_heartbeat_timeout();

        let vote = self.vote.take();
        if let Some(mut vote) = vote {
            if vote.is_active() {
                info!(
                    vote_term = vote.vote_term(),
                    "Canceling the previous vote process"
                );
                vote.cancel().await;
            }
        }
        self.vote = Some(vote::Vote::start(vote::Args {
            id: self.config.id.clone(),
            current_term: self.state.current_term,
            msg_queue: self.tx.clone(),
            peers: self.peers.clone(),
        }));
    }

    fn back_to_follower(&mut self, term: u64) {
        // There's a case that the leader wants to step down due to some
        // unrecoverable error, so `==` condition must be taken care of.
        //
        // TODO: in thi case, the process shouldn't become a leader again until
        // the problem is resolved.
        if term < self.state.current_term {
            return;
        }

        // TODO: should probably await here to ensure the old process exits.
        self.vote.take();
        self.leader.take();

        self.state.current_term = term;
        self.state.state = NodeState::Follower;
        self.reset_heartbeat_timeout();
    }
}
