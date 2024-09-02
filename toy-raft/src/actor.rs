use crate::{grpc, state_machine::ApplyResponseReceiver, ApplyResponse};

use super::{leader, log, message::*, vote, StateMachineError};
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
    pub async fn new(config: crate::config::Config) -> Result<Actor, StateMachineError> {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(());
        let (commit_index_tx, commit_index_rx) = tokio::sync::watch::channel(Index::new(0));

        let _writer = Arc::new(
            super::writer::Writer::start(super::writer::Args {
                cancel: cancel_rx.clone(),
                storage: config.storage.clone(),
                state_machine: config.state_machine.clone(),
                commit_index: commit_index_rx.clone(),
            })
            .await?,
        );

        let (msg_tx, msg_rx) = tokio::sync::mpsc::channel(32);

        let peers: Vec<_> = config
            .peers
            .iter()
            .map(|peer| {
                // Config has already been validated.
                let url = peer.addr.parse::<http::Uri>().unwrap();

                // TODO: the current client doesn't reconnect automatically.
                // Support reconnection.
                //
                // See: https://github.com/hyperium/tonic/issues/1254
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
                current_term: Term::new(0),
                voted_for: None,
                state: NodeState::Follower,
                heartbeat_deadline: tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(150), // TODO: randomize
            },
            config,
            peers,
            rng: rand::rngs::StdRng::from_entropy(),
            commit_index_tx,
            commit_index_rx,
            leader: None,
            vote: None,
            cancel: cancel_rx,
            tx: msg_tx.clone(),
            rx: msg_rx,
        };
        tokio::spawn(actor.run());

        Ok(Actor {
            _cancel: cancel_tx,
            message_queue: msg_tx,
        })
    }

    pub async fn state(&self) -> Result<ActorState, MessageError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_queue.send(Message::GetState(tx)).await?;
        Ok(rx.await?)
    }

    /// Append a new entry to the log. When require_response is false, this
    /// method returns immediately after writing the entry to the log even if
    /// the entry is not applied to the state machine.
    ///
    /// This method fails when the node is not a leader.
    pub async fn append_entry(
        &self,
        entry: Arc<Vec<u8>>,
        require_response: bool,
    ) -> Result<Result<Option<ApplyResponse>, AppendEntryError>, MessageError> {
        // TODO: refactor response error type.
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_queue
            .send(Message::AppendEntry {
                entry,
                require_response,
                result: tx,
            })
            .await?;
        match rx.await? {
            Ok(Some(rx)) => Ok(Ok(rx.await?)),
            Ok(None) => Ok(Ok(None)),
            Err(e) => Ok(Err(e)),
        }
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

    commit_index_tx: tokio::sync::watch::Sender<Index>,
    commit_index_rx: tokio::sync::watch::Receiver<Index>,
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
            Message::AppendEntry {
                entry,
                require_response,
                result,
            } => {
                let response = self.append_entry(entry, require_response).await;
                if result.send(response).is_err() {
                    info!("Returning the result of AppendEntry failed");
                }
            }
            Message::AppendEntries { request, result } => {
                let response = self.append_entries(request).await;
                if result.send(response).is_err() {
                    info!("Returning the result of AppendEntries failed");
                }
            }
            Message::GrantVote { request, result } => {
                // Never reset the heartbeat here to minimize the possibility
                // that a stale candidate keeps trying to become a leader.
                let granted = self.grant_vote(request).await;
                if result.send((self.state.clone(), granted)).is_err() {
                    info!("Returning the result of GrantVote failed");
                }
            }
            Message::VoteCompleted(res) => self.vote_completed(res),
            Message::BackToFollower { term } => self.back_to_follower(term),
        }
    }

    async fn append_entry(
        &self,
        entry: Arc<Vec<u8>>,
        require_response: bool,
    ) -> Result<Option<ApplyResponseReceiver>, AppendEntryError> {
        match self.leader {
            Some(ref leader) => Ok(leader.append_entry(entry, require_response).await?),
            None => Err(AppendEntryError::NotLeader(self.state.voted_for.clone())),
        }
    }

    async fn append_entries(
        &mut self,
        request: grpc::AppendEntriesRequest,
    ) -> Result<grpc::AppendEntriesResponse, tonic::Status> {
        if request.term < self.state.current_term.get() {
            info!(
                leader_id = self
                    .state
                    .voted_for
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or(""),
                "The leader is stale"
            );
            // The term is too old, so don't reset the heartbeat timeout here.
            return Ok(grpc::AppendEntriesResponse {
                term: self.state.current_term.get(),
                success: false,
            });
        }

        match self.state.state {
            NodeState::Follower => {}
            NodeState::Leader if request.term == self.state.current_term.get() => {
                error!(
                    duplicated_leader_id = request.leader_id,
                    "Detected a duplicated leader"
                );
                return Ok(grpc::AppendEntriesResponse {
                    term: self.state.current_term.get(),
                    success: false,
                });
            }
            NodeState::Candidate | NodeState::Leader => {
                self.back_to_follower(Term::new(request.term));
            }
        };

        if self.state.voted_for.is_none() {
            info!(
                term = self.state.current_term.get(),
                new_term = request.term,
                leader_id = request.leader_id,
                "Found a new leader",
            )
        }
        self.state.current_term = Term::new(request.term);
        self.state.voted_for = Some(request.leader_id.clone());

        // This redundant implementation of reset_heartbeat_timeout is to avoid
        // the borrow checker error.
        let _guard;
        reset_timeout!(self, _guard, &mut self.state.heartbeat_deadline, 150);

        match self
            .config
            .storage
            .append_entries(
                Index::new(request.prev_log_index),
                Term::new(request.prev_log_term),
                request.entries.into_iter().map(Into::into).collect(),
            )
            .await
        {
            Ok(()) => {
                self.commit_index_tx.send_if_modified(|index| {
                    if index.get() < request.leader_commit {
                        *index = Index::new(request.leader_commit);
                        true
                    } else {
                        false
                    }
                });
                Ok(grpc::AppendEntriesResponse {
                    term: self.state.current_term.get(),
                    success: true,
                })
            }
            Err(log::StorageError::InconsistentPreviousEntry {
                expected_term,
                actual_term,
            }) => {
                if let Some(actual_term) = actual_term {
                    info!(
                        expected_term = expected_term.get(),
                        actual_term = actual_term.get(),
                        "Log inconsistency detected"
                    );
                }
                // TODO: return a hint to the leader for quicker recovery.
                Ok(grpc::AppendEntriesResponse {
                    term: self.state.current_term.get(),
                    success: false,
                })
            }
            Err(e) => {
                error!(error = e.to_string(), "Failed to append entries");
                let mut status = tonic::Status::internal("failed to append entries");
                status.set_source(Arc::new(e));
                Err(status)
            }
        }
    }

    async fn request_vote(&mut self) {
        let (last_log_index, last_log_term) = match self.config.storage.get_last_entry().await {
            Ok(Some(entry)) => (entry.index(), entry.term()),
            Ok(None) => (Index::new(0), Term::new(0)),
            Err(e) => {
                error!(
                    error = e.to_string(),
                    "Failed to get the last entry of the log before request vote, keep being a follower"
                );
                self.reset_heartbeat_timeout();
                return;
            }
        };

        self.state.current_term.inc();
        self.state.state = NodeState::Candidate;
        self.state.voted_for = None;
        self.reset_heartbeat_timeout();

        let vote = self.vote.take();
        if let Some(mut vote) = vote {
            if vote.is_active() {
                info!(
                    vote_term = vote.vote_term().get(),
                    "Canceling the previous vote process"
                );
                vote.cancel().await;
            }
        }
        self.vote = Some(vote::Vote::start(vote::Args {
            id: self.config.id.clone(),
            current_term: self.state.current_term,
            last_log_index,
            last_log_term,
            msg_queue: self.tx.clone(),
            peers: self.peers.clone(),
        }));
    }

    async fn grant_vote(&mut self, request: grpc::RequestVoteRequest) -> bool {
        if request.term <= self.state.current_term.get() {
            return false;
        }
        if self.state.voted_for.is_some() {
            return false;
        }
        let (last_index, last_term) = match self.config.storage.get_last_entry().await {
            Ok(Some(entry)) => (entry.index(), entry.term()),
            Ok(None) => (Index::new(0), Term::new(0)),
            Err(e) => {
                error!(
                    error = e.to_string(),
                    "Failed to get the last entry of the log"
                );
                // This process can't be certain about the vote request, so
                // returning false.
                return false;
            }
        };

        // Check the safety guarantee for the Leader Completeness property.
        if request.last_log_index < last_index.get() || request.last_log_term < last_term.get() {
            return false;
        }

        // TODO: check log terms

        if let Some(vote) = self.vote.take() {
            vote.cancel().await;
        }
        self.leader.take();

        info!(
            current_term = self.state.current_term.get(),
            new_term = request.term,
            candidate_id = request.candidate_id,
            "Granting vote"
        );

        self.state.current_term = Term::new(request.term);
        self.state.voted_for = Some(request.candidate_id.clone());
        self.reset_heartbeat_timeout();

        match self.state.state {
            NodeState::Follower => {}
            NodeState::Candidate | NodeState::Leader => {
                self.state.state = NodeState::Follower;
            }
        };
        true
    }

    fn vote_completed(&mut self, res: VoteResult) {
        let ignore_old = |vote_term: Term| {
            if vote_term != self.state.current_term {
                trace!(
                    current_term = self.state.current_term.get(),
                    vote_term = vote_term.get(),
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
                    term = self.state.current_term.get(),
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
                    commit_index_tx: self.commit_index_tx.clone(),
                    commit_index_rx: self.commit_index_rx.clone(),
                }));
            }
            VoteResult::NotGranted { vote_term } => {
                if ignore_old(vote_term) {
                    return;
                }
                self.state.state = NodeState::Follower;
                info!(
                    term = self.state.current_term.get(),
                    "Vote not granted, back to a follower"
                );
                self.reset_heartbeat_timeout();
            }
            VoteResult::NewerTermFound {
                vote_term,
                response: res,
            } => {
                if ignore_old(vote_term) {
                    return;
                }

                self.state.current_term = Term::new(res.term);
                self.state.state = NodeState::Follower;
                info!(
                    term = res.term,
                    "Vote not granted but a node with a newer term found, back to a follower"
                );
                self.reset_heartbeat_timeout();
            }
        }
    }

    fn back_to_follower(&mut self, term: Term) {
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
