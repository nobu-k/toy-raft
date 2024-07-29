use crate::grpc;
use tracing::info;

pub struct Raft {
    message_queue: tokio::sync::mpsc::Sender<Message>,
}

#[derive(Debug, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

impl Raft {
    pub fn new() -> Raft {
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        let actor = Actor {
            state: ActorState {
                current_term: 0,
                voted_for: None,
                state: NodeState::Follower,
                heartbeat_deadline: tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(150), // TODO: randomize
            },
            rx,
        };
        tokio::spawn(actor.run());

        Raft { message_queue: tx }
    }

    pub async fn state(&self) -> Result<ActorState, MessageError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.message_queue.send(Message::GetState(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn grant_vote(
        &mut self,
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

struct Actor {
    state: ActorState,

    /// Receiver for messages. This channel also acts as a trigger to stop the
    /// actor (i.e. cancellation) that happens when Raft is dropped.
    rx: tokio::sync::mpsc::Receiver<Message>,
}

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
}

impl Actor {
    pub async fn run(mut self) {
        info!("Starting Raft actor");
        loop {
            // TODO: heartbeat timeout
            tokio::select! {
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
            }
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
}
