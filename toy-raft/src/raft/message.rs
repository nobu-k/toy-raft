use crate::grpc;

#[derive(Debug, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

// TODO: rename ActorState and state because there are too many "state"s
#[derive(Debug, Clone)]
pub struct ActorState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub state: NodeState,
    pub heartbeat_deadline: tokio::time::Instant,
}

pub enum Message {
    GetState(tokio::sync::oneshot::Sender<ActorState>),
    AppendEntries {
        request: grpc::AppendEntriesRequest,
        result: tokio::sync::oneshot::Sender<grpc::AppendEntriesResponse>,
    },
    GrantVote {
        request: grpc::RequestVoteRequest,
        result: tokio::sync::oneshot::Sender<(ActorState, bool)>,
    },
    VoteCompleted(VoteResult),
    BackToFollower {
        term: u64,
    },
}

pub enum VoteResult {
    Granted {
        /// The term of the voting process was initiated. Ignore the result when
        /// the value is older than the current term upon receiving.
        vote_term: u64,
    },
    NotGranted {
        /// The term of the voting process was initiated. Ignore the result when
        /// the value is older than the current term upon receiving.
        vote_term: u64,
        response: grpc::RequestVoteResponse,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("failed to send a message: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("failed to receive a message: {0}")]
    ReceiveError(#[from] tokio::sync::oneshot::error::RecvError),
}
