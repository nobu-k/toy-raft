use crate::{grpc, log::StorageError, state_machine::ApplyResponseReceiver, StateMachineError};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Term(u64);

impl Term {
    pub fn new(term: u64) -> Self {
        Term(term)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn inc(&mut self) {
        self.0 += 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Index(u64);

impl Index {
    pub fn new(index: u64) -> Self {
        Index(index)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    pub fn prev(&self) -> Self {
        Index(self.0 - 1)
    }

    pub fn next(&self) -> Self {
        Index(self.0 + 1)
    }

    pub fn dec(&mut self) {
        self.0 -= 1;
    }

    pub fn inc(&mut self) {
        self.0 += 1;
    }
}

pub type PeerJoinSet<R> = tokio::task::JoinSet<PeerJoinResponse<R>>;
pub type PeerJoinResult<R> = Result<PeerJoinResponse<R>, tokio::task::JoinError>;

pub struct PeerJoinResponse<R> {
    pub id: Arc<String>,
    pub result: Result<tonic::Response<R>, tonic::Status>,
}

#[derive(Debug, Clone)]
pub struct PeerClient {
    // id is Arc to reduce unnecessary cloning.
    pub id: Arc<String>,
    pub client: grpc::raft_client::RaftClient<tonic::transport::Channel>,
}

#[derive(Debug, Clone, Copy)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

// TODO: rename ActorState and state because there are too many "state"s
#[derive(Debug, Clone)]
pub struct ActorState {
    pub current_term: Term,
    pub voted_for: Option<String>,
    pub state: NodeState,
    pub heartbeat_deadline: tokio::time::Instant,
}

pub enum Message {
    GetState(tokio::sync::oneshot::Sender<ActorState>),
    AppendEntry {
        entry: Arc<Vec<u8>>,
        require_response: bool,

        // TODO: properly define the response type
        result:
            tokio::sync::oneshot::Sender<Result<Option<ApplyResponseReceiver>, AppendEntryError>>,
    },
    AppendEntries {
        request: grpc::AppendEntriesRequest,
        result: tokio::sync::oneshot::Sender<Result<grpc::AppendEntriesResponse, tonic::Status>>,
    },
    GrantVote {
        request: grpc::RequestVoteRequest,
        result: tokio::sync::oneshot::Sender<(ActorState, bool)>,
    },
    VoteCompleted(VoteResult),
    BackToFollower {
        term: Term,
    },
}

pub enum VoteResult {
    Granted {
        /// The term of the voting process was initiated. Ignore the result when
        /// the value is older than the current term upon receiving.
        vote_term: Term,
    },
    NotGranted {
        /// The term of the voting process was initiated. Ignore the result when
        /// the value is older than the current term upon receiving.
        vote_term: Term,
    },
    NewerTermFound {
        /// The term of the voting process was initiated. Ignore the result when
        /// the value is older than the current term upon receiving.
        vote_term: Term,
        response: grpc::RequestVoteResponse,
    },
}

/// The error response of the AppendEntry message.
#[derive(Debug, thiserror::Error)]
pub enum AppendEntryError {
    /// The error indicates that the operation to the state machine cannot be
    /// performed by non-leader nodes. When this error happens, call the
    /// corresponding operation of the leader node.
    ///
    /// This error contains the ID of the leader when available.
    #[error("operation can only be performed by leader")]
    NotLeader(Option<String>),

    #[error("failed to write an entry: {0}")]
    StorageError(#[from] StorageError),

    #[error("failed to apply an entry to the state machine: {0}")]
    StateMachineError(StateMachineError),
}

#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("failed to send a message: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("failed to receive a message: {0}")]
    ReceiveError(#[from] tokio::sync::oneshot::error::RecvError),
}
