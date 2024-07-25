use crate::grpc;

pub struct Raft {
    current_term: u64,
    voted_for: Option<String>,
    state: State,
    heartbeat_deadline: std::time::Instant,
}

#[derive(Debug, Clone, Copy)]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

impl Raft {
    pub fn new() -> Raft {
        Raft {
            current_term: 0,
            voted_for: None,
            state: State::Follower,
            heartbeat_deadline: std::time::Instant::now() + std::time::Duration::from_millis(150), // TODO: randomize
        }
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn grant_vote(&mut self, msg: &grpc::RequestVoteRequest) -> bool {
        if msg.term < self.current_term {
            return false;
        }

        // TODO: check log terms

        self.voted_for = Some(msg.candidate_id.clone());
        match self.state {
            State::Follower => {}
            State::Candidate | State::Leader => {
                self.state = State::Follower;
                self.voted_for = Some(msg.candidate_id.clone());
            }
        };
        true
    }
}
