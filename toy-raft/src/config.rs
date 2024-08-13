use std::{net::ToSocketAddrs, sync::Arc};

pub type SharedStorage = Arc<dyn crate::raft::log::Storage + Sync + Send + 'static>;
pub type SharedStateMachine = Arc<dyn crate::raft::StateMachine + Sync + Send + 'static>;

#[derive(Clone)]
pub struct Config {
    pub(crate) id: String,
    pub(crate) addr: String,
    pub(crate) peers: Vec<Peer>,
    pub(crate) storage: SharedStorage,
    pub(crate) state_machine: SharedStateMachine,
}

pub struct Builder {
    id: Option<String>,
    addr: Option<String>,
    peers: Vec<Peer>,
    storage: Option<SharedStorage>,
    state_machine: Option<SharedStateMachine>,
}

impl Config {
    pub fn builder() -> Builder {
        Builder {
            id: None,
            addr: None,
            peers: vec![],
            storage: None,
            state_machine: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub id: String,
    pub addr: String,
}

impl Peer {
    /// Parse a peer string into a Peer struct. The format must be
    /// `<id>@<addr>`.
    pub fn parse(peer: String) -> Result<Peer, ConfigError> {
        let mut parts = peer.splitn(2, '@');
        let id = parts
            .next()
            .ok_or(ConfigError::InvalidPeerFormat(peer.clone()))?
            .to_owned();
        let addr = parts
            .next()
            .ok_or(ConfigError::InvalidPeerFormat(peer.clone()))?
            .to_owned();
        Ok(Peer { id, addr })
    }

    /// Parse args parses the peer strings into Peer structs. The values can
    /// contain comma-separated list of peers in `<id>@<addr>` format.
    pub fn parse_args<'a, S, T>(peers: &'a S) -> Result<Vec<Peer>, ConfigErrors>
    where
        S: std::convert::AsRef<[T]>,
        T: std::convert::AsRef<str>,
    {
        let mut errors = vec![];
        let res = peers
            .as_ref()
            .iter()
            .flat_map(|p| p.as_ref().split(','))
            .filter_map(|p| match Self::parse(p.to_owned()) {
                Ok(peer) => Some(peer),
                Err(e) => {
                    errors.push(e);
                    None
                }
            })
            .collect();

        if errors.is_empty() {
            Ok(res)
        } else {
            Err(ConfigErrors { errors })
        }
    }
}

impl Builder {
    pub fn build(mut self) -> Result<Config, ConfigErrors> {
        // For smooth integration with StatefulSet, peer can contain the
        // current server. It's excluded from the list.
        self.peers = if let Some(id) = self.id.as_ref() {
            self.peers.into_iter().filter(|p| p.id != *id).collect()
        } else {
            self.peers
        };

        let errors: Vec<ConfigError> = [
            self.validate_id(),
            self.validate_addr(),
            self.validate_peers(),
            self.validate_state_machine(),
        ]
        .into_iter()
        .filter_map(|e| e.err())
        .flatten()
        .collect();
        if !errors.is_empty() {
            return Err(ConfigErrors { errors });
        }

        Ok(Config {
            id: self.id.unwrap(),
            addr: self.addr.unwrap(),
            peers: self.peers,
            storage: self
                .storage
                .unwrap_or_else(|| Arc::new(crate::raft::log::MemoryStorage::new())),
            state_machine: self.state_machine.unwrap(),
        })
    }

    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }

    pub fn addr(mut self, addr: String) -> Self {
        self.addr = Some(addr);
        self
    }

    /// Set the list of peers. The list can contain the current server.
    pub fn peers(mut self, peers: Vec<Peer>) -> Self {
        self.peers = peers;
        self
    }

    pub fn storage(mut self, storage: SharedStorage) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn state_machine(mut self, state_machine: SharedStateMachine) -> Self {
        self.state_machine = Some(state_machine);
        self
    }

    fn validate_id(&self) -> Result<(), Vec<ConfigError>> {
        if self.id.is_none() || self.id.as_ref().unwrap().is_empty() {
            Err(vec![ConfigError::MissingRequiredParameter("id".to_owned())])
        } else {
            Ok(())
        }
    }

    fn validate_addr(&self) -> Result<(), Vec<ConfigError>> {
        if self.addr.is_none() || self.addr.as_ref().unwrap().is_empty() {
            return Err(vec![ConfigError::MissingRequiredParameter(
                "addr".to_owned(),
            )]);
        }

        let addr = self.addr.as_ref().unwrap();
        if let Err(e) = addr.to_socket_addrs() {
            Err(vec![ConfigError::InvalidAddress(addr.clone(), e)])
        } else {
            Ok(())
        }
    }

    fn validate_peers(&self) -> Result<(), Vec<ConfigError>> {
        let mut errors = vec![];

        // TODO: restrict the minimum number of peers
        // if self.peers.len() < 2 {
        //     errors.push(ConfigError::InsufficientPeers(self.peers.len()));
        // }

        for peer in &self.peers {
            // TODO: validate the peer ID
            let _ = peer
                .addr
                .parse::<http::Uri>()
                .map_err(|e| errors.push(ConfigError::InvalidPeerAddr(peer.addr.clone(), e)));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn validate_state_machine(&self) -> Result<(), Vec<ConfigError>> {
        if self.state_machine.is_none() {
            Err(vec![ConfigError::MissingRequiredParameter(
                "state_machine".to_owned(),
            )])
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required parameter: {}", .0)]
    MissingRequiredParameter(String),

    #[error("invalid peer format (must be <id>@<addr>): {0}")]
    InvalidPeerFormat(String),

    #[error("invalid address: {}, {}", .0, .1)]
    InvalidAddress(String, #[source] std::io::Error),

    #[error("the number of peers must be greater than or equal to 2: {0}")]
    InsufficientPeers(usize),

    #[error("peers must have valid URLs: {}: {}", .0, .1)]
    InvalidPeerAddr(String, #[source] http::uri::InvalidUri),
}

#[derive(Debug, thiserror::Error)]
#[error("configuration errors:\n{}", errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n"))]
pub struct ConfigErrors {
    errors: Vec<ConfigError>,
}
