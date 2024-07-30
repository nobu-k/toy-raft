use std::net::ToSocketAddrs;

pub struct Config {
    pub(crate) id: String,
    pub(crate) addr: String,
    pub(crate) peers: Vec<String>,
}

pub struct Builder {
    id: Option<String>,
    addr: Option<String>,
    peers: Vec<String>,
}

impl Config {
    pub fn builder() -> Builder {
        Builder {
            id: None,
            addr: None,
            peers: vec![],
        }
    }
}

impl Builder {
    pub fn build(self) -> Result<Config, ConfigErrors> {
        let errors: Vec<ConfigError> = [
            self.validate_id(),
            self.validate_addr(),
            self.validate_peers(),
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
            let _ = url::Url::parse(peer)
                .map_err(|e| errors.push(ConfigError::InvalidPeer(peer.clone(), e)));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing required parameter: {}", .0)]
    MissingRequiredParameter(String),

    #[error("invalid address: {}, {}", .0, .1)]
    InvalidAddress(String, #[source] std::io::Error),

    #[error("the number of peers must be greater than or equal to 2: {0}")]
    InsufficientPeers(usize),

    #[error("peers must have valid URLs: {}: {}", .0, .1)]
    InvalidPeer(String, #[source] url::ParseError),
}

#[derive(Debug, thiserror::Error)]
#[error("configuration errors:\n{}", errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n"))]
pub struct ConfigErrors {
    errors: Vec<ConfigError>,
}
