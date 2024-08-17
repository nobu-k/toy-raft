use crate::{
    grpc,
    message::{Index, Term},
    state_machine::{ApplyResponseReceiver, ApplyResponseSender},
};
use std::sync::Arc;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The error type providing common errors for storage engines.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The given term is older than the latest term in the storage.
    #[error(
        "the given term is older than the latest term in the storage: {} < {}", term.get(), latest_term.get()
    )]
    StaleTerm {
        /// The term given to append_entry.
        term: Term,

        /// The latest term in the storage.
        latest_term: Term,
    },

    #[error("the storage is full{}", .0.as_ref().map_or_else(|| "".to_owned(), |e| format!(": {}", e)))]
    StorageFull(#[source] Option<BoxedError>),

    #[error("the requested previous log entry was not found or found but the term does not match")]
    InconsistentPreviousEntry {
        expected_term: Term,
        /// The actual term of the corresponding entry. If this is None, it
        /// means the entry does not exist at prev_index.
        actual_term: Option<Term>,
    },

    /// Any custom error that the storage engine can return.
    #[error("storage engine specific error: {0}")]
    StorageEngineError(#[source] BoxedError),
}

/// The trait that the log storage engine need to implement. The index starts
/// from 1.
///
/// Storage engines usually need fine-grained locking for performance. So, this
/// trait is designed to be used with Arc with interior mutability implemented
/// by each engine.
#[async_trait::async_trait]
pub trait Storage {
    /// Returns the first index of the log stored in the storage. The first
    /// index might not be 0 because the log can be compacted. It returns None
    /// when the log is empty.
    async fn get_first_entry(&self) -> Result<Option<Entry>, StorageError>;

    /// Returns the last index of the log stored in the storage. It returns None
    /// when the log is empty.
    async fn get_last_entry(&self) -> Result<Option<Entry>, StorageError>;

    /// Returns the entry at the given index. It returns None when the index
    /// does not exist.
    async fn get_entry(&self, index: Index) -> Result<Option<Entry>, StorageError>;

    /// Another version of get_entry used when the entry needs to be applied to
    /// the state machine. The storage must return the channel, which is
    /// internally managed, to send the response from the state machine to the
    /// original requester.
    async fn get_entry_for_apply(
        &self,
        index: Index,
    ) -> Result<Option<(Entry, Option<ApplyResponseSender>)>, StorageError>;

    /// Returns the entries after the given index. The entry at the index
    /// position will not be included.
    // TODO: add some configuration such as read as many entries as possible in given time or until the size limit.
    async fn get_entries_after(&self, index: Index) -> Result<Vec<Entry>, StorageError>;

    /// Appends a new entry to the log. It returns the index information of the
    /// new entry. This method is usually called by the leader.
    async fn append_entry(
        &self,
        term: Term,
        entry: Arc<Vec<u8>>,
        require_response: bool,
    ) -> Result<(Entry, Option<ApplyResponseReceiver>), StorageError>;

    /// Append the entries provided by the leader. If the prev_index is not the
    /// last entry, the storage will delete the existing entries after the
    /// prev_index and append the new entries.
    ///
    /// The storage MUST NOT perform log truncation when new_entries is empty
    /// because it is just a heartbeat. In that case, the storage only needs to
    /// check if the prev_index and prev_term pair matches the corresponding
    /// entry in the log.
    ///
    /// It returns StorageError::InconsistentPreviousEntry when the prev_index
    /// does not exist or exists but the term does not match.
    // TODO: return the potentially matching index or term to optimize initialization process at the leader.
    async fn append_entries(
        &self,
        prev_index: Index,
        prev_term: Term,
        new_entries: Vec<Entry>,
    ) -> Result<(), StorageError>;

    // TODO: add method for compaction using last_applied_index and
    // compaction_index. This method can also perform the garbage collection of
    // response channels.
}

/// Entry represents a single log entry.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// The index of the entry in the log.
    index: Index,

    /// The term that this entry has been written.
    term: Term,

    /// The data of the entry. The content can be empty.
    data: Arc<Vec<u8>>,
    // TODO: add a response channel of the state machine.
}

impl Entry {
    /// Creates a new Entry instance.
    pub fn new(index: Index, term: Term, data: Arc<Vec<u8>>) -> Self {
        Entry {
            index,
            term,
            data: data,
        }
    }

    /// Returns the index of the entry.
    pub fn index(&self) -> Index {
        self.index
    }

    /// Returns the term of the entry.
    pub fn term(&self) -> Term {
        self.term
    }

    /// Returns the data of the entry.
    pub fn data(&self) -> Arc<Vec<u8>> {
        self.data.clone()
    }
}

impl From<grpc::LogEntry> for Entry {
    fn from(entry: grpc::LogEntry) -> Self {
        Entry {
            index: Index::new(entry.index),
            term: Term::new(entry.term),
            data: Arc::new(entry.data),
        }
    }
}
