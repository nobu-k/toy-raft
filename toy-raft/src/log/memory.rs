use crate::{
    message::{Index, Term},
    state_machine::{ApplyResponseReceiver, ApplyResponseSender},
};

use super::storage::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub struct MemoryStorage {
    entries: RwLock<Vec<Entry>>,
    response_channels: RwLock<HashMap<Index, ApplyResponseSender>>,
    snapshot: Option<Vec<u8>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            entries: RwLock::new(vec![]),
            response_channels: RwLock::new(HashMap::new()),
            snapshot: None,
        }
    }
}

#[async_trait::async_trait]
impl Storage for MemoryStorage {
    async fn get_first_entry(&self) -> Result<Option<Entry>, StorageError> {
        let entries = self.entries.read().await;
        match entries.first() {
            Some(entry) => Ok(Some(entry.clone())),
            None => Ok(None),
        }
    }

    async fn get_last_entry(&self) -> Result<Option<Entry>, StorageError> {
        let entries = self.entries.read().await;
        match entries.last() {
            Some(entry) => Ok(Some(entry.clone())),
            None => Ok(None),
        }
    }

    async fn get_entry(&self, index: Index) -> Result<Option<Entry>, StorageError> {
        if index.get() == 0 {
            return Ok(None);
        }

        let entries = self.entries.read().await;
        match search_entry(&entries, index) {
            Some(e) => Ok(entries.get(e).cloned()),
            None => Ok(None),
        }
    }

    async fn get_entry_for_apply(
        &self,
        index: Index,
    ) -> Result<Option<(Entry, Option<ApplyResponseSender>)>, StorageError> {
        if index.get() == 0 {
            return Ok(None);
        }

        // Read lock is fine because the entries will not be modified although
        // response_channels will have a new entry.
        let entries = self.entries.read().await;
        match search_entry(&entries, index) {
            Some(e) => {
                let mut response_channels = self.response_channels.write().await;
                let tx = response_channels.remove(&index);
                Ok(Some((entries.get(e).unwrap().clone(), tx)))
            }
            None => Ok(None),
        }
    }

    async fn get_entries_after(&self, index: Index) -> Result<Vec<Entry>, StorageError> {
        let entries = self.entries.read().await;
        match search_entry(&entries, index.next()) {
            Some(e) => Ok(entries[e..].to_vec()),
            None => Ok(Vec::new()),
        }
    }

    async fn append_entry(
        &self,
        term: Term,
        entry: Arc<Vec<u8>>,
        require_response: bool,
    ) -> Result<(Entry, Option<ApplyResponseReceiver>), StorageError> {
        let mut entries = self.entries.write().await;
        if let Some(last) = entries.last() {
            if last.term() > term {
                return Err(StorageError::StaleTerm {
                    term: term,
                    latest_term: last.term(),
                });
            }
        }

        let next_index = Index::new(entries.last().map_or(0, |entry| entry.index().get()) + 1);
        let entry = Entry::new(next_index, term, entry);
        entries.push(entry.clone());

        if !require_response {
            return Ok((entry, None));
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.response_channels.write().await.insert(next_index, tx);
        Ok((entry, Some(rx)))
    }

    async fn append_entries(
        &self,
        prev_index: Index,
        prev_term: Term,
        new_entries: Vec<Entry>,
    ) -> Result<(), StorageError> {
        let mut entries = self.entries.write().await;

        if prev_index.get() == 0 {
            entries.clear();
            entries.extend(new_entries);
            return Ok(());
        }

        match search_entry(&entries, prev_index) {
            Some(prev_entry) => {
                let entry = &entries[prev_entry];
                if entry.term() != prev_term {
                    return Err(StorageError::InconsistentPreviousEntry {
                        expected_term: prev_term,
                        actual_term: Some(entry.term()),
                    });
                }

                if new_entries.is_empty() {
                    // This is a heartbeat message. The truncation MUST NOT be
                    // performed.
                    return Ok(());
                }

                entries.truncate(prev_entry + 1);
                entries.extend(new_entries);

                // Remove the response channels for truncated entries.
                self.response_channels
                    .write()
                    .await
                    .retain(|index, _| *index <= prev_index);
                Ok(())
            }
            None => Err(StorageError::InconsistentPreviousEntry {
                expected_term: prev_term,
                actual_term: None,
            }),
        }
    }
}

fn search_entry(entries: &[Entry], index: Index) -> Option<usize> {
    match entries.binary_search_by(|entry| entry.index().cmp(&index)) {
        Ok(e) => Some(e),
        Err(_) => None,
    }
}

// TODO: test response channel

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_entry() {
        let storage = MemoryStorage::new();
        let (entry, _) = storage
            .append_entry(Term::new(2), Arc::new(vec![1, 2, 3]), false)
            .await
            .unwrap();
        assert_eq!(entry.index(), Index::new(1));
        assert_eq!(entry.term(), Term::new(2));
        assert_eq!(entry.data().as_ref(), &[1, 2, 3]);

        let (entry, _) = storage
            .append_entry(Term::new(2), Arc::new(vec![4, 5, 6]), false)
            .await
            .unwrap();
        assert_eq!(entry.index(), Index::new(2));
        assert_eq!(entry.term(), Term::new(2));
        assert_eq!(entry.data().as_ref(), &[4, 5, 6]);

        match storage
            .append_entry(Term::new(1), Arc::new(vec![1, 2, 3]), false)
            .await
        {
            Err(StorageError::StaleTerm { term, latest_term }) => {
                assert_eq!(term, Term::new(1));
                assert_eq!(latest_term, Term::new(2));
            }
            Err(_) => panic!("unexpected error"),
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[tokio::test]
    async fn test_get_entry() {
        let storage = MemoryStorage::new();
        storage
            .append_entry(Term::new(1), Arc::new(vec![1, 2, 3]), false)
            .await
            .unwrap();
        storage
            .append_entry(Term::new(1), Arc::new(vec![4, 5, 6]), false)
            .await
            .unwrap();

        let entry = storage.get_entry(Index::new(1)).await.unwrap().unwrap();
        assert_eq!(entry.index(), Index::new(1));
        assert_eq!(entry.term(), Term::new(1));
        assert_eq!(entry.data().as_ref(), &[1, 2, 3]);

        let entry = storage.get_entry(Index::new(2)).await.unwrap().unwrap();
        assert_eq!(entry.index(), Index::new(2));
        assert_eq!(entry.term(), Term::new(1));
        assert_eq!(entry.data().as_ref(), &[4, 5, 6]);

        assert!(storage.get_entry(Index::new(3)).await.unwrap().is_none());
        assert!(storage.get_entry(Index::new(0)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_append_entries() {
        let storage = MemoryStorage::new();
        storage
            .append_entry(Term::new(1), Arc::new(vec![1, 2, 3]), false)
            .await
            .unwrap();
        storage
            .append_entry(Term::new(1), Arc::new(vec![4, 5, 6]), false)
            .await
            .unwrap();

        let entries = vec![
            Entry::new(Index::new(3), Term::new(2), Arc::new(vec![7, 8, 9])),
            Entry::new(Index::new(4), Term::new(2), Arc::new(vec![10, 11, 12])),
        ];
        storage
            .append_entries(Index::new(2), Term::new(1), entries.clone())
            .await
            .unwrap();

        let entry = storage.get_entry(Index::new(3)).await.unwrap().unwrap();
        assert_eq!(entry, entries[0]);

        let entry = storage.get_entry(Index::new(4)).await.unwrap().unwrap();
        assert_eq!(entry, entries[1]);

        let entry = storage.get_entry(Index::new(1)).await.unwrap().unwrap();
        assert_eq!(entry.index(), Index::new(1));
        assert_eq!(entry.term(), Term::new(1));
        assert_eq!(entry.data().as_ref(), &[1, 2, 3]);

        let entry = storage.get_entry(Index::new(2)).await.unwrap().unwrap();
        assert_eq!(entry.index(), Index::new(2));
        assert_eq!(entry.term(), Term::new(1));
        assert_eq!(entry.data().as_ref(), &[4, 5, 6]);

        match storage
            .append_entries(Index::new(3), Term::new(1), entries.clone())
            .await
        {
            Err(StorageError::InconsistentPreviousEntry {
                expected_term,
                actual_term,
            }) => {
                assert_eq!(expected_term, Term::new(1));
                assert_eq!(actual_term, Some(Term::new(2)));
            }
            Err(_) => panic!("unexpected error"),
            Ok(_) => panic!("unexpected success"),
        }

        match storage
            .append_entries(Index::new(10), Term::new(2), entries.clone())
            .await
        {
            Err(StorageError::InconsistentPreviousEntry {
                expected_term,
                actual_term,
            }) => {
                assert_eq!(expected_term, Term::new(2));
                assert_eq!(actual_term, None);
            }
            Err(_) => panic!("unexpected error"),
            Ok(_) => panic!("unexpected success"),
        }

        match storage
            .append_entries(Index::new(1), Term::new(1), vec![])
            .await
        {
            Err(_) => panic!("unexpected error"),
            Ok(_) => {}
        }

        let entry = storage.get_entry(Index::new(4)).await.unwrap().unwrap();
        assert_eq!(entry, entries[1], "no truncation should be performed");
    }

    #[tokio::test]
    async fn test_sync_send() {
        let _: Arc<dyn Storage + Sync + Send + 'static> = Arc::new(MemoryStorage::new());
    }
}
