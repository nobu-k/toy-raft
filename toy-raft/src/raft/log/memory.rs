use super::storage::*;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MemoryStorage {
    entries: RwLock<Vec<Entry>>,
    snapshot: Option<Vec<u8>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            entries: RwLock::new(vec![]),
            snapshot: None,
        }
    }
}

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

    async fn get_entry(&self, index: u64) -> Result<Option<Entry>, StorageError> {
        if index == 0 {
            return Ok(None);
        }

        let entries = self.entries.read().await;
        match search_entry(&entries, index) {
            Some(e) => Ok(entries.get(e).cloned()),
            None => Ok(None),
        }
    }

    async fn append_entry(
        &mut self,
        term: u64,
        entry: Arc<Vec<u8>>,
    ) -> Result<Entry, StorageError> {
        let mut entries = self.entries.write().await;
        if let Some(last) = entries.last() {
            if last.term() > term {
                return Err(StorageError::StaleTerm {
                    term: term,
                    latest_term: last.term(),
                });
            }
        }

        let next_index = entries.last().map_or(0, |entry| entry.index()) + 1;
        let entry = Entry::new(next_index, term, entry);
        entries.push(entry.clone());
        Ok(entry)
    }

    async fn append_entries(
        &mut self,
        prev_index: u64,
        prev_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<(), StorageError> {
        let mut entries = self.entries.write().await;

        if prev_index == 0 {
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
                entries.truncate(prev_entry + 1);
                entries.extend(new_entries);
                Ok(())
            }
            None => Err(StorageError::InconsistentPreviousEntry {
                expected_term: prev_term,
                actual_term: None,
            }),
        }
    }
}

fn search_entry(entries: &[Entry], index: u64) -> Option<usize> {
    match entries.binary_search_by(|entry| entry.index().cmp(&index)) {
        Ok(e) => Some(e),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_append_entry() {
        let mut storage = MemoryStorage::new();
        let entry = storage
            .append_entry(2, Arc::new(vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(entry.index(), 1);
        assert_eq!(entry.term(), 2);
        assert_eq!(entry.data().as_ref(), &[1, 2, 3]);

        let entry = storage
            .append_entry(2, Arc::new(vec![4, 5, 6]))
            .await
            .unwrap();
        assert_eq!(entry.index(), 2);
        assert_eq!(entry.term(), 2);
        assert_eq!(entry.data().as_ref(), &[4, 5, 6]);

        match storage.append_entry(1, Arc::new(vec![1, 2, 3])).await {
            Err(StorageError::StaleTerm { term, latest_term }) => {
                assert_eq!(term, 1);
                assert_eq!(latest_term, 2);
            }
            Err(_) => panic!("unexpected error"),
            Ok(_) => panic!("unexpected success"),
        }
    }

    #[tokio::test]
    async fn test_get_entry() {
        let mut storage = MemoryStorage::new();
        storage
            .append_entry(1, Arc::new(vec![1, 2, 3]))
            .await
            .unwrap();
        storage
            .append_entry(1, Arc::new(vec![4, 5, 6]))
            .await
            .unwrap();

        let entry = storage.get_entry(1).await.unwrap().unwrap();
        assert_eq!(entry.index(), 1);
        assert_eq!(entry.term(), 1);
        assert_eq!(entry.data().as_ref(), &[1, 2, 3]);

        let entry = storage.get_entry(2).await.unwrap().unwrap();
        assert_eq!(entry.index(), 2);
        assert_eq!(entry.term(), 1);
        assert_eq!(entry.data().as_ref(), &[4, 5, 6]);

        assert!(storage.get_entry(3).await.unwrap().is_none());
        assert!(storage.get_entry(0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_append_entries() {
        let mut storage = MemoryStorage::new();
        storage
            .append_entry(1, Arc::new(vec![1, 2, 3]))
            .await
            .unwrap();
        storage
            .append_entry(1, Arc::new(vec![4, 5, 6]))
            .await
            .unwrap();

        let entries = vec![
            Entry::new(3, 2, Arc::new(vec![7, 8, 9])),
            Entry::new(4, 2, Arc::new(vec![10, 11, 12])),
        ];
        storage.append_entries(2, 1, entries.clone()).await.unwrap();

        let entry = storage.get_entry(3).await.unwrap().unwrap();
        assert_eq!(entry, entries[0]);

        let entry = storage.get_entry(4).await.unwrap().unwrap();
        assert_eq!(entry, entries[1]);

        let entry = storage.get_entry(1).await.unwrap().unwrap();
        assert_eq!(entry.index(), 1);
        assert_eq!(entry.term(), 1);
        assert_eq!(entry.data().as_ref(), &[1, 2, 3]);

        let entry = storage.get_entry(2).await.unwrap().unwrap();
        assert_eq!(entry.index(), 2);
        assert_eq!(entry.term(), 1);
        assert_eq!(entry.data().as_ref(), &[4, 5, 6]);

        match storage.append_entries(3, 1, entries.clone()).await {
            Err(StorageError::InconsistentPreviousEntry {
                expected_term,
                actual_term,
            }) => {
                assert_eq!(expected_term, 1);
                assert_eq!(actual_term, Some(2));
            }
            Err(_) => panic!("unexpected error"),
            Ok(_) => panic!("unexpected success"),
        }

        match storage.append_entries(10, 2, entries).await {
            Err(StorageError::InconsistentPreviousEntry {
                expected_term,
                actual_term,
            }) => {
                assert_eq!(expected_term, 2);
                assert_eq!(actual_term, None);
            }
            Err(_) => panic!("unexpected error"),
            Ok(_) => panic!("unexpected success"),
        }
    }
}
