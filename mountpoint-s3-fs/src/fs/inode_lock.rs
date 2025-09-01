use std::collections::HashMap;
use crate::sync::{AsyncRwLock, AsyncMutex, Arc};
use crate::fs::InodeNo;
use event_listener::Event;
use crate::superblock::WriteStatus;

pub struct InodeLock {
    locks: AsyncMutex<HashMap<InodeNo, Arc<AsyncRwLock<WriteStatus>>>>,
    events: AsyncMutex<HashMap<InodeNo, Arc<Event>>>,
}

impl InodeLock {
    pub fn new() -> Self {
        Self {
            locks: AsyncMutex::new(HashMap::new()),
            events: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn get_status_lock(&self, ino: InodeNo) -> Arc<AsyncRwLock<WriteStatus>> {
        let mut locks = self.locks.lock().await;
        locks.entry(ino).or_insert_with(|| Arc::new(AsyncRwLock::new(WriteStatus::Remote))).clone()
    }

    pub async fn wait_for_remote(&self, ino: InodeNo) {
        loop {
            let status_lock = self.get_status_lock(ino).await;
            
            let (.., listener) = {
                let status = status_lock.read().await;
                if *status == WriteStatus::Remote {
                    return;
                }
                
                let mut events = self.events.lock().await;
                let event = events.entry(ino).or_insert_with(|| Arc::new(Event::new())).clone();
                (*status, event.listen())
            };

            listener.await;
        }
    }

    pub async fn set_release_in_progress(&self, ino: InodeNo) {
        let status_lock = self.get_status_lock(ino).await;
        
        let mut status = status_lock.write().await;
        *status = WriteStatus::ReleaseInProgress;
    }

    pub async fn set_release_complete(&self, ino: InodeNo) {
        let status_lock = self.get_status_lock(ino).await;
        
        let mut status = status_lock.write().await;
        *status = WriteStatus::Remote;

        let event = {
            let events = self.events.lock().await;
            events.get(&ino).cloned()
        };

        if let Some(event) = event {
            event.notify(usize::MAX);
        }
    }
}