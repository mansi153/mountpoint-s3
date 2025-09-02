use std::collections::HashMap;
use crate::sync::{AsyncRwLock, AsyncMutex, Arc};
use crate::fs::InodeNo;
use event_listener::Event;
use crate::superblock::WriteStatus;
use tracing::{Level, debug, trace};

struct PriorityRwLock {
    lock: AsyncRwLock<WriteStatus>,
    writer_waiting: AsyncMutex<bool>,
    writer_event: Event,
}

pub struct InodeLock {
    locks: AsyncMutex<HashMap<InodeNo, Arc<PriorityRwLock>>>,
    events: AsyncMutex<HashMap<InodeNo, Arc<Event>>>,
}

impl InodeLock {
    pub fn new() -> Self {
        Self {
            locks: AsyncMutex::new(HashMap::new()),
            events: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn get_priority_lock(&self, ino: InodeNo) -> Arc<PriorityRwLock> {
        let mut locks = self.locks.lock().await;
        locks.entry(ino).or_insert_with(|| Arc::new(PriorityRwLock {
            lock: AsyncRwLock::new(WriteStatus::Remote),
            writer_waiting: AsyncMutex::new(false),
            writer_event: Event::new(),
        })).clone()
    }

    /// Wait only for ReleaseInProgress to complete - allows LocalOpen to return normal error
    pub async fn wait_for_remote(&self, ino: InodeNo) {
        loop {
            debug!("Waiting for priority lock");
            let priority_lock = self.get_priority_lock(ino).await;

            // Wait if writer is waiting
            loop {
                debug!("Waiting for writer to finish");
                let listener = priority_lock.writer_event.listen();
                let writer_waiting = priority_lock.writer_waiting.lock().await;
                if !*writer_waiting {
                    break;
                }
                drop(writer_waiting);
               listener.await;
            }

            let status = priority_lock.lock.read().await;
            debug!("Got the lock status");
            if *status != WriteStatus::ReleaseInProgress {
                return; // Proceed for Remote AND LocalOpen
            }

            let listener = {
                debug!("Waiting for event lock");
                let events = self.events.lock().await;
                if let Some(event) = events.get(&ino) {
                    Some(event.listen())
                } else {
                    None // No event exists, RELEASE hasn't started yet
                }
            };
            drop(status);
            if let Some(listener) = listener {
                debug!("Listening to event");
                listener.await;
            } else {
                debug!("No event to listen to, yielding");
                // No event exists, just yield and retry
                std::future::ready(()).await;
            }
        }
    }

    /// Set status to ReleaseInProgress when release starts
    pub async fn set_release_in_progress(&self, ino: InodeNo) {
        // Hold HashMap lock during entire reservation process
        let mut locks = self.locks.lock().await;
        let priority_lock = locks.entry(ino).or_insert_with(|| Arc::new(PriorityRwLock {
            lock: AsyncRwLock::new(WriteStatus::Remote),
            writer_waiting: AsyncMutex::new(false),
            writer_event: Event::new(),
        })).clone();

        // Signal writer is waiting while holding HashMap lock
        {
            let mut writer_waiting = priority_lock.writer_waiting.lock().await;
            *writer_waiting = true;
        }
        
        // Get write lock while still holding HashMap lock
        let mut status = priority_lock.lock.write().await;
        
        // Now release HashMap lock - OPEN can proceed but will see writer_waiting=true
        drop(locks);
        
        *status = WriteStatus::ReleaseInProgress;

        // Create the event that OPEN will wait on
        {
            let mut events = self.events.lock().await;
            events.entry(ino).or_insert_with(|| Arc::new(Event::new()));
        }

        // Clear writer waiting flag
        {
            let mut writer_waiting = priority_lock.writer_waiting.lock().await;
            *writer_waiting = false;
        }
        priority_lock.writer_event.notify(usize::MAX);
    }

    /// Set status to Remote when release completes and notify waiters
    pub async fn set_release_complete(&self, ino: InodeNo) {
        let priority_lock = self.get_priority_lock(ino).await;

        let mut status = priority_lock.lock.write().await;
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
