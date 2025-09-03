use crate::ll::{INodeNo, RequestId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use futures::lock::Mutex;
use event_listener::Event;

/// Trait for coordinating FUSE requests to prevent race conditions
pub trait RequestCoordinator: Send + Sync {
    /// Wait for any conflicting requests to complete before proceeding
    fn coordinate_request(
        &self,
        ino: INodeNo,
        request_id: RequestId,
        operation: &str,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Simple coordinator that ensures RELEASE completes before OPEN on same inode
#[allow(dead_code)]
pub struct OpenReleaseCoordinator {
    pending_releases: Arc<Mutex<HashMap<u64, Arc<Event>>>>,
}

#[allow(dead_code)]
impl OpenReleaseCoordinator {
    pub fn new() -> Self {
        Self {
            pending_releases: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl RequestCoordinator for OpenReleaseCoordinator {
    fn coordinate_request(&self, ino: INodeNo, _request_id: RequestId, operation: &str) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let pending_releases = self.pending_releases.clone();
        let operation = operation.to_string();
        
        Box::pin(async move {
            match operation.as_str() {
                "RELEASE" => {
                    let event = Arc::new(Event::new());
                    {
                        let mut pending = pending_releases.lock().await;
                        pending.insert(ino.0, event.clone());
                    }
                    event.notify(usize::MAX);
                }
                "OPEN" => {
                    let event_opt = {
                        let pending = pending_releases.lock().await;
                        pending.get(&ino.0).cloned()
                    };
                    if let Some(event) = event_opt {
                        event.listen().await;
                    }
                }
                _ => {}
            }
        })
    }
}

