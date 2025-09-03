use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use crate::sync::AsyncMutex;
use event_listener::Event;
use fuser::{RequestCoordinator, INodeNo, RequestId};

/// Coordinates OPEN/RELEASE requests to prevent race conditions
pub struct S3RequestCoordinator {
    pending_operations: Arc<AsyncMutex<HashMap<u64, Arc<Event>>>>,
}

impl S3RequestCoordinator {
    pub fn new() -> Self {
        Self {
            pending_operations: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }
}

impl RequestCoordinator for S3RequestCoordinator {
    fn coordinate_request(&self, ino: INodeNo, _request_id: RequestId, operation: &str) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let pending_operations = self.pending_operations.clone();
        let operation = operation.to_string(); // Copy the string to avoid lifetime issues
        
        Box::pin(async move {
            match operation.as_str() {
                "RELEASE" => {
                    // Mark RELEASE as in progress
                    let event = Arc::new(Event::new());
                    {
                        let mut pending = pending_operations.lock().await;
                        pending.insert(ino.0, event.clone());
                    }
                    
                    // Notify when RELEASE completes
                    event.notify(usize::MAX);
                }
                "OPEN" => {
                    // Wait for any pending RELEASE to complete
                    let event_opt = {
                        let pending = pending_operations.lock().await;
                        pending.get(&ino.0).cloned()
                    };
                    
                    if let Some(event) = event_opt {
                        event.listen().await;
                    }
                }
                _ => {} // No coordination for other operations
            }
        })
    }
    

}