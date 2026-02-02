//! Notification system for waking consumers when new events arrive

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Strategy for waking consumers when new events arrive
#[derive(Clone)]
pub enum WakeStrategy {
    /// Poll periodically for new events
    Poll { interval: Duration },

    /// Use async notifications (tokio::sync::Notify)
    Notify(Arc<RwLock<HashMap<String, Arc<tokio::sync::Notify>>>>),
}

impl WakeStrategy {
    /// Create a polling wake strategy with the given interval
    pub fn poll(interval: Duration) -> Self {
        Self::Poll { interval }
    }

    /// Create a notification-based wake strategy
    pub fn notify() -> Self {
        Self::Notify(Arc::new(RwLock::new(HashMap::new())))
    }

    /// Wait for new events on a stream
    pub async fn wait(&self, stream: &str) {
        match self {
            WakeStrategy::Poll { interval } => {
                tokio::time::sleep(*interval).await;
            }
            WakeStrategy::Notify(hub) => {
                // Get or create notify for this stream
                let notify = {
                    let mut map = hub.write().unwrap();
                    map.entry(stream.to_string())
                        .or_insert_with(|| Arc::new(tokio::sync::Notify::new()))
                        .clone()
                };
                notify.notified().await;
            }
        }
    }

    /// Notify all consumers waiting on a stream
    ///
    /// This is a no-op for Poll strategy.
    pub fn notify_stream(&self, stream: &str) {
        if let WakeStrategy::Notify(hub) = self {
            let map = hub.read().unwrap();
            if let Some(notify) = map.get(stream) {
                notify.notify_waiters();
            }
        }
    }

    /// Notify all consumers on all streams
    ///
    /// This is a no-op for Poll strategy.
    pub fn notify_all(&self) {
        if let WakeStrategy::Notify(hub) = self {
            let map = hub.read().unwrap();
            for notify in map.values() {
                notify.notify_waiters();
            }
        }
    }
}

impl Default for WakeStrategy {
    fn default() -> Self {
        // Default to polling with 10ms interval
        Self::Poll {
            interval: Duration::from_millis(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_poll_strategy() {
        let strategy = WakeStrategy::poll(Duration::from_millis(1));

        let start = std::time::Instant::now();
        strategy.wait("test").await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(1));
        assert!(elapsed < Duration::from_millis(100)); // Sanity check
    }

    #[tokio::test]
    async fn test_notify_strategy() {
        let strategy = WakeStrategy::notify();

        // Spawn a task that will wait
        let strategy_clone = strategy.clone();
        let handle = tokio::spawn(async move {
            strategy_clone.wait("test").await;
        });

        // Give it a moment to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify
        strategy.notify_stream("test");

        // Task should complete
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("Task should complete")
            .expect("Task should not panic");
    }

    #[tokio::test]
    async fn test_notify_all() {
        let strategy = WakeStrategy::notify();

        // Spawn tasks waiting on different streams
        let strategy1 = strategy.clone();
        let handle1 = tokio::spawn(async move {
            strategy1.wait("stream1").await;
        });

        let strategy2 = strategy.clone();
        let handle2 = tokio::spawn(async move {
            strategy2.wait("stream2").await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Notify all
        strategy.notify_all();

        // Both should complete
        tokio::time::timeout(Duration::from_millis(100), handle1)
            .await
            .expect("Task 1 should complete")
            .expect("Task 1 should not panic");

        tokio::time::timeout(Duration::from_millis(100), handle2)
            .await
            .expect("Task 2 should complete")
            .expect("Task 2 should not panic");
    }
}
