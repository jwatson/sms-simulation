//! Tracks system-wide delivery metrics.

use tokio::sync::{mpsc, oneshot};

/// Spawns a task that manages shared access to system-wide metrics.
///
/// The `cap`acity parameter controls the depth of the bounded MPSC channel
/// used to communicate between tasks.
///
/// The returned [`MetricsHandle`] can be used to update/retrieve metrics.
pub fn spawn_metrics(cap: usize) -> MetricsHandle {
    let (tx, rx) = mpsc::channel(cap);

    tokio::spawn(async move {
        let mut tracker = MetricsTracker::new(rx);
        tracker.collect_metrics().await;
    });

    MetricsHandle { msg_tx: tx }
}

/// The set of metrics being tracked.
#[derive(Clone, Copy)]
pub struct Metrics {
    /// The number of messages successfully sent.
    pub sent_msgs: u64,
    /// The number of messages that failed to send.
    pub failed_msgs: u64,
    /// The average time spent sending a message.
    pub time_per_msg: u64,
}

/// A handle used to communicate with the metrics task.
#[derive(Clone)]
pub struct MetricsHandle {
    msg_tx: mpsc::Sender<MetricsMessage>,
}

impl MetricsHandle {
    /// Increments the `sent_msgs` metric.
    ///
    /// The `time` parameter is the time it took to send the message. This is
    /// used to calculate the average time per message.
    #[tracing::instrument(skip(self))]
    pub async fn inc_message_sent(&self, time: u64) {
        let res = self.msg_tx.send(MetricsMessage::IncSent { time }).await;
        if res.is_err() {
            tracing::error!("metrics receiver is closed");
        }
    }

    /// Increments the `failed_msgs` metric.
    #[tracing::instrument(skip(self))]
    pub async fn inc_message_failed(&self) {
        let res = self.msg_tx.send(MetricsMessage::IncFailed).await;
        if res.is_err() {
            tracing::error!("metrics receiver is closed");
        }
    }

    /// Returns the latest `Metrics`.
    #[tracing::instrument(skip(self))]
    pub async fn get_metrics(&self) -> Metrics {
        let (tx, rx) = oneshot::channel();
        let msg = MetricsMessage::GetMetrics { respond_to: tx };

        let _ = self.msg_tx.send(msg).await;
        rx.await.unwrap()
    }
}

/// Messages passed between client tasks and the worker.
enum MetricsMessage {
    /// Increment the sent metric, and add `time` to the rolling sum.
    IncSent { time: u64 },
    /// Increment the failed metric.
    IncFailed,
    /// Return the latest metrics on the `respond_to` oneshot channel.
    GetMetrics {
        respond_to: oneshot::Sender<Metrics>,
    },
}

/// State managed by the tracker task.
struct MetricsTracker {
    msg_rx: mpsc::Receiver<MetricsMessage>,
    total_time: u64,
    metrics: Metrics,
}

impl MetricsTracker {
    #[inline]
    #[must_use]
    fn new(msg_rx: mpsc::Receiver<MetricsMessage>) -> Self {
        Self {
            msg_rx,
            total_time: 0,
            metrics: Metrics {
                sent_msgs: 0,
                failed_msgs: 0,
                time_per_msg: 0,
            },
        }
    }

    /// Handles request/response to client tasks.
    #[tracing::instrument(skip(self))]
    async fn collect_metrics(&mut self) {
        while let Some(msg) = self.msg_rx.recv().await {
            match msg {
                MetricsMessage::IncSent { time } => {
                    self.metrics.sent_msgs += 1;
                    self.total_time += time;
                    self.metrics.time_per_msg = self.total_time / self.metrics.sent_msgs;
                }
                MetricsMessage::IncFailed => {
                    self.metrics.failed_msgs += 1;
                }
                MetricsMessage::GetMetrics { respond_to } => {
                    let _ = respond_to.send(self.metrics);
                }
            }
        }
    }
}

// Unit tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Validates the metrics API.
    #[tokio::test]
    async fn validate_api() {
        let handle = spawn_metrics(1);

        handle.inc_message_sent(100).await;
        handle.inc_message_sent(100).await;
        handle.inc_message_failed().await;

        let metrics = handle.get_metrics().await;
        check!(metrics.sent_msgs == 2);
        check!(metrics.failed_msgs == 1);
        check!(metrics.time_per_msg == 100);
    }
}
