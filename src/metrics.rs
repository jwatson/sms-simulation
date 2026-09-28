use tokio::sync::{mpsc, oneshot};

pub fn spawn_metrics() -> MetricsHandle {
    let (tx, rx) = mpsc::channel(32);

    tokio::spawn(async move {
        let mut tracker = MetricsTracker::new(rx);
        tracker.collect_metrics().await;
    });

    MetricsHandle { msg_tx: tx }
}

#[derive(Clone, Copy)]
pub struct Metrics {
    pub sent_msgs: u64,
    pub failed_msgs: u64,
    pub time_per_msg: u64,
}

#[derive(Clone)]
pub struct MetricsHandle {
    msg_tx: mpsc::Sender<MetricsMessage>,
}

impl MetricsHandle {
    #[tracing::instrument(skip(self))]
    pub async fn inc_message_sent(&self, time: u64) {
        let res = self.msg_tx.send(MetricsMessage::IncSent { time }).await;
        if res.is_err() {
            tracing::error!("metrics receiver is closed");
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn inc_message_failed(&self) {
        let res = self.msg_tx.send(MetricsMessage::IncFailed).await;
        if res.is_err() {
            tracing::error!("metrics receiver is closed");
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_metrics(&self) -> Metrics {
        let (tx, rx) = oneshot::channel();
        let msg = MetricsMessage::GetMetrics { respond_to: tx };

        let _ = self.msg_tx.send(msg).await;
        rx.await.unwrap()
    }
}

enum MetricsMessage {
    IncSent {
        time: u64,
    },
    IncFailed,
    GetMetrics {
        respond_to: oneshot::Sender<Metrics>,
    },
}

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
