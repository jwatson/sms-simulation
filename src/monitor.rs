use crate::metrics::MetricsHandle;

use tokio::{task::JoinHandle, time};
use tokio_util::sync::CancellationToken;

pub fn spawn_monitor(metrics: &MetricsHandle, period: u64) -> MonitorHandle {
    let cancel_token = CancellationToken::new();
    let token = cancel_token.clone();
    let handle = metrics.clone();

    let join = tokio::spawn(async move {
        let monitor = Monitor::new(handle, token, period);
        monitor.monitor_metrics().await;
    });

    MonitorHandle { join, cancel_token }
}

pub struct MonitorHandle {
    pub join: JoinHandle<()>,
    cancel_token: CancellationToken,
}

impl MonitorHandle {
    pub fn stop(&self) {
        self.cancel_token.cancel();
    }
}

struct Monitor {
    handle: MetricsHandle,
    cancel_token: CancellationToken,
    period: u64,
}

impl Monitor {
    #[inline]
    #[must_use]
    fn new(handle: MetricsHandle, cancel_token: CancellationToken, period: u64) -> Self {
        Self {
            handle,
            cancel_token,
            period,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn monitor_metrics(&self) {
        let mut interval = time::interval(time::Duration::from_secs(self.period));

        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    self.print_metrics().await;
                    return
                }
                _ = interval.tick() => {
                    self.print_metrics().await
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn print_metrics(&self) {
        let metrics = self.handle.get_metrics().await;
        println!("PROGRESS MONITOR");
        println!("──────────────────────────");
        println!("• messages sent:    {}", metrics.sent_msgs);
        println!("• messages failed:  {}", metrics.failed_msgs);
        println!("• time per message: {}ms\n", metrics.time_per_msg);
    }
}
