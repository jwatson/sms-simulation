//! Displays periodic system updates.

use tokio::{task::JoinHandle, time};
use tokio_util::sync::CancellationToken;

use crate::metrics::MetricsHandle;

/// Spawns a task that reports the latest system metrics every `period` milliseconds.
///
/// The returned [`MonitorHandle`] can be used to display a final report and
/// gracefully shut down the task.
pub fn spawn_monitor(metrics: &MetricsHandle, period: u64) -> MonitorHandle {
    let cancel_token = CancellationToken::new();
    let token = cancel_token.clone();
    let handle = metrics.clone();

    let join = tokio::spawn(async move {
        let monitor = Monitor::new(handle, token);
        monitor.monitor_metrics(period).await;
    });

    MonitorHandle { join, cancel_token }
}

/// A handle used to communicate with the monitor task.
pub struct MonitorHandle {
    /// The monitor task's join handle.
    pub join: JoinHandle<()>,
    cancel_token: CancellationToken,
}

impl MonitorHandle {
    /// Stops the monitor, which will print the latest metrics before terminating.
    pub fn stop(&self) {
        self.cancel_token.cancel();
    }
}

/// State managed by the monitor task.
struct Monitor {
    handle: MetricsHandle,
    cancel_token: CancellationToken,
}

impl Monitor {
    #[inline]
    #[must_use]
    fn new(handle: MetricsHandle, cancel_token: CancellationToken) -> Self {
        Self {
            handle,
            cancel_token,
        }
    }

    /// The monitor task's main loop.
    #[tracing::instrument(skip(self))]
    async fn monitor_metrics(&self, period: u64) {
        let mut interval = time::interval(time::Duration::from_secs(period));

        loop {
            // Wait for either the cancellation or interval futures to resolve.
            // The polling order is biased toward cancellation since it is
            // client-driven and should happen ASAP.
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

    /// Gets and prints the latest metrics from the worker task.
    ///
    /// An argument can be made that `println` is blocking I/O and should be
    /// run on the blocking thread pool, but I think we'll be fine.
    #[tracing::instrument(skip(self))]
    async fn print_metrics(&self) {
        let metrics = self.handle.get_metrics().await;
        println!(
            "\nSIMULATION PROGRESS\n\
            ──────────────────────────\n\
            • messages sent: {}\n\
            • messages failed: {}\n\
            • time per message: {}ms",
            metrics.sent_msgs, metrics.failed_msgs, metrics.time_per_msg
        );
    }
}
