use crate::metrics::MetricsHandle;

use tokio::time;

pub fn spawn_monitor(metrics: &MetricsHandle) {
    let handle = metrics.clone();

    tokio::spawn(async move {
        let monitor = Monitor::new(handle);
        monitor.monitor_metrics().await;
    });
}

struct Monitor {
    handle: MetricsHandle,
}

impl Monitor {
    #[inline]
    #[must_use]
    fn new(handle: MetricsHandle) -> Self {
        Self { handle }
    }

    async fn monitor_metrics(&self) {
        let mut interval = time::interval(time::Duration::from_secs(2));

        loop {
            interval.tick().await;

            let metrics = self.handle.get_metrics().await;

            println!("PROGRESS MONITOR");
            println!("──────────────────────────");
            println!("• messages sent:    {}", metrics.sent_msgs);
            println!("• messages failed:  {}", metrics.failed_msgs);
            println!("• time per message: {}ms\n", metrics.time_per_msg);
        }
    }
}
