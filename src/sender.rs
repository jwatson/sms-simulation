use std::time::Duration;

use async_channel::Receiver;
use rand::rngs::StdRng;
use rand_distr::{Bernoulli, Distribution, Triangular};
use tokio::{task::JoinSet, time::sleep};

use crate::metrics::MetricsHandle;

pub fn spawn_senders(
    channel_rx: Receiver<String>,
    metrics: &MetricsHandle,
    count: usize,
    mean: u64,
    fail: f64,
) -> JoinSet<()> {
    let mut set = JoinSet::new();

    for id in 0..count {
        let recv = channel_rx.clone();
        let handle = metrics.clone();

        set.spawn(async move {
            let mut sender = Sender::new(id, recv, handle, mean, fail);
            sender.send_messages().await;
        });
    }

    set
}

struct Sender {
    id: usize,
    channel_rx: Receiver<String>,
    metrics: MetricsHandle,
    rng: StdRng,
    send_distr: Triangular<f64>,
    fail_distr: Bernoulli,
}

impl Sender {
    #[must_use]
    fn new(
        id: usize,
        channel_rx: Receiver<String>,
        metrics: MetricsHandle,
        mean_send_time: u64,
        failure_rate: f64,
    ) -> Self {
        let rng = rand::make_rng();

        let min = (mean_send_time / 2) as f64;
        let max = (mean_send_time * 2) as f64;
        let mode = mean_send_time as f64;
        // This unwrap won't panic because this is a valid interval.
        let send_distr = Triangular::new(min, max, mode).unwrap();

        // This unwrap won't panic because we've already ensured that the
        // failure rate is in the range [0, 1].
        let fail_distr = Bernoulli::new(failure_rate).unwrap();

        Self {
            id,
            channel_rx,
            metrics,
            rng,
            send_distr,
            fail_distr,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn send_messages(&mut self) {
        // Run until the channel is closed. This will happen once the producer
        // has finished and dropped the send side, and all buffered messages
        // have been consumed.
        while let Ok(sms) = self.channel_rx.recv().await {
            tracing::trace!(sender = self.id, sms, "consume SMS");

            let send_time = self.send_distr.sample(&mut self.rng) as u64;

            // Sleep to simulate sending the SMS.
            sleep(Duration::from_millis(send_time)).await;
            tracing::trace!(sender = self.id, send_time, "simulate send SMS");

            if self.fail_distr.sample(&mut self.rng) {
                self.metrics.inc_message_failed().await;
            } else {
                self.metrics.inc_message_sent(send_time).await;
            }
        }
    }
}
