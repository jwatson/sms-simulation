//! Generates random SMS messages.

use async_channel::Sender;
use rand::{distr::SampleString, rngs::StdRng};
use rand_distr::{Alphanumeric, Distribution, Uniform};
use tokio::task::JoinHandle;

/// The maximum length of an SMS message.
const MAX_MSG_LEN: usize = 100;

/// Spawns a new producer task, returning a [`JoinHandle`] for it.
///
/// The producer will generate random SMS messages and send them over
/// `channel_tx`, terminating once `count` messages have been sent.
pub fn spawn_producer(channel_tx: Sender<String>, count: usize) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut producer = Producer::new(channel_tx);
        producer.produce_sms_messages(count).await;
    })
}

/// The producer keeps a strong reference to the `async_channel`'s `Sender`,
/// and to state surrounding the creation of random SMS messages.
struct Producer {
    channel_tx: Sender<String>,
    rng: StdRng,
    length_distr: Uniform<usize>,
}

impl Producer {
    #[must_use]
    fn new(channel_tx: Sender<String>) -> Self {
        let rng = rand::make_rng();

        // This unwrap won't panic because this is a valid interval.
        let length_distr = Uniform::new_inclusive(1, MAX_MSG_LEN).unwrap();

        Self {
            channel_tx,
            rng,
            length_distr,
        }
    }

    /// Generates `count` random strings and sends them over the `async_channel`.
    #[tracing::instrument(skip(self))]
    async fn produce_sms_messages(&mut self, count: usize) {
        for message_id in 0..count {
            // Get a random length from the closed range [1, MAX_MSG_LEN].
            let len = self.length_distr.sample(&mut self.rng);

            // Create a random string sampled from [a-zA-Z0-9].
            let sms = Alphanumeric.sample_string(&mut self.rng, len);

            tracing::trace!(message_id, sms, "produce SMS");

            // This `send` call can't fail because the channel won't close until
            // the queue is empty and the sender is dropped.
            let _ = self.channel_tx.send(sms).await;
        }
    }
}

// Unit tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::check;
    use tokio::time::timeout;

    use super::*;

    /// Tests that the producer blocks when the queue is full.
    #[tokio::test]
    async fn blocks_when_queue_is_full() {
        // Create a queue with 1 slot, and a producer of 2 messages.
        let (tx, _rx) = async_channel::bounded(1);
        let handle = spawn_producer(tx, 2);

        // The join should wait forever, since the producer is blocked on a
        // full queue.
        check!(timeout(Duration::from_millis(10), handle).await.is_err());
    }

    /// Tests that the producer will resume when there is space in the channel.
    #[tokio::test]
    async fn completes_when_queue_is_read() {
        // Create a queue with 1 slot, and a producer of 2 messages.
        let (tx, rx) = async_channel::bounded(1);
        let handle = spawn_producer(tx, 2);

        // Consume 1 message from the queue, which should unblock the producer.
        check!(rx.recv().await.is_ok());

        // The producer task will have completed so the future should resolve
        // immediately.
        check!(handle.await.is_ok());
    }

    /// Tests that multiple consumers don't see the same value.
    #[tokio::test]
    async fn consumers_see_unique_values() {
        let (tx, rx1) = async_channel::bounded(2);
        let rx2 = rx1.clone();

        // Spawn the producer; it should produce 2 values and end immediately.
        let handle = spawn_producer(tx, 2);
        check!(handle.await.is_ok());

        // A `recv()` from two consumers should yield two unique strings.
        let msg1 = rx1.recv().await.unwrap();
        let msg2 = rx2.recv().await.unwrap();
        check!(msg1 != msg2);
    }
}
