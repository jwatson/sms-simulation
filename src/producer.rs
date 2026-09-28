use async_channel::Sender;
use rand::{distr::SampleString, rngs::StdRng};
use rand_distr::{Alphanumeric, Distribution, Uniform};
use tokio::task::JoinHandle;

const MAX_MSG_LEN: usize = 100;

pub fn spawn_producer(channel_tx: Sender<String>, count: usize) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut producer = Producer::new(channel_tx, count);
        producer.produce_sms_messages().await;
    })
}

struct Producer {
    channel_tx: Sender<String>,
    count: usize,
    rng: StdRng,
    length_distr: Uniform<usize>,
}

impl Producer {
    #[must_use]
    fn new(channel_tx: Sender<String>, count: usize) -> Self {
        let rng = rand::make_rng();

        // This unwrap won't panic because this is a valid interval.
        let length_distr = Uniform::new_inclusive(1, MAX_MSG_LEN).unwrap();

        Self {
            channel_tx,
            count,
            rng,
            length_distr,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn produce_sms_messages(&mut self) {
        for message_id in 0..self.count {
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

#[cfg(test)]
mod tests {}
