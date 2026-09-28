use sms_simulation::{producer::spawn_producer, sender::spawn_senders};

use clap::Parser;
use color_eyre::Report;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

const DEFAULT_NUM_SENDERS: usize = 32;
const DEFAULT_NUM_MESSAGES: usize = 1000;
const DEFAULT_SEND_DURATION: u64 = 250;
const DEFAULT_FAILURE_RATE: f64 = 0.05;
const DEFAULT_MSG_QUEUE_DEPTH: usize = 100;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None, next_line_help = true)]
struct Args {
    /// Number of concurrent senders.
    #[arg(short = 's', long, require_equals = true, value_name = "NUM", default_value_t = DEFAULT_NUM_SENDERS)]
    num_senders: usize,

    /// Number of messages to produce.
    #[arg(short = 'm', long, require_equals = true, value_name = "NUM", default_value_t = DEFAULT_NUM_MESSAGES)]
    num_messages: usize,

    /// Mean send duration, in milliseconds.
    #[arg(short = 't', long, require_equals = true, value_name = "MS", default_value_t = DEFAULT_SEND_DURATION)]
    send_duration: u64,

    /// How often a failure occurs.
    #[arg(short, long, require_equals = true, value_name = "PERCENT", default_value_t = DEFAULT_FAILURE_RATE)]
    failure_rate: f64,

    /// Producer/consumer backpressure.
    #[arg(long, require_equals = true, value_name = "NUM", default_value_t = DEFAULT_MSG_QUEUE_DEPTH)]
    message_queue_depth: usize,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json().with_current_span(true));

    #[cfg(feature = "console")]
    let registry = registry.with(console_subscriber::spawn());

    registry.init();
}

#[tracing::instrument]
async fn run_simulation(args: &Args) -> Result<(), Report> {
    let (msg_send, msg_recv) = async_channel::bounded(10);
    let producer = spawn_producer(msg_send, args.num_messages);
    let senders = spawn_senders(msg_recv, args.num_senders, args.send_duration);

    producer.await?;
    senders.join_all().await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;

    let args = Args::parse();

    init_tracing();
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "service starting");

    run_simulation(&args).await?;

    Ok(())
}
