use sms_simulation::{
    metrics::spawn_metrics, monitor::spawn_monitor, producer::spawn_producer, sender::spawn_senders,
};

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

    /// Producer/sender backpressure.
    #[arg(long, require_equals = true, value_name = "NUM", default_value_t = DEFAULT_MSG_QUEUE_DEPTH)]
    message_queue_depth: usize,
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error"));

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json().with_current_span(true));

    #[cfg(feature = "console")]
    let registry = registry.with(console_subscriber::spawn());

    registry.init();
}

#[tracing::instrument]
async fn run_simulation(args: &Args) -> Result<(), Report> {
    // Spawn a task that senders can communicate with to track SMS simulation
    // metrics globally.
    let metrics_handle = spawn_metrics();

    // Spawn a task that periodically prints the current set of metrics.
    spawn_monitor(&metrics_handle);

    // `async_channel` is mpmc, but unlike Tokio's `broadcast` channel only
    // one consumer will see any given message.
    let (msg_send, msg_recv) = async_channel::bounded(args.message_queue_depth);

    let producer = spawn_producer(msg_send, args.num_messages);
    let senders = spawn_senders(
        msg_recv,
        &metrics_handle,
        args.num_senders,
        args.send_duration,
    );

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

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "service stopping");
    Ok(())
}
