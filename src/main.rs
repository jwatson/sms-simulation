use sms_simulation::{
    metrics::spawn_metrics, monitor::spawn_monitor, producer::spawn_producer, sender::spawn_senders,
};

use clap::Parser;
use color_eyre::Report;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use std::ops::RangeInclusive;

const DEFAULT_NUM_SENDERS: usize = 32;
const DEFAULT_NUM_MESSAGES: usize = 1000;
const DEFAULT_SEND_DURATION: u64 = 250;
const DEFAULT_FAILURE_RATE: f64 = 0.05;
const DEFAULT_MSG_QUEUE_DEPTH: usize = 100;

const FAILURE_RANGE: RangeInclusive<f64> = 0.0..=1.0;

fn failure_rate_in_range(s: &str) -> Result<f64, String> {
    let rate: f64 = s.parse().map_err(|_| format!("`{s}` isn't a percentage"))?;
    if FAILURE_RANGE.contains(&rate) {
        Ok(rate)
    } else {
        Err(format!(
            "percentage not in range {}-{}",
            FAILURE_RANGE.start(),
            FAILURE_RANGE.end()
        ))
    }
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None, next_line_help = true)]
struct Args {
    /// Number of concurrent senders.
    #[arg(short = 's', long, value_name = "NUM", default_value_t = DEFAULT_NUM_SENDERS)]
    num_senders: usize,

    /// Number of messages to produce.
    #[arg(short = 'm', long, value_name = "NUM", default_value_t = DEFAULT_NUM_MESSAGES)]
    num_messages: usize,

    /// Mean send duration, in milliseconds.
    #[arg(short = 't', long, value_name = "MS", default_value_t = DEFAULT_SEND_DURATION)]
    send_duration: u64,

    /// How often a failure occurs.
    #[arg(short, long, value_name = "PERCENT", default_value_t = DEFAULT_FAILURE_RATE)]
    #[arg(value_parser=failure_rate_in_range)]
    failure_rate: f64,

    /// Producer/sender backpressure.
    #[arg(long, value_name = "NUM", default_value_t = DEFAULT_MSG_QUEUE_DEPTH)]
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
    let monitor_handle = spawn_monitor(&metrics_handle);

    // `async_channel` is mpmc, but unlike Tokio's `broadcast` channel only
    // one consumer will see any given message.
    let (msg_send, msg_recv) = async_channel::bounded(args.message_queue_depth);

    let producer = spawn_producer(msg_send, args.num_messages);
    let senders = spawn_senders(
        msg_recv,
        &metrics_handle,
        args.num_senders,
        args.send_duration,
        args.failure_rate,
    );

    producer.await?;
    senders.join_all().await;

    monitor_handle.stop();
    monitor_handle.join.await?;

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
