use clap::Parser;
use color_eyre::Report;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use sms_simulation::{
    cli::Args, metrics::spawn_metrics, monitor::spawn_monitor, producer::spawn_producer,
    sender::spawn_senders,
};

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
    let metrics_handle = spawn_metrics(args.num_senders / 2);

    // Spawn a task that periodically prints the current set of metrics.
    let monitor_handle = spawn_monitor(&metrics_handle, args.monitor_update);

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

    // The `stop()` method tells the monitor to print the metrics one last time
    // before terminating. We await the monitor's join handle to ensure that it
    // has time to run.
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
