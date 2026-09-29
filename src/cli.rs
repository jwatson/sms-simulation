use std::ops::RangeInclusive;

use clap::Parser;

const DEFAULT_NUM_SENDERS: usize = 32;
const DEFAULT_NUM_MESSAGES: usize = 1000;
const DEFAULT_SEND_DURATION: u64 = 250;
const DEFAULT_FAILURE_RATE: f64 = 0.05;
const DEFAULT_MONITOR_UPDATE: u64 = 2;
const DEFAULT_MSG_QUEUE_DEPTH: usize = 64;

const MONITOR_RANGE: RangeInclusive<u64> = 1..=3600;
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

/// Command line arguments.
#[derive(Debug, Parser)]
#[command(version, about, long_about = None, next_line_help = true)]
pub struct Args {
    /// Number of concurrent senders.
    #[arg(short = 's', long, value_name = "NUM", default_value_t = DEFAULT_NUM_SENDERS)]
    pub num_senders: usize,

    /// Number of messages to produce.
    #[arg(short = 'm', long, value_name = "NUM", default_value_t = DEFAULT_NUM_MESSAGES)]
    pub num_messages: usize,

    /// Mean send duration, in milliseconds.
    #[arg(short = 't', long, value_name = "MS", default_value_t = DEFAULT_SEND_DURATION)]
    pub send_duration: u64,

    /// How often a failure occurs.
    #[arg(short, long, value_name = "PERCENT", default_value_t = DEFAULT_FAILURE_RATE)]
    #[arg(value_parser=failure_rate_in_range)]
    pub failure_rate: f64,

    /// Rate to update the progress monitor at, in seconds.
    #[arg(short = 'u', long, value_name = "SECONDS", default_value_t = DEFAULT_MONITOR_UPDATE)]
    #[arg(value_parser=clap::value_parser!(u64).range(MONITOR_RANGE))]
    pub monitor_update: u64,

    /// Producer/sender backpressure.
    #[arg(long, value_name = "NUM", default_value_t = DEFAULT_MSG_QUEUE_DEPTH)]
    pub message_queue_depth: usize,
}

// Unit tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn verify_parser() {
        Args::command().debug_assert();
    }
}
