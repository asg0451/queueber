use clap::Parser;
use color_eyre::Result;
use queueber::worker::{WorkerConfig, connect_queue_client, run_worker_batch};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "queueber-worker",
    version,
    about = "Run a single worker that polls and processes jobs"
)]
struct Args {
    /// Server address (host:port)
    #[arg(short = 'a', long = "addr", default_value = "127.0.0.1:9090")]
    addr: String,

    /// Items per poll
    #[arg(long = "batch", default_value_t = 16)]
    batch: u32,

    /// Lease validity seconds per poll
    #[arg(long = "lease", default_value_t = 30)]
    lease: u64,

    /// Long poll timeout seconds
    #[arg(long = "timeout", default_value_t = 5)]
    timeout: u64,

    /// Min per-job processing time in ms
    #[arg(long = "min-ms", default_value_t = 10)]
    min_ms: u64,

    /// Max per-job processing time in ms
    #[arg(long = "max-ms", default_value_t = 100)]
    max_ms: u64,

    /// Optional run duration; if 0, runs indefinitely
    #[arg(long = "duration", default_value_t = 0)]
    duration_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();
    let addr = SocketAddr::from_str(&args.addr)?;

    let cfg = WorkerConfig {
        poll_batch_size: args.batch,
        lease_validity_secs: args.lease,
        process_time_min_ms: args.min_ms,
        process_time_max_ms: args.max_ms,
        poll_timeout_secs: args.timeout,
    };

    tokio::task::LocalSet::new()
        .run_until(async move {
            let client = connect_queue_client(addr).await;
            let deadline = if args.duration_secs > 0 {
                Some(Instant::now() + Duration::from_secs(args.duration_secs))
            } else {
                None
            };

            loop {
                if let Some(dl) = deadline && Instant::now() >= dl { break; }
                match run_worker_batch(client.clone(), &cfg).await {
                    Ok(0) => {
                        // No items; backoff briefly to avoid tight loop
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok(_n) => {
                        // processed _n items in this batch; immediately continue
                    }
                    Err(e) => {
                        eprintln!("worker batch error: {e}");
                        // brief backoff on error
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        })
        .await;

    Ok(())
}
