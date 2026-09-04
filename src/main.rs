use anyhow::anyhow;
use std::io;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configure_logging()?;

    let stdin = io::stdin();
    loop {
        let mut msg = String::new();
        stdin.read_line(&mut msg)?;
        let msg = msg.trim();
        info!(%msg, "Received message");
        println!("{}", msg);
    }

    Ok(())
}

fn configure_logging() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .without_time()
        .with_target(false)
        .with_writer(io::stderr)
        .try_init()
        .map_err(|e| anyhow!(e))
}
