use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use neoengram_agentd::{check_health, run, AgentConfig, HealthMode, LoggingFormat};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "neoengram-agent", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Runs the volume-scoped Agent enrollment process.
    Run(RunArgs),
    /// Checks process health using the daemon-owned state directory.
    Health(HealthArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Args)]
struct HealthArgs {
    #[arg(long)]
    state_dir: PathBuf,
    #[arg(long, value_enum)]
    mode: HealthMode,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match Cli::parse().command {
        Command::Run(arguments) => {
            let config = AgentConfig::load(arguments.config)?;
            initialize_logging(&config)?;
            run(config).await?;
        }
        Command::Health(arguments) => check_health(arguments.state_dir, arguments.mode)?,
    }
    Ok(())
}

fn initialize_logging(
    config: &AgentConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = EnvFilter::try_new(&config.logging.level)?;
    match config.logging.format {
        LoggingFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .try_init()?,
        LoggingFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()?,
    }
    Ok(())
}
