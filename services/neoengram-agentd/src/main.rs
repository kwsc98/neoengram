use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use neoengram_agentd::{
    check_health, run, run_with_development_directory_probe, AgentConfig, HealthMode, LoggingFormat,
};
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
    /// Treats storage.mount_path as a local directory boundary; loopback endpoints only.
    #[arg(long)]
    development_directory_probe: bool,
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
            if arguments.development_directory_probe {
                run_with_development_directory_probe(config).await?;
            } else {
                run(config).await?;
            }
        }
        Command::Health(arguments) => check_health(arguments.state_dir, arguments.mode)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn development_directory_probe_is_opt_in() {
        let cli =
            Cli::try_parse_from(["neoengram-agent", "run", "--config", "/tmp/agent.yaml"]).unwrap();
        let Command::Run(arguments) = cli.command else {
            panic!("expected run command");
        };
        assert!(!arguments.development_directory_probe);

        let cli = Cli::try_parse_from([
            "neoengram-agent",
            "run",
            "--config",
            "/tmp/agent.yaml",
            "--development-directory-probe",
        ])
        .unwrap();
        let Command::Run(arguments) = cli.command else {
            panic!("expected run command");
        };
        assert!(arguments.development_directory_probe);
    }
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
