use std::path::PathBuf;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use neoengram_agent::{
    check_health, load_persisted_identity, run, run_with_development_directory_probe, AgentConfig,
    HealthMode, LoggingFormat, PlacementInventoryConfig, SqlitePlacementInventory,
    VolumeIntegrityScanner,
};
use neoengram_domain::protocol::AgentId;
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
    /// Scans the configured Managed Volume CAS and local placement inventory without modifying it.
    IntegrityCheck(IntegrityCheckArgs),
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

#[derive(Debug, Args)]
struct IntegrityCheckArgs {
    #[arg(long)]
    config: PathBuf,
    /// Emits the complete report as JSON instead of the concise operator summary.
    #[arg(long)]
    json: bool,
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
        Command::IntegrityCheck(arguments) => run_integrity_check(arguments)?,
    }
    Ok(())
}

fn run_integrity_check(
    arguments: IntegrityCheckArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = AgentConfig::load(arguments.config)?;
    initialize_logging(&config)?;
    let identity = load_persisted_identity(&config.storage.state_dir)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Agent identity is not initialized",
        )
    })?;
    let approved_agent_id = identity.approved_agent_id.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Agent identity has no approved Agent ID",
        )
    })?;
    let agent_id = AgentId::new(approved_agent_id)?;
    let inventory = Arc::new(SqlitePlacementInventory::open(
        PlacementInventoryConfig::new(
            config.storage.state_dir.clone(),
            agent_id,
            config.tenant_id.clone(),
            config.storage_volume_id.clone(),
        ),
    )?);
    let scanner = VolumeIntegrityScanner::new(
        config.storage.mount_path,
        config.tenant_id,
        config.storage_volume_id,
        inventory,
    );
    let report = scanner.scan()?;
    if arguments.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "Integrity scan: {} scanned, {} verified, {} missing, {} corrupt, {} orphan, {} unknown",
            report.scanned_objects,
            report.verified_objects,
            report.missing.len(),
            report.corrupt.len(),
            report.orphan.len(),
            report.unknown.len(),
        );
        for issue in report
            .missing
            .iter()
            .chain(report.corrupt.iter())
            .chain(report.orphan.iter())
            .chain(report.unknown.iter())
        {
            println!(
                "{:?}: {} ({})",
                issue.kind,
                issue.path.display(),
                issue.detail
            );
        }
    }
    if report.is_healthy() {
        Ok(())
    } else {
        Err("Managed Volume integrity check found issues".into())
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

    #[test]
    fn integrity_check_accepts_json_output() {
        let cli = Cli::try_parse_from([
            "neoengram-agent",
            "integrity-check",
            "--config",
            "/etc/neoengram-agent.yaml",
            "--json",
        ])
        .unwrap();
        let Command::IntegrityCheck(arguments) = cli.command else {
            panic!("expected integrity-check command");
        };
        assert_eq!(arguments.config, PathBuf::from("/etc/neoengram-agent.yaml"));
        assert!(arguments.json);
    }
}
