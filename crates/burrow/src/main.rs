mod client;

use std::path::PathBuf;

use anyhow::Context;
use burrow_proto::ctrl::{CtrlOk, CtrlRequest};
use clap::{Parser, Subcommand};
use client::{call, fmt_bytes, fmt_time};

#[derive(Parser)]
#[command(name = "burrow", version, about = "Distributed backup among friends, over iroh")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create the repo key and a starter config
    Init {
        /// Node name shown to peers (defaults to hostname)
        #[arg(long)]
        name: Option<String>,
    },
    /// Daemon lifecycle
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Show daemon, backup, and (later) peer status
    Status,
    /// Run and inspect backups
    Backup {
        #[command(subcommand)]
        command: BackupCommand,
    },
    /// List snapshots
    Snapshots {
        /// Only this backup id
        backup_id: Option<String>,
    },
    /// Restore a snapshot
    Restore {
        backup_id: String,
        /// Snapshot timestamp (unix seconds, from `burrow snapshots`); latest if omitted
        #[arg(long)]
        snapshot: Option<u64>,
        /// Directory to restore into
        #[arg(long)]
        target: PathBuf,
    },
    /// Repo key operations
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Run the daemon in the foreground (systemd/launchd entry point)
    Run,
}

#[derive(Subcommand)]
enum BackupCommand {
    /// Run a configured backup now
    Run { backup_id: String },
}

#[derive(Subcommand)]
enum KeyCommand {
    /// Print the 24-word recovery phrase for the repo key
    Phrase,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Init { name } => init(name),
        Command::Daemon { command: DaemonCommand::Run } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info,iroh=warn,iroh_blobs=warn".into()),
                )
                .init();
            let config = burrow_daemon::config::Config::load(&burrow_daemon::paths::config_file())?;
            burrow_daemon::daemon::run(config).await
        }
        Command::Status => status().await,
        Command::Backup { command: BackupCommand::Run { backup_id } } => {
            match call(CtrlRequest::BackupRun { backup_id }).await? {
                CtrlOk::BackupDone(s) => {
                    println!(
                        "snapshot {} of {:?}: {} files, {} scanned, {} new",
                        fmt_time(s.created_at),
                        s.backup_id,
                        s.file_count,
                        fmt_bytes(s.bytes_scanned),
                        fmt_bytes(s.bytes_new),
                    );
                    Ok(())
                }
                other => anyhow::bail!("unexpected reply: {other:?}"),
            }
        }
        Command::Snapshots { backup_id } => {
            match call(CtrlRequest::SnapshotList { backup_id }).await? {
                CtrlOk::Snapshots(list) if list.is_empty() => {
                    println!("no snapshots yet — run `burrow backup run <id>`");
                    Ok(())
                }
                CtrlOk::Snapshots(list) => {
                    println!(
                        "{:<12} {:<20} {:<12} {:>8} {:>12} {:>12}",
                        "BACKUP", "CREATED", "TIMESTAMP", "FILES", "SCANNED", "NEW"
                    );
                    for s in list {
                        println!(
                            "{:<12} {:<20} {:<12} {:>8} {:>12} {:>12}",
                            s.backup_id,
                            fmt_time(s.created_at),
                            s.created_at,
                            s.file_count,
                            fmt_bytes(s.bytes_scanned),
                            fmt_bytes(s.bytes_new),
                        );
                    }
                    Ok(())
                }
                other => anyhow::bail!("unexpected reply: {other:?}"),
            }
        }
        Command::Restore { backup_id, snapshot, target } => {
            match call(CtrlRequest::Restore { backup_id, snapshot, target }).await? {
                CtrlOk::RestoreDone { files, bytes, target } => {
                    println!("restored {files} files ({}) to {}", fmt_bytes(bytes), target.display());
                    Ok(())
                }
                other => anyhow::bail!("unexpected reply: {other:?}"),
            }
        }
        Command::Key { command: KeyCommand::Phrase } => {
            let key = burrow_daemon::keys::load(&burrow_daemon::paths::repo_key_file())?;
            print_recovery_phrase(&key);
            Ok(())
        }
    }
}

async fn status() -> anyhow::Result<()> {
    match call(CtrlRequest::Status).await? {
        CtrlOk::Status(s) => {
            println!("burrow {} on {:?}", s.version, s.node_name);
            println!("data: {}", s.data_dir.display());
            if s.backups.is_empty() {
                println!("\nno backups configured — add a [[backup]] block to your config");
                return Ok(());
            }
            println!();
            println!("{:<12} {:<9} {:<10} {:<20} {}", "BACKUP", "REPLICAS", "SNAPSHOTS", "LAST RUN", "PATHS");
            for b in &s.backups {
                let last = b
                    .last_snapshot
                    .as_ref()
                    .map(|s| fmt_time(s.created_at))
                    .unwrap_or_else(|| "never".into());
                let paths: Vec<String> =
                    b.paths.iter().map(|p| p.display().to_string()).collect();
                println!(
                    "{:<12} {:<9} {:<10} {:<20} {}",
                    b.backup_id,
                    b.replicas,
                    b.snapshot_count,
                    last,
                    paths.join(", ")
                );
            }
            Ok(())
        }
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

fn init(name: Option<String>) -> anyhow::Result<()> {
    let config_dir = burrow_daemon::paths::config_dir();
    std::fs::create_dir_all(&config_dir)?;

    let key_path = burrow_daemon::paths::repo_key_file();
    let key = burrow_daemon::keys::generate_and_save(&key_path)
        .context("a burrow repo may already be initialized here")?;
    println!("repo key written to {}", key_path.display());

    let config_path = burrow_daemon::paths::config_file();
    if !config_path.exists() {
        let node_name = name.unwrap_or_else(|| {
            gethostname_or_default()
        });
        std::fs::write(
            &config_path,
            format!(
                r#"[node]
name = "{node_name}"

# [storage]
# offer_path = "/tank/burrow-held"   # where friends' chunks are stored (M3+)
# offer_max = "500gb"

# [[backup]]
# id = "documents"
# paths = ["{home}/Documents"]
# exclude = ["*.tmp", ".cache/**"]
# replicas = 2
# schedule = "0 3 * * *"
"#,
                home = std::env::var("HOME").unwrap_or_else(|_| "/home/you".into()),
            ),
        )?;
        println!("starter config written to {}", config_path.display());
    } else {
        println!("config already exists at {}, leaving it alone", config_path.display());
    }

    print_recovery_phrase(&key);
    Ok(())
}

fn print_recovery_phrase(key: &burrow_core::RepoKey) {
    let phrase = key.to_recovery_phrase();
    let words: Vec<&str> = phrase.split_whitespace().collect();
    println!();
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│  RECOVERY PHRASE — write this down and store it OFF this     │");
    println!("│  machine. Anyone with it can read your backups; without it,  │");
    println!("│  a lost disk means your backups are gone forever.            │");
    println!("├──────────────────────────────────────────────────────────────┤");
    for row in words.chunks(4) {
        let cells: Vec<String> = row.iter().map(|w| format!("{w:<14}")).collect();
        println!("│  {:<60}│", cells.join("").trim_end());
    }
    println!("└──────────────────────────────────────────────────────────────┘");
}

fn gethostname_or_default() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().trim_end_matches(".local").to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "burrow-node".into())
}
