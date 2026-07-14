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
    /// Manage peerings with friends
    Peer {
        #[command(subcommand)]
        command: PeerCommand,
    },
    /// List peers with grants and liveness
    Peers,
    /// Show pending peerings and space requests
    Requests,
    /// Approve a pending peer
    Approve { name: String },
    /// Deny a pending peer or clear their space request
    Deny { name: String },
    /// Reserve space for a peer, e.g. `burrow grant anna 200gb` (0 revokes)
    Grant { name: String, size: String },
    /// Ask a peer to reserve space for us, e.g. `burrow request anna 100gb`
    Request { name: String, size: String },
}

#[derive(Subcommand)]
enum PeerCommand {
    /// Print this node's pairing ticket (send it to a friend)
    Invite,
    /// Add a friend from their pairing ticket
    Add {
        ticket: String,
        /// Local nickname for this peer
        #[arg(long)]
        name: String,
    },
    /// Remove a peer entirely
    Remove { name: String },
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
        Command::Peer { command } => match command {
            PeerCommand::Invite => match call(CtrlRequest::PeerInvite).await? {
                CtrlOk::Ticket(t) => {
                    println!("send this ticket to your friend; they run:\n");
                    println!("  burrow peer add {t} --name <your-nickname>\n");
                    Ok(())
                }
                other => anyhow::bail!("unexpected reply: {other:?}"),
            },
            PeerCommand::Add { ticket, name } => {
                done(call(CtrlRequest::PeerAdd { ticket, name }).await?)
            }
            PeerCommand::Remove { name } => done(call(CtrlRequest::PeerRemove { name }).await?),
        },
        Command::Peers => peers_table().await,
        Command::Requests => requests_table().await,
        Command::Approve { name } => done(call(CtrlRequest::Approve { name }).await?),
        Command::Deny { name } => done(call(CtrlRequest::Deny { name }).await?),
        Command::Grant { name, size } => {
            let bytes = burrow_daemon::config::parse_size(&size)?;
            done(call(CtrlRequest::Grant { name, bytes }).await?)
        }
        Command::Request { name, size } => {
            let bytes = burrow_daemon::config::parse_size(&size)?;
            done(call(CtrlRequest::RequestSpace { name, bytes }).await?)
        }
    }
}

fn done(reply: CtrlOk) -> anyhow::Result<()> {
    match reply {
        CtrlOk::Done(msg) => {
            println!("{msg}");
            Ok(())
        }
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

async fn peers_table() -> anyhow::Result<()> {
    let CtrlOk::Peers(peers) = call(CtrlRequest::PeerList).await? else {
        anyhow::bail!("unexpected reply");
    };
    if peers.is_empty() {
        println!("no peers yet — run `burrow peer invite` and swap tickets with a friend");
        return Ok(());
    }
    let (given, received): (u64, u64) =
        peers.iter().fold((0, 0), |acc, p| (acc.0 + p.given_bytes, acc.1 + p.received_bytes));
    println!(
        "{:<12} {:<10} {:<8} {:<22} {:<22} THEIR NAME",
        "PEER", "STATE", "ONLINE", "YOU GIVE (used)", "YOU GET (used)"
    );
    for p in &peers {
        let online = match p.online {
            Some(true) => "yes",
            Some(false) => "no",
            None => "-",
        };
        let state = if p.state == "active" && p.approved_by_them == Some(false) {
            "await-them".to_string()
        } else {
            p.state.clone()
        };
        println!(
            "{:<12} {:<10} {:<8} {:<22} {:<22} {}",
            p.name,
            state,
            online,
            format!("{} ({})", fmt_bytes(p.given_bytes), fmt_bytes(p.given_used)),
            format!("{} ({})", fmt_bytes(p.received_bytes), fmt_bytes(p.received_used)),
            p.hello_name.as_deref().unwrap_or("-"),
        );
    }
    println!(
        "\nratio: you give {} / you get {}",
        fmt_bytes(given),
        fmt_bytes(received)
    );
    Ok(())
}

async fn requests_table() -> anyhow::Result<()> {
    let CtrlOk::Pending { peers, space_requests } = call(CtrlRequest::PendingList).await? else {
        anyhow::bail!("unexpected reply");
    };
    if peers.is_empty() && space_requests.is_empty() {
        println!("nothing pending");
        return Ok(());
    }
    if !peers.is_empty() {
        println!("pending peerings (approve with `burrow approve <name>`):");
        for p in &peers {
            println!(
                "  {}  (calls itself {:?}, id {})",
                p.name,
                p.hello_name.as_deref().unwrap_or("?"),
                p.endpoint_id.iter().take(4).map(|b| format!("{b:02x}")).collect::<String>(),
            );
        }
    }
    if !space_requests.is_empty() {
        println!("\nspace requests (grant with `burrow grant <name> <size>`, refuse with `burrow deny <name>`):");
        for r in &space_requests {
            println!(
                "  {} asks for {}  (their ratio: gives {} / gets {})",
                r.peer_name,
                fmt_bytes(r.bytes),
                fmt_bytes(r.given_total),
                fmt_bytes(r.received_total),
            );
        }
    }
    Ok(())
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
            println!("{:<12} {:<9} {:<10} {:<20} PATHS", "BACKUP", "REPLICAS", "SNAPSHOTS", "LAST RUN");
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
