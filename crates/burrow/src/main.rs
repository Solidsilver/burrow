use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "burrow", version, about = "Distributed backup among friends, over iroh")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show daemon and backup status (M2)
    Status,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Status => {
            eprintln!("burrow is under construction — the daemon arrives in milestone 2");
            std::process::exit(1);
        }
    }
}
