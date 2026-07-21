//! Declarative daemon configuration (TOML). The daemon treats this file as
//! the desired state and reconciles to it; peers/grants are runtime state in
//! SQLite, not config.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub device: DeviceConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub repair: RepairConfig,
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default, rename = "backup")]
    pub backups: Vec<BackupConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DeviceConfig {
    /// "host" stores data for yourself and friends; "client" only backs up
    /// (laptops). Client devices refuse inbound grants and store requests.
    pub mode: DeviceMode,
    /// When false, scheduled backups and replication defer while on battery.
    pub run_on_battery: bool,
}

impl Default for DeviceConfig {
    fn default() -> Self {
        Self {
            mode: DeviceMode::Host,
            run_on_battery: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeviceMode {
    Host,
    Client,
}

impl DeviceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            DeviceMode::Host => "host",
            DeviceMode::Client => "client",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RepairConfig {
    /// Peer unseen this long stops counting as a replica holder.
    pub grace_period: String,
    /// How long an owner gets to evacuate after a grant shrinks below usage.
    pub evac_window: String,
    /// How often to spot-check that peers really hold our blobs.
    pub verify_interval: String,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            grace_period: "72h".into(),
            evac_window: "14d".into(),
            verify_interval: "1h".into(),
        }
    }
}

impl RepairConfig {
    pub fn grace_period_secs(&self) -> u64 {
        parse_duration(&self.grace_period).expect("validated at load")
    }
    pub fn evac_window_secs(&self) -> u64 {
        parse_duration(&self.evac_window).expect("validated at load")
    }
    pub fn verify_interval_secs(&self) -> u64 {
        parse_duration(&self.verify_interval).expect("validated at load")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct WebConfig {
    /// Serve the optional web UI + JSON API. Off by default; the core daemon
    /// never depends on it.
    pub enable: bool,
    /// Address to bind, e.g. "127.0.0.1:8385". Loopback clients need no
    /// token; any other bind requires the token in web.token (auto-generated
    /// on first start, printed by `burrow web token`).
    pub bind: String,
    /// Trust loopback clients without a token (same model as the control
    /// socket). Set false when the UI sits behind a reverse proxy: a
    /// same-host proxy makes every remote client look like 127.0.0.1.
    pub trust_loopback: bool,
    /// Extra `Host` names to accept beyond IP literals and localhost, e.g.
    /// "burrow.example.com" when the API sits behind a reverse proxy. The
    /// DNS-rebinding guard rejects requests whose `Host` is a DNS name not
    /// listed here (a rebound attack page always keeps its own domain).
    pub allowed_hosts: Vec<String>,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            enable: false,
            bind: "127.0.0.1:8385".into(),
            trust_loopback: true,
            allowed_hosts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    /// Display name shown to peers; defaults to the hostname.
    pub name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Ceiling across all grants, e.g. "500gb". None = grant-by-grant only.
    pub offer_max: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    /// Stable identifier, e.g. "photos". Renaming orphans old snapshots.
    pub id: String,
    pub paths: Vec<PathBuf>,
    #[serde(default)]
    pub exclude: Vec<String>,
    /// Target number of remote replicas (local copy not counted).
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// Cron expression (5-field crontab); None = manual runs only.
    pub schedule: Option<String>,
    /// Keep only the newest N snapshots; older ones are pruned after each
    /// run and their unique blobs released from peers. None = keep all.
    pub keep_last: Option<u32>,
    /// Copies required on OTHER owners' machines (off-site guarantee),
    /// beyond whatever `replicas` places. 0 = no requirement.
    #[serde(default)]
    pub min_offsite: u32,
}

fn default_replicas() -> u32 {
    2
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut seen = std::collections::HashSet::new();
        for b in &self.backups {
            if b.id.is_empty()
                || !b
                    .id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                bail!("backup id {:?} must be non-empty [a-zA-Z0-9_-]", b.id);
            }
            if !seen.insert(&b.id) {
                bail!("duplicate backup id {:?}", b.id);
            }
            if b.paths.is_empty() {
                bail!("backup {:?} has no paths", b.id);
            }
        }
        if let Some(max) = &self.storage.offer_max {
            parse_size(max).with_context(|| format!("storage.offer_max {max:?}"))?;
        }
        self.web
            .bind
            .parse::<std::net::SocketAddr>()
            .with_context(|| {
                format!(
                    "web.bind {:?} (want addr:port, e.g. 127.0.0.1:8385)",
                    self.web.bind
                )
            })?;
        for (label, value) in [
            ("repair.grace_period", &self.repair.grace_period),
            ("repair.evac_window", &self.repair.evac_window),
            ("repair.verify_interval", &self.repair.verify_interval),
        ] {
            parse_duration(value).with_context(|| format!("{label} {value:?}"))?;
        }
        for b in &self.backups {
            if let Some(s) = &b.schedule {
                crate::scheduler::parse_cron(s)
                    .with_context(|| format!("backup {:?} schedule {s:?}", b.id))?;
            }
        }
        Ok(())
    }

    pub fn node_name(&self) -> String {
        self.node
            .name
            .clone()
            .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned())
    }

    pub fn backup(&self, id: &str) -> Option<&BackupConfig> {
        self.backups.iter().find(|b| b.id == id)
    }
}

/// Parse human sizes: "500gb", "1.5tb", "100 MiB", plain bytes. Decimal units
/// (kb/mb/gb/tb) are powers of 1000; binary (kib/mib/gib/tib) powers of 1024.
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_ascii_lowercase().replace(' ', "");
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: f64 = num
        .parse()
        .with_context(|| format!("bad size number in {s:?}"))?;
    let mult: u64 = match unit {
        "" | "b" => 1,
        "kb" => 1000,
        "mb" => 1000_u64.pow(2),
        "gb" => 1000_u64.pow(3),
        "tb" => 1000_u64.pow(4),
        "kib" => 1 << 10,
        "mib" => 1 << 20,
        "gib" => 1 << 30,
        "tib" => 1u64 << 40,
        other => bail!("unknown size unit {other:?}"),
    };
    if value < 0.0 || !value.is_finite() {
        bail!("bad size {s:?}");
    }
    Ok((value * mult as f64) as u64)
}

/// Parse durations like "30m", "72h", "14d", "90s".
pub fn parse_duration(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_ascii_lowercase();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: u64 = num.parse().with_context(|| format!("bad duration {s:?}"))?;
    let mult = match unit {
        "s" | "" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        other => bail!("unknown duration unit {other:?} (use s/m/h/d)"),
    };
    Ok(value * mult)
}

/// Human-readable decimal size, e.g. 1_500_000_000 -> "1.5 GB".
pub fn fmt_size(n: u64) -> String {
    const UNITS: [(&str, u64); 4] = [
        ("TB", 1_000_000_000_000),
        ("GB", 1_000_000_000),
        ("MB", 1_000_000),
        ("KB", 1_000),
    ];
    for (unit, mult) in UNITS {
        if n >= mult {
            let v = n as f64 / mult as f64;
            return if v >= 100.0 {
                format!("{v:.0} {unit}")
            } else {
                format!("{v:.1} {unit}")
            };
        }
    }
    format!("{n} B")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sizes() {
        assert_eq!(parse_size("500gb").unwrap(), 500_000_000_000);
        assert_eq!(parse_size("1.5tb").unwrap(), 1_500_000_000_000);
        assert_eq!(parse_size("100 MiB").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("12345").unwrap(), 12345);
        assert!(parse_size("10 parsecs").is_err());
    }

    #[test]
    fn full_config_parses() {
        let cfg: Config = toml::from_str(
            r#"
            [node]
            name = "nas"

            [storage]
            offer_max = "500gb"

            [[backup]]
            id = "photos"
            paths = ["/home/luke/photos"]
            exclude = ["*.tmp", ".cache/**"]
            replicas = 3
            schedule = "0 3 * * *"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.backups[0].replicas, 3);
        assert_eq!(cfg.node_name(), "nas");
    }

    #[test]
    fn rejects_duplicate_ids() {
        let cfg: Config = toml::from_str(
            r#"
            [[backup]]
            id = "a"
            paths = ["/x"]
            [[backup]]
            id = "a"
            paths = ["/y"]
            "#,
        )
        .unwrap();
        assert!(cfg.validate().is_err());
    }
}
