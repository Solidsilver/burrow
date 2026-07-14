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
    pub storage: StorageConfig,
    #[serde(default, rename = "backup")]
    pub backups: Vec<BackupConfig>,
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
    /// Where chunks held for friends are stored (M3+). Defaults to the data dir.
    pub offer_path: Option<PathBuf>,
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
    /// Cron expression; None = manual runs only.
    pub schedule: Option<String>,
}

fn default_replicas() -> u32 {
    2
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut seen = std::collections::HashSet::new();
        for b in &self.backups {
            if b.id.is_empty() || !b.id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
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
        Ok(())
    }

    pub fn node_name(&self) -> String {
        self.node.name.clone().unwrap_or_else(|| {
            gethostname::gethostname().to_string_lossy().into_owned()
        })
    }

    pub fn backup(&self, id: &str) -> Option<&BackupConfig> {
        self.backups.iter().find(|b| b.id == id)
    }
}

/// Parse human sizes: "500gb", "1.5tb", "100 MiB", plain bytes. Decimal units
/// (kb/mb/gb/tb) are powers of 1000; binary (kib/mib/gib/tib) powers of 1024.
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim().to_ascii_lowercase().replace(' ', "");
    let split = s.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let value: f64 = num.parse().with_context(|| format!("bad size number in {s:?}"))?;
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

/// Human-readable decimal size, e.g. 1_500_000_000 -> "1.5 GB".
pub fn fmt_size(n: u64) -> String {
    const UNITS: [(&str, u64); 4] =
        [("TB", 1_000_000_000_000), ("GB", 1_000_000_000), ("MB", 1_000_000), ("KB", 1_000)];
    for (unit, mult) in UNITS {
        if n >= mult {
            let v = n as f64 / mult as f64;
            return if v >= 100.0 { format!("{v:.0} {unit}") } else { format!("{v:.1} {unit}") };
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
            offer_path = "/tank/burrow-held"
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
