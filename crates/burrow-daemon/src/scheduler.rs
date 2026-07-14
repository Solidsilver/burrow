//! Cron scheduler for configured backups.

use std::str::FromStr;

use crate::daemon::AppState;

/// Parse a 5-field crontab expression ("0 3 * * *"); a 6-field form with
/// seconds is also accepted.
pub fn parse_cron(expr: &str) -> anyhow::Result<cron::Schedule> {
    let fields = expr.split_whitespace().count();
    let normalized = if fields == 5 { format!("0 {expr}") } else { expr.to_string() };
    cron::Schedule::from_str(&normalized)
        .map_err(|e| anyhow::anyhow!("invalid cron expression {expr:?}: {e}"))
}

pub fn spawn_scheduler(state: std::sync::Weak<AppState>) {
    tokio::spawn(async move {
        let mut last_check = chrono::Utc::now();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let Some(state) = state.upgrade() else { break };
            if state.is_paused() {
                continue; // missed runs fire after resume
            }
            if crate::sys::on_battery() && !state.config.device.run_on_battery {
                // Don't advance last_check: missed runs fire once we're back
                // on AC, anacron-style.
                continue;
            }
            let now = chrono::Utc::now();
            for b in &state.config.backups {
                let Some(expr) = &b.schedule else { continue };
                let Ok(schedule) = parse_cron(expr) else { continue }; // validated at load
                let due = schedule.after(&last_check).next().map(|t| t <= now).unwrap_or(false);
                if due {
                    tracing::info!(backup = %b.id, "scheduled backup starting");
                    match crate::ops::backup_run(&state, &b.id).await {
                        Ok(info) => tracing::info!(
                            backup = %b.id,
                            files = info.file_count,
                            new_bytes = info.bytes_new,
                            "scheduled backup done"
                        ),
                        Err(e) => tracing::error!(backup = %b.id, "scheduled backup failed: {e:#}"),
                    }
                }
            }
            last_check = now;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_field_crontab_accepted() {
        parse_cron("0 3 * * *").unwrap();
        parse_cron("*/15 * * * *").unwrap();
        assert!(parse_cron("not a cron").is_err());
    }
}
