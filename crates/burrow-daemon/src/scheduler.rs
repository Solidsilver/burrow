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
        let started_at = chrono::Utc::now();
        // Failed/attempted runs, so an erroring backup isn't retried every
        // 30s but waits for its next cron slot.
        let mut last_attempt: std::collections::HashMap<String, chrono::DateTime<chrono::Utc>> =
            std::collections::HashMap::new();
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let Some(state) = state.upgrade() else { break };
            if state.is_paused() {
                continue; // missed runs fire after resume
            }
            if crate::sys::on_battery() && !state.config.device.run_on_battery {
                continue; // missed runs fire once back on AC, anacron-style
            }
            let now = chrono::Utc::now();
            for b in &state.config.backups {
                let Some(expr) = &b.schedule else { continue };
                let Ok(schedule) = parse_cron(expr) else { continue }; // validated at load
                // Baseline on the last recorded snapshot, not daemon uptime:
                // a laptop asleep over the 03:00 slot then catches up at next
                // wake instead of silently skipping the day. Backups that
                // never ran wait for their first regular slot.
                let last_run = latest_run(&state, &b.id)
                    .await
                    .and_then(|ts| chrono::DateTime::from_timestamp(ts as i64, 0));
                let mut base = last_run.unwrap_or(started_at);
                if let Some(attempt) = last_attempt.get(&b.id) {
                    base = base.max(*attempt);
                }
                let due = schedule.after(&base).next().map(|t| t <= now).unwrap_or(false);
                if due {
                    last_attempt.insert(b.id.clone(), now);
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
        }
    });
}

/// Unix time of the newest snapshot of a backup (successful runs only).
async fn latest_run(state: &std::sync::Arc<AppState>, backup_id: &str) -> Option<u64> {
    let id = backup_id.to_string();
    state
        .db
        .call(move |conn| {
            Ok(conn
                .query_row(
                    "SELECT MAX(created_at) FROM snapshots WHERE backup_id = ?1",
                    [&id],
                    |r| r.get::<_, Option<u64>>(0),
                )
                .ok()
                .flatten())
        })
        .await
        .ok()
        .flatten()
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
