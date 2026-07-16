//! Small system probes: disk capacity (hosting limits) and battery state
//! (laptop-aware scheduling).

use std::path::Path;

/// Free bytes on the filesystem containing `path` (None if the probe fails).
pub fn available_disk_bytes(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::statvfs(cpath.as_ptr(), &mut stat) };
        if rc != 0 {
            return None;
        }
        Some(stat.f_bavail as u64 * stat.f_frsize as u64)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Whether the machine is running on battery. `BURROW_FORCE_BATTERY=1|0`
/// overrides (tests). Unknown/desktop → false.
pub fn on_battery() -> bool {
    match std::env::var("BURROW_FORCE_BATTERY").as_deref() {
        Ok("1") => return true,
        Ok("0") => return false,
        _ => {}
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("pmset")
            .args(["-g", "batt"])
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            return text.contains("Battery Power");
        }
        false
    }
    #[cfg(target_os = "linux")]
    {
        // Discharging battery + no AC online => on battery.
        let mut discharging = false;
        if let Ok(entries) = std::fs::read_dir("/sys/class/power_supply") {
            for e in entries.flatten() {
                let p = e.path();
                let kind = std::fs::read_to_string(p.join("type")).unwrap_or_default();
                match kind.trim() {
                    "Mains" => {
                        if std::fs::read_to_string(p.join("online"))
                            .map(|s| s.trim() == "1")
                            .unwrap_or(false)
                        {
                            return false;
                        }
                    }
                    "Battery" => {
                        if std::fs::read_to_string(p.join("status"))
                            .map(|s| s.trim() == "Discharging")
                            .unwrap_or(false)
                        {
                            discharging = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        discharging
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        false
    }
}
