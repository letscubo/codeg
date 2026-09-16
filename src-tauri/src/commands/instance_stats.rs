//! This container's own version and resource usage, for the instance status panel.
//!
//! ## Why the numbers are read from cgroup, not from `/proc`
//!
//! Inside a container `/proc/meminfo` and `/proc/cpuinfo` describe the **host**:
//! a 4 GiB instance on a 32 GiB box reports 32 GiB and every core the machine
//! has. The quota that actually binds lives in the cgroup, so that is what gets
//! read here. This is also why no `sysinfo`-style crate is pulled in — its
//! defaults would report exactly the host figures we must not show.
//!
//! Measured on a live instance (2026-09-09, A1 — 8 cores / 16 GiB / 160 GiB):
//!
//! ```text
//! memory.current  475574272        →  0.44 GiB used
//! memory.max      17179869184      →  16 GiB limit
//! cpu.max         800000 100000    →  8 cores (quota µs per period µs)
//! cpu.stat        usage_usec …     →  cumulative CPU time
//! df -B1 /        171798691840     →  160 GiB (XFS project quota on the overlay)
//! ```
//!
//! cgroup v2 only. v1 lays the same values out under different paths; a v1 host
//! yields `None` for the affected field rather than a wrong number, and the panel
//! renders it as unknown. Every MyClaw host runs v2 today (verified on us1), so
//! adding v1 paths would be untested code guarding a case that does not exist.
//!
//! ## Reading is deliberately failure-tolerant
//!
//! Every field is optional and read independently. A status panel must not go
//! blank because one file moved — it shows what it has. The one thing it must
//! never do is show a host figure as if it were the container's, which is why an
//! unreadable file yields `None` and never a fallback to `/proc`.

use std::time::Duration;

use serde::Serialize;

const CGROUP: &str = "/sys/fs/cgroup";

/// Gap between the two CPU samples.
///
/// CPU percentage needs a delta: `cpu.stat` is a monotonic counter, so one read
/// tells you nothing. 200 ms is short enough to sit inside a popover's load and
/// long enough that the counter's resolution (microseconds) is not the limiting
/// factor.
const CPU_SAMPLE_GAP: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InstanceStats {
    /// codeg's own version — the panel labels it "MyClaw".
    pub version: String,
    /// Percent of the container's CPU **quota** in use, 0-100. `None` when
    /// unreadable. Normalised against the quota, not the host's core count, so
    /// 100% means "using the whole allowance".
    pub cpu_percent: Option<f64>,
    /// Cores the container is allowed. `None` when the quota is `max`
    /// (unlimited), in which case `cpu_percent` is also `None` — there is no
    /// denominator to be a percentage of.
    pub cpu_cores: Option<f64>,
    pub mem_used_bytes: Option<u64>,
    pub mem_total_bytes: Option<u64>,
    pub disk_used_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

fn read_u64(path: &str) -> Option<u64> {
    read_trimmed(path)?.parse().ok()
}

/// Parse `cpu.max`: `"<quota> <period>"`, where quota is `max` when unlimited.
///
/// Returns the allowance in cores. Exposed for tests — the format is the one
/// thing here that is easy to get subtly wrong.
pub fn parse_cpu_cores(raw: &str) -> Option<f64> {
    let mut parts = raw.split_whitespace();
    let quota = parts.next()?;
    let period: f64 = parts.next()?.parse().ok()?;
    if quota == "max" || period <= 0.0 {
        return None;
    }
    let quota: f64 = quota.parse().ok()?;
    if quota <= 0.0 {
        return None;
    }
    Some(quota / period)
}

/// Pull `usage_usec` out of `cpu.stat` (first line in practice, but matched by
/// key rather than position — the file grows new keys across kernel versions).
pub fn parse_usage_usec(raw: &str) -> Option<u64> {
    raw.lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .and_then(|v| v.trim().parse().ok())
}

/// CPU used over `elapsed`, as a percentage of the container's quota.
///
/// `delta_usec` is CPU-microseconds consumed; the allowance over the same wall
/// window is `cores * elapsed`. Clamped to 100: a burst can briefly exceed the
/// quota's average before the scheduler throttles it, and a bar past the end of
/// its track reads as a bug.
pub fn cpu_percent_from(delta_usec: u64, elapsed: Duration, cores: f64) -> Option<f64> {
    if cores <= 0.0 {
        return None;
    }
    let window_usec = elapsed.as_secs_f64() * 1_000_000.0 * cores;
    if window_usec <= 0.0 {
        return None;
    }
    Some(((delta_usec as f64 / window_usec) * 100.0).clamp(0.0, 100.0))
}

/// `memory.max` is `max` when unlimited; anything else is a byte count.
pub fn parse_mem_max(raw: &str) -> Option<u64> {
    if raw.trim() == "max" {
        return None;
    }
    raw.trim().parse().ok()
}

/// Container filesystem usage for `/`.
///
/// The root overlay carries the instance's XFS project quota, so `statvfs`
/// reports the instance's own allowance rather than the host disk. Uses `df`
/// rather than a libc binding to keep this dependency-free; the numbers are
/// small and this runs on a popover, not a hot path.
fn read_disk() -> (Option<u64>, Option<u64>) {
    let out = std::process::Command::new("df").args(["-B1", "/"]).output().ok();
    let Some(out) = out.filter(|o| o.status.success()) else {
        return (None, None);
    };
    let text = String::from_utf8_lossy(&out.stdout);
    parse_df(&text)
}

/// Parse `df -B1 /` output: header line, then `<fs> <total> <used> <avail> …`.
pub fn parse_df(text: &str) -> (Option<u64>, Option<u64>) {
    let Some(line) = text.lines().nth(1) else {
        return (None, None);
    };
    let cols: Vec<&str> = line.split_whitespace().collect();
    if cols.len() < 4 {
        return (None, None);
    }
    (cols[2].parse().ok(), cols[1].parse().ok())
}

/// Collect a snapshot. Takes ~`CPU_SAMPLE_GAP` because of the CPU delta.
pub async fn collect() -> InstanceStats {
    let cores = read_trimmed(&format!("{CGROUP}/cpu.max")).and_then(|s| parse_cpu_cores(&s));

    let cpu_percent = match (cores, read_trimmed(&format!("{CGROUP}/cpu.stat"))) {
        (Some(cores), Some(first)) => {
            let before = parse_usage_usec(&first);
            tokio::time::sleep(CPU_SAMPLE_GAP).await;
            let after = read_trimmed(&format!("{CGROUP}/cpu.stat")).and_then(|s| parse_usage_usec(&s));
            match (before, after) {
                // saturating_sub: the counter only goes up, but a cgroup swap
                // under us would otherwise underflow into a huge bogus number.
                (Some(a), Some(b)) => cpu_percent_from(b.saturating_sub(a), CPU_SAMPLE_GAP, cores),
                _ => None,
            }
        }
        _ => None,
    };

    let (disk_used_bytes, disk_total_bytes) = read_disk();

    InstanceStats {
        version: env!("CARGO_PKG_VERSION").to_string(),
        cpu_percent,
        cpu_cores: cores,
        mem_used_bytes: read_u64(&format!("{CGROUP}/memory.current")),
        mem_total_bytes: read_trimmed(&format!("{CGROUP}/memory.max")).and_then(|s| parse_mem_max(&s)),
        disk_used_bytes,
        disk_total_bytes,
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_max_parses_to_cores() {
        // A1's real value: 8 cores.
        assert_eq!(parse_cpu_cores("800000 100000"), Some(8.0));
        assert_eq!(parse_cpu_cores("200000 100000"), Some(2.0));
        // Fractional allowances are legal and must not round to zero.
        assert_eq!(parse_cpu_cores("50000 100000"), Some(0.5));
    }

    #[test]
    fn unlimited_cpu_has_no_core_count() {
        // No quota means no denominator — reporting a percentage would require
        // inventing one out of the host's core count, which is the exact error
        // this module exists to avoid.
        assert_eq!(parse_cpu_cores("max 100000"), None);
    }

    #[test]
    fn malformed_cpu_max_yields_none_rather_than_a_wrong_number() {
        for raw in ["", "800000", "abc 100000", "800000 0", "0 100000", "800000 abc"] {
            assert_eq!(parse_cpu_cores(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn usage_usec_is_matched_by_key_not_position() {
        let stat = "nr_periods 0\nusage_usec 57544191\nuser_usec 51783294";
        assert_eq!(parse_usage_usec(stat), Some(57544191));
        assert_eq!(parse_usage_usec("user_usec 1\nsystem_usec 2"), None);
    }

    #[test]
    fn cpu_percent_is_a_share_of_the_quota_not_of_one_core() {
        // 8 cores fully busy for 1s = 8s of CPU time = 100%.
        let p = cpu_percent_from(8_000_000, Duration::from_secs(1), 8.0).unwrap();
        assert!((p - 100.0).abs() < 0.001, "{p}");
        // One of eight cores busy = 12.5%, not 100%.
        let p = cpu_percent_from(1_000_000, Duration::from_secs(1), 8.0).unwrap();
        assert!((p - 12.5).abs() < 0.001, "{p}");
    }

    #[test]
    fn cpu_percent_clamps_at_100() {
        // A burst can outrun the quota's average before the scheduler throttles
        // it; a bar drawn past its track reads as a bug.
        let p = cpu_percent_from(20_000_000, Duration::from_secs(1), 8.0).unwrap();
        assert_eq!(p, 100.0);
    }

    #[test]
    fn cpu_percent_needs_a_positive_denominator() {
        assert_eq!(cpu_percent_from(1000, Duration::from_secs(1), 0.0), None);
        assert_eq!(cpu_percent_from(1000, Duration::ZERO, 8.0), None);
    }

    #[test]
    fn memory_max_of_max_means_unlimited() {
        assert_eq!(parse_mem_max("17179869184"), Some(17179869184));
        assert_eq!(parse_mem_max("max"), None);
        assert_eq!(parse_mem_max("nonsense"), None);
    }

    #[test]
    fn df_columns_are_used_then_total() {
        // Real output from A1: 160 GiB quota, 1.3 GiB used.
        let text = "Filesystem      1B-blocks       Used   Available Use% Mounted on\n\
                    overlay      171798691840 1399529472 170399162368   1% /";
        assert_eq!(parse_df(text), (Some(1399529472), Some(171798691840)));
    }

    #[test]
    fn df_garbage_yields_none() {
        assert_eq!(parse_df(""), (None, None));
        assert_eq!(parse_df("only a header"), (None, None));
        assert_eq!(parse_df("hdr\noverlay 123"), (None, None));
    }
}
