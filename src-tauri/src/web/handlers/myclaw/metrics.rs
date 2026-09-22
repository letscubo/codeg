//! `POST /api/myclaw/metrics` — fork(letscubo)专属:实例自己的 CPU / 内存 / 磁盘用量。
//!
//! MyClaw 面板顶栏的负荷环原先读 `vm_idle` —— 那张表由**宿主机 agent** 每 60s 采集
//! docker 容器的 cgroup 写入。kind=all 换成不经宿主机的部署(EKS 集群 / Cube 沙箱)之后
//! 没有任何 agent 采集,面板三项永远是空的(2026-09-23 实测:MyClaw 与 C4 在 vm_idle
//! 里都没有行)。改由页面经 codeg 直接取 —— 谁在跑,谁自己报。
//!
//! ## 取值口径
//!
//! 优先 cgroup(容器自己的限额与用量),读不到才退 `/proc`:docker 容器里 `/proc/meminfo`
//! 看到的是**整台宿主机**的内存,直接用会把 8G 的容器报成 128G。microVM(EKS)与沙箱
//! (Cube)里两者一致,顺序不影响结果。
//!
//! - CPU:cgroup v2 `cpu.stat` 的 `usage_usec` / v1 `cpuacct.usage`(ns),退 `/proc/stat`。
//!   百分比 = 两次采样间的 CPU 时间 ÷ 墙钟时间 × 100,**单核 = 100%**(多核可超 100),
//!   与 `docker stats`、与 agent 写 vm_idle 的口径一致。进程内记住上一次采样:常态是
//!   「距上次轮询(约 60s)的平均值」;没有上次采样时就地采 150ms 补一个,不让首帧空着。
//! - 内存:used = current − inactive_file(去页缓存,同 docker stats);total 取 cgroup
//!   上限,没有上限(`max`)时退 `/proc/meminfo` 的 MemTotal。
//! - 磁盘:根文件系统 `statvfs("/")`。
//!
//! 鉴权与 `/api/myclaw/exec` 同:受保护路由组内,Bearer 或 WS 子协议。前端经 WS
//! `invoke("myclaw/metrics")` 调用(见 web/ws_invoke.rs),不再经平台路由。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub struct MetricsResponse {
    /// 单核 = 100% 口径;取不到时 null。
    pub cpu_percent: Option<f64>,
    pub cpu_cores: Option<usize>,
    pub mem_used_kb: Option<u64>,
    pub mem_total_kb: Option<u64>,
    pub disk_used_bytes: Option<u64>,
    pub disk_total_bytes: Option<u64>,
    /// 采集时刻(RFC3339),前端据此判断新鲜度。
    pub collected_at: String,
}

/// 上一次 CPU 采样:(累计 CPU 时间, 采样时刻)。进程内共享 —— 多个页面轮询时
/// 窗口会被共享,这是**故意**的:同一实例的负荷本来就只有一份。
static LAST_CPU: Mutex<Option<(Duration, Instant)>> = Mutex::new(None);

pub async fn metrics() -> Json<MetricsResponse> {
    let cpu_percent = sample_cpu_percent().await;
    let (mem_used_kb, mem_total_kb) = read_memory();
    let (disk_used_bytes, disk_total_bytes) = read_disk();
    Json(MetricsResponse {
        cpu_percent,
        cpu_cores: std::thread::available_parallelism().ok().map(|n| n.get()),
        mem_used_kb,
        mem_total_kb,
        disk_used_bytes,
        disk_total_bytes,
        collected_at: chrono::Utc::now().to_rfc3339(),
    })
}

/// 与上次采样比,算出这段时间的平均 CPU%。没有上次采样就地补采 150ms。
async fn sample_cpu_percent() -> Option<f64> {
    let now_usage = read_cpu_usage()?;
    let now = Instant::now();
    let previous = {
        let mut last = LAST_CPU.lock().ok()?;
        let prev = *last;
        *last = Some((now_usage, now));
        prev
    };
    if let Some((prev_usage, prev_at)) = previous {
        if let Some(p) = cpu_percent_between(prev_usage, prev_at, now_usage, now) {
            return Some(p);
        }
    }
    // 首次(或时钟/计数器回绕):就地采一小段,别让面板首帧空着
    tokio::time::sleep(Duration::from_millis(150)).await;
    let later_usage = read_cpu_usage()?;
    let later = Instant::now();
    if let Ok(mut last) = LAST_CPU.lock() {
        *last = Some((later_usage, later));
    }
    cpu_percent_between(now_usage, now, later_usage, later)
}

/// 两次采样 → 百分比。计数器回绕 / 零时长返回 None(调用方当作取不到)。
pub fn cpu_percent_between(
    prev_usage: Duration,
    prev_at: Instant,
    now_usage: Duration,
    now_at: Instant,
) -> Option<f64> {
    let elapsed = now_at.checked_duration_since(prev_at)?;
    if elapsed.is_zero() {
        return None;
    }
    let used = now_usage.checked_sub(prev_usage)?;
    Some(used.as_secs_f64() / elapsed.as_secs_f64() * 100.0)
}

/// 容器累计 CPU 时间:cgroup v2 → v1 → /proc/stat。
fn read_cpu_usage() -> Option<Duration> {
    if let Some(usec) = std::fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .ok()
        .and_then(|s| parse_cpu_stat_usec(&s))
    {
        return Some(Duration::from_micros(usec));
    }
    for p in [
        "/sys/fs/cgroup/cpuacct/cpuacct.usage",
        "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage",
    ] {
        if let Some(ns) = std::fs::read_to_string(p).ok().and_then(|s| s.trim().parse::<u64>().ok()) {
            return Some(Duration::from_nanos(ns));
        }
    }
    std::fs::read_to_string("/proc/stat")
        .ok()
        .and_then(|s| parse_proc_stat_busy_ticks(&s))
        .map(|ticks| Duration::from_secs_f64(ticks as f64 / clock_ticks_per_second()))
}

fn clock_ticks_per_second() -> f64 {
    #[cfg(unix)]
    {
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if hz > 0 {
            return hz as f64;
        }
    }
    100.0
}

/// cgroup v2 `cpu.stat` 的 `usage_usec`。
pub fn parse_cpu_stat_usec(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .and_then(|v| v.trim().parse().ok())
}

/// `/proc/stat` 首行的 CPU 时间(不含 idle / iowait)。
pub fn parse_proc_stat_busy_ticks(text: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let v: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|x| x.parse().ok())
        .collect();
    if v.len() < 4 {
        return None;
    }
    // user + nice + system + irq + softirq + steal(跳过 idle[3] 与 iowait[4])
    let mut busy = v[0] + v[1] + v[2];
    for i in [5usize, 6, 7] {
        busy += v.get(i).copied().unwrap_or(0);
    }
    Some(busy)
}

/// (used_kb, total_kb):cgroup 优先,退 /proc/meminfo。
fn read_memory() -> (Option<u64>, Option<u64>) {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let host_total_kb = meminfo.as_deref().and_then(|s| parse_meminfo_kb(s, "MemTotal"));

    // cgroup v2
    let v2_current = read_u64("/sys/fs/cgroup/memory.current");
    if let Some(current) = v2_current {
        let inactive = std::fs::read_to_string("/sys/fs/cgroup/memory.stat")
            .ok()
            .and_then(|s| parse_memory_stat_key(&s, "inactive_file"))
            .unwrap_or(0);
        let used_kb = current.saturating_sub(inactive) / 1024;
        let limit_kb = read_cgroup_limit("/sys/fs/cgroup/memory.max").map(|b| b / 1024);
        return (Some(used_kb), limit_kb.or(host_total_kb));
    }
    // cgroup v1
    if let Some(usage) = read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes") {
        let inactive = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.stat")
            .ok()
            .and_then(|s| parse_memory_stat_key(&s, "total_inactive_file"))
            .unwrap_or(0);
        let used_kb = usage.saturating_sub(inactive) / 1024;
        let limit_kb = read_cgroup_limit("/sys/fs/cgroup/memory/memory.limit_in_bytes").map(|b| b / 1024);
        return (Some(used_kb), limit_kb.or(host_total_kb));
    }
    // /proc/meminfo:used = total − available
    let available = meminfo.as_deref().and_then(|s| parse_meminfo_kb(s, "MemAvailable"));
    let used = match (host_total_kb, available) {
        (Some(t), Some(a)) => Some(t.saturating_sub(a)),
        _ => None,
    };
    (used, host_total_kb)
}

/// cgroup 上限:`max` / 未设(v1 的天文数字)当作没有上限 → None。
fn read_cgroup_limit(path: &str) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    let t = raw.trim();
    if t == "max" {
        return None;
    }
    let v: u64 = t.parse().ok()?;
    // v1 未设限额时是 u64::MAX 对齐页大小的一个巨值
    if v >= u64::MAX / 2 {
        None
    } else {
        Some(v)
    }
}

fn read_u64(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// `/proc/meminfo` 的 `<key>:  <n> kB`。
pub fn parse_meminfo_kb(text: &str, key: &str) -> Option<u64> {
    text.lines()
        .find(|l| l.starts_with(key) && l[key.len()..].starts_with(':'))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
}

/// cgroup `memory.stat` 的 `<key> <n>`。
pub fn parse_memory_stat_key(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|l| {
        let mut it = l.split_whitespace();
        match (it.next(), it.next()) {
            (Some(k), Some(v)) if k == key => v.parse().ok(),
            _ => None,
        }
    })
}

/// 根文件系统 (used, total) 字节。
fn read_disk() -> (Option<u64>, Option<u64>) {
    #[cfg(unix)]
    {
        let path = std::ffi::CString::new("/").ok();
        if let Some(path) = path {
            // SAFETY: path 是合法的 C 字符串,statvfs 只写出参
            let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
            if unsafe { libc::statvfs(path.as_ptr(), &mut st) } == 0 {
                let frsize = if st.f_frsize > 0 { st.f_frsize as u64 } else { st.f_bsize as u64 };
                let total = st.f_blocks as u64 * frsize;
                let used = (st.f_blocks as u64).saturating_sub(st.f_bfree as u64) * frsize;
                if total > 0 {
                    return (Some(used), Some(total));
                }
            }
        }
    }
    (None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cgroup_v2_cpu_stat() {
        let s = "usage_usec 1234567\nuser_usec 1000000\nsystem_usec 234567\n";
        assert_eq!(parse_cpu_stat_usec(s), Some(1_234_567));
        assert_eq!(parse_cpu_stat_usec("nr_periods 0\n"), None);
    }

    #[test]
    fn parses_proc_stat_busy_without_idle_and_iowait() {
        // user nice system idle iowait irq softirq steal
        let s = "cpu  100 20 30 900 40 1 2 3\ncpu0 1 2 3 4 5 6 7 8\n";
        // 100+20+30 + irq 1 + softirq 2 + steal 3 = 156(idle 900 / iowait 40 不计)
        assert_eq!(parse_proc_stat_busy_ticks(s), Some(156));
        assert_eq!(parse_proc_stat_busy_ticks("intr 1\n"), None);
    }

    #[test]
    fn parses_meminfo_and_memory_stat() {
        let mi = "MemTotal:        8000000 kB\nMemFree:  10 kB\nMemAvailable:    6000000 kB\n";
        assert_eq!(parse_meminfo_kb(mi, "MemTotal"), Some(8_000_000));
        assert_eq!(parse_meminfo_kb(mi, "MemAvailable"), Some(6_000_000));
        // 前缀相同但不是同一个键:MemFree 不能被 "Mem" 之类误取
        assert_eq!(parse_meminfo_kb(mi, "MemTotalX"), None);
        let ms = "anon 100\ninactive_file 4096\nslab 7\n";
        assert_eq!(parse_memory_stat_key(ms, "inactive_file"), Some(4096));
        assert_eq!(parse_memory_stat_key(ms, "nope"), None);
    }

    #[test]
    fn cpu_percent_is_single_core_100_scale() {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(10);
        // 10s 墙钟里用掉 10s CPU = 满一核
        assert_eq!(
            cpu_percent_between(Duration::ZERO, t0, Duration::from_secs(10), t1),
            Some(100.0)
        );
        // 用掉 25s = 2.5 核
        assert_eq!(
            cpu_percent_between(Duration::ZERO, t0, Duration::from_secs(25), t1),
            Some(250.0)
        );
        // 计数器回绕 / 零时长 → None
        assert_eq!(
            cpu_percent_between(Duration::from_secs(5), t0, Duration::from_secs(1), t1),
            None
        );
        assert_eq!(cpu_percent_between(Duration::ZERO, t0, Duration::from_secs(1), t0), None);
    }

    #[tokio::test]
    async fn metrics_reports_something_on_this_machine() {
        let Json(m) = metrics().await;
        assert!(!m.collected_at.is_empty());
        assert!(m.cpu_cores.unwrap_or(0) >= 1);
        // Linux 以外(开发机 macOS)只保证不 panic;容器里三项都应有值
        if cfg!(target_os = "linux") {
            assert!(m.mem_total_kb.is_some(), "mem_total_kb");
            assert!(m.disk_total_bytes.is_some(), "disk_total_bytes");
        }
    }
}
