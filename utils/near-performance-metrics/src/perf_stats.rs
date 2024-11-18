use nix::sys::resource::{getrusage, Usage, UsageWho};
use nix::sys::time::{TimeVal, TimeValLike};
use nix::Result as NixResult;
use std::time::{Duration, Instant};

pub struct Stats {
    pub total_time: Duration,
    pub user_cpu_time: Duration,
    pub system_cpu_time: Duration,
    pub blocked_time: Duration,
}

fn display_duration(d: &Duration) -> String {
    let secs = d.as_secs();
    let micros = d.subsec_micros();
    if secs > 0 {
        format!("{}s {}us", secs, micros)
    } else {
        format!("{}us", micros)
    }
}

pub fn display_stats(stats: &NixResult<Stats>) -> String {
    match stats {
        Ok(stats) => {
            format!(
                "total: {} user: {} sys: {} blocked: {}",
                display_duration(&stats.total_time),
                display_duration(&stats.user_cpu_time),
                display_duration(&stats.system_cpu_time),
                display_duration(&stats.blocked_time)
            )
        }
        Err(e) => format!("Error: {:?}", e),
    }
}

fn duration_from_timeval(timeval: TimeVal) -> Duration {
    Duration::new(timeval.num_seconds() as u64, timeval.num_nanoseconds() as u32)
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "openbsd"))]
const USAGE_WHO: UsageWho = UsageWho::RUSAGE_THREAD;

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "openbsd")))]
const USAGE_WHO: UsageWho = UsageWho::RUSAGE_SELF;

fn usage_stats(start: Instant, usage_start: Usage) -> NixResult<Stats> {
    let usage_end = getrusage(USAGE_WHO).expect("Failed to get resource usage");

    // Record final wall-clock time and CPU usage
    let total_time = start.elapsed();
    // Calculate CPU time used in user and system space
    let user_cpu_time = duration_from_timeval(usage_end.user_time())
        .saturating_sub(duration_from_timeval(usage_start.user_time()));
    let system_cpu_time = duration_from_timeval(usage_end.system_time())
        .saturating_sub(duration_from_timeval(usage_start.system_time()));

    // Calculate blocked time as total time minus CPU time

    let blocked_time = total_time.saturating_sub(user_cpu_time).saturating_sub(system_cpu_time);

    Ok(Stats { total_time, user_cpu_time, system_cpu_time, blocked_time })
}

pub fn instrument<F, R>(func: F) -> (R, NixResult<Stats>)
where
    F: FnOnce() -> R,
{
    // Record wall-clock start time
    let start = Instant::now();

    // Record initial CPU usage
    let usage_start = getrusage(USAGE_WHO);

    // Execute the function
    let ret = func();

    match usage_start {
        Ok(usage_start) => (ret, usage_stats(start, usage_start)),
        Err(e) => (ret, Err(e)),
    }
}
