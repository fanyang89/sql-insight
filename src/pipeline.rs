use std::sync::mpsc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::ValueEnum;
use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Once,
    Daemon,
}

#[derive(Debug, Clone, Serialize)]
pub struct SchedulerConfig {
    pub mode: RunMode,
    pub interval_secs: u64,
    pub jitter_pct: f64,
    pub timeout_secs: u64,
    pub retry_times: u32,
    pub retry_backoff_ms: u64,
    pub max_cycles: Option<u32>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            mode: RunMode::Once,
            interval_secs: 60,
            jitter_pct: 0.1,
            timeout_secs: 120,
            retry_times: 1,
            retry_backoff_ms: 1_000,
            max_cycles: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ScheduleWindow {
    pub start_unix_ms: u128,
    pub end_unix_ms: u128,
    pub duration_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttemptTrace {
    pub attempt: u32,
    pub duration_ms: u128,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceStatus {
    pub source: String,
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnifiedCollectionRecord<T>
where
    T: Serialize,
{
    pub contract_version: String,
    pub run_id: String,
    pub cycle: u32,
    pub engine: String,
    pub requested_level: String,
    pub selected_level: Option<String>,
    pub schedule: SchedulerConfig,
    pub window: ScheduleWindow,
    pub attempts: Vec<AttemptTrace>,
    pub source_status: Vec<SourceStatus>,
    pub warnings: Vec<String>,
    pub status: String,
    pub error: Option<String>,
    pub payload: Option<T>,
}

pub fn run_with_timeout<T, F>(timeout: Duration, job: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = job();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(format!(
            "collection timed out after {}ms",
            timeout.as_millis()
        )),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err("collection worker disconnected".to_string())
        }
    }
}

pub fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|it| it.as_millis())
        .unwrap_or(0)
}

pub fn new_run_id() -> String {
    let now = now_unix_ms();
    format!("run-{now}-{}", std::process::id())
}

pub fn jittered_interval(base_secs: u64, jitter_pct: f64) -> Duration {
    let base_ms = (base_secs as u128) * 1_000;
    if base_ms == 0 {
        return Duration::from_millis(0);
    }
    let jitter_abs = ((base_ms as f64) * jitter_pct.clamp(0.0, 0.9)).round() as i128;
    if jitter_abs == 0 {
        return Duration::from_millis(base_ms as u64);
    }
    let span = (jitter_abs * 2 + 1) as u128;
    let entropy = now_unix_ms() % span;
    let offset = entropy as i128 - jitter_abs;
    let jittered = (base_ms as i128 + offset).max(1_000);
    Duration::from_millis(jittered as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_should_fail() {
        let result = run_with_timeout(Duration::from_millis(30), || {
            thread::sleep(Duration::from_millis(100));
            Ok::<_, String>(42)
        });
        assert!(result.is_err());
    }

    #[test]
    fn jitter_should_stay_in_bounds() {
        let base_secs = 10;
        let jitter_pct = 0.2;
        let d = jittered_interval(base_secs, jitter_pct);
        let min_ms = ((base_secs as f64) * (1.0 - jitter_pct) * 1_000.0) as u64;
        let max_ms = ((base_secs as f64) * (1.0 + jitter_pct) * 1_000.0) as u64;
        let got = d.as_millis() as u64;
        assert!(got >= min_ms);
        assert!(got <= max_ms);
    }
}
