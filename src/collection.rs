use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CollectionLevel {
    Level0,
    Level1,
    Level2,
    Level3,
}

impl CollectionLevel {
    pub const ALL_DESC: [CollectionLevel; 4] = [
        CollectionLevel::Level3,
        CollectionLevel::Level2,
        CollectionLevel::Level1,
        CollectionLevel::Level0,
    ];
}

impl fmt::Display for CollectionLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            CollectionLevel::Level0 => "Level 0",
            CollectionLevel::Level1 => "Level 1",
            CollectionLevel::Level2 => "Level 2",
            CollectionLevel::Level3 => "Level 3",
        };
        write!(f, "{label}")
    }
}

#[derive(Debug, Clone)]
pub struct CollectionPolicy {
    pub preferred_level: CollectionLevel,
    pub max_accepted_level: CollectionLevel,
    pub expert_mode_enabled: bool,
}

impl Default for CollectionPolicy {
    fn default() -> Self {
        Self {
            preferred_level: CollectionLevel::Level3,
            max_accepted_level: CollectionLevel::Level2,
            expert_mode_enabled: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CapabilityProbe {
    pub has_mysql_status_access: bool,
    pub has_mysql_variables_access: bool,
    pub has_information_schema_access: bool,
    pub has_replication_status_access: bool,
    pub has_os_metrics_access: bool,
    pub can_enable_slow_log_hot_switch: bool,
    pub can_read_slow_log: bool,
    pub can_read_error_log: bool,
    pub performance_schema_enabled: bool,
    pub has_performance_schema_access: bool,
    pub has_sys_schema_access: bool,
    pub can_capture_tcpdump_short_window: bool,
    pub can_capture_perf_short_window: bool,
    pub can_capture_strace_short_window: bool,
    pub can_sample_innodb_status_high_frequency: bool,
}

#[derive(Debug, Clone)]
pub struct CollectionTask {
    pub level: CollectionLevel,
    pub name: &'static str,
    pub source: &'static str,
    pub purpose: &'static str,
}

#[derive(Debug, Clone)]
pub struct LevelEvaluation {
    pub level: CollectionLevel,
    pub ok: bool,
    pub reasons: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub struct NegotiationResult {
    pub selected_level: CollectionLevel,
    pub evaluations: Vec<LevelEvaluation>,
    pub tasks: Vec<CollectionTask>,
}

pub fn negotiate_collection_level(
    policy: &CollectionPolicy,
    probe: &CapabilityProbe,
) -> NegotiationResult {
    let target = policy.preferred_level.min(policy.max_accepted_level);
    let mut evaluations = Vec::with_capacity(CollectionLevel::ALL_DESC.len());
    let mut selected = CollectionLevel::Level0;

    for level in CollectionLevel::ALL_DESC {
        if level > target {
            continue;
        }
        let evaluation = evaluate_level(level, policy, probe);
        if selected == CollectionLevel::Level0 && evaluation.ok {
            selected = level;
        }
        evaluations.push(evaluation);
    }

    if !evaluations
        .iter()
        .any(|entry| entry.level == CollectionLevel::Level0 && entry.ok)
    {
        selected = CollectionLevel::Level0;
    }

    let tasks = tasks_for_level(selected, probe);
    NegotiationResult {
        selected_level: selected,
        evaluations,
        tasks,
    }
}

fn evaluate_level(
    level: CollectionLevel,
    policy: &CollectionPolicy,
    probe: &CapabilityProbe,
) -> LevelEvaluation {
    let mut reasons = Vec::new();

    match level {
        CollectionLevel::Level0 => {
            if !probe.has_mysql_status_access {
                reasons.push("missing SHOW GLOBAL STATUS access");
            }
            if !probe.has_mysql_variables_access {
                reasons.push("missing SHOW VARIABLES access");
            }
            if !probe.has_information_schema_access {
                reasons.push("missing INFORMATION_SCHEMA access");
            }
            if !probe.has_replication_status_access {
                reasons.push("missing replication status access");
            }
            if !probe.has_os_metrics_access {
                reasons.push("missing OS-level metrics access (/proc, vmstat, iostat, sar)");
            }
        }
        CollectionLevel::Level1 => {
            reasons.extend(evaluate_level(CollectionLevel::Level0, policy, probe).reasons);
            if !probe.can_enable_slow_log_hot_switch {
                reasons.push("cannot hot-enable slow log");
            }
            if !probe.can_read_slow_log {
                reasons.push("cannot collect slow log for digest aggregation");
            }
            if !probe.can_read_error_log {
                reasons.push("cannot collect error log");
            }
        }
        CollectionLevel::Level2 => {
            reasons.extend(evaluate_level(CollectionLevel::Level1, policy, probe).reasons);
            if !probe.performance_schema_enabled {
                reasons.push("performance_schema is disabled");
            }
            if !probe.has_performance_schema_access {
                reasons.push("missing performance_schema read access");
            }
        }
        CollectionLevel::Level3 => {
            reasons.extend(evaluate_level(CollectionLevel::Level2, policy, probe).reasons);
            if !policy.expert_mode_enabled {
                reasons.push("expert mode is disabled by policy");
            }
            if !probe.can_sample_innodb_status_high_frequency {
                reasons.push("cannot sample InnoDB status at high frequency");
            }
            let has_deep_sampler = probe.can_capture_tcpdump_short_window
                || probe.can_capture_perf_short_window
                || probe.can_capture_strace_short_window;
            if !has_deep_sampler {
                reasons.push("no short-window deep sampler available (tcpdump/perf/strace)");
            }
        }
    }

    LevelEvaluation {
        level,
        ok: reasons.is_empty(),
        reasons,
    }
}

pub fn tasks_for_level(level: CollectionLevel, probe: &CapabilityProbe) -> Vec<CollectionTask> {
    let mut tasks = Vec::new();
    tasks.extend(level0_tasks());

    if level >= CollectionLevel::Level1 {
        tasks.extend(level1_tasks());
    }
    if level >= CollectionLevel::Level2 {
        tasks.extend(level2_tasks(probe));
    }
    if level >= CollectionLevel::Level3 {
        tasks.extend(level3_tasks());
    }
    tasks
}

fn level0_tasks() -> Vec<CollectionTask> {
    vec![
        CollectionTask {
            level: CollectionLevel::Level0,
            name: "global_status",
            source: "SHOW GLOBAL STATUS",
            purpose: "Capture throughput/latency/error counters",
        },
        CollectionTask {
            level: CollectionLevel::Level0,
            name: "global_variables",
            source: "SHOW VARIABLES",
            purpose: "Capture runtime settings and limits",
        },
        CollectionTask {
            level: CollectionLevel::Level0,
            name: "schema_storage",
            source: "INFORMATION_SCHEMA.TABLES/STATISTICS",
            purpose: "Collect table size and index layout",
        },
        CollectionTask {
            level: CollectionLevel::Level0,
            name: "replication_status",
            source: "SHOW REPLICA/SLAVE STATUS",
            purpose: "Observe replication health and lag",
        },
        CollectionTask {
            level: CollectionLevel::Level0,
            name: "os_basic_metrics",
            source: "/proc + vmstat/iostat/sar",
            purpose: "Capture CPU, memory, IO pressure",
        },
    ]
}

fn level1_tasks() -> Vec<CollectionTask> {
    vec![
        CollectionTask {
            level: CollectionLevel::Level1,
            name: "slow_log_window",
            source: "slow_query_log",
            purpose: "Collect slow SQL with threshold and time window",
        },
        CollectionTask {
            level: CollectionLevel::Level1,
            name: "slow_log_digest",
            source: "slow_query_log digest",
            purpose: "Aggregate similar SQL fingerprints",
        },
        CollectionTask {
            level: CollectionLevel::Level1,
            name: "error_log_alerts",
            source: "error log",
            purpose: "Detect deadlock/crash recovery/purge/replication alerts",
        },
    ]
}

fn level2_tasks(probe: &CapabilityProbe) -> Vec<CollectionTask> {
    let mut tasks = vec![
        CollectionTask {
            level: CollectionLevel::Level2,
            name: "statement_summary",
            source: "performance_schema.events_statements_summary_by_digest",
            purpose: "Track SQL latency and digest-level hotspots",
        },
        CollectionTask {
            level: CollectionLevel::Level2,
            name: "wait_events",
            source: "performance_schema wait event tables",
            purpose: "Attribute CPU/IO/lock wait bottlenecks",
        },
        CollectionTask {
            level: CollectionLevel::Level2,
            name: "metadata_locks",
            source: "performance_schema.metadata_locks",
            purpose: "Find metadata lock contention",
        },
        CollectionTask {
            level: CollectionLevel::Level2,
            name: "transaction_lock_views",
            source: "performance_schema",
            purpose: "Correlate transaction and lock wait chains",
        },
    ];

    if probe.has_sys_schema_access {
        tasks.push(CollectionTask {
            level: CollectionLevel::Level2,
            name: "sys_schema_helpers",
            source: "sys schema",
            purpose: "Use pre-joined lock and statement diagnostic views",
        });
    }
    tasks
}

fn level3_tasks() -> Vec<CollectionTask> {
    vec![
        CollectionTask {
            level: CollectionLevel::Level3,
            name: "tcpdump_short_window",
            source: "tcpdump",
            purpose: "Capture packet-level anomalies in short windows",
        },
        CollectionTask {
            level: CollectionLevel::Level3,
            name: "perf_or_strace_short_window",
            source: "perf/strace",
            purpose: "Capture syscall and CPU hotspots in short windows",
        },
        CollectionTask {
            level: CollectionLevel::Level3,
            name: "innodb_status_hf",
            source: "SHOW ENGINE INNODB STATUS",
            purpose: "High-frequency sampling for expert diagnostics",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_downgrade_to_level1_when_performance_schema_unavailable() {
        let policy = CollectionPolicy {
            preferred_level: CollectionLevel::Level3,
            max_accepted_level: CollectionLevel::Level3,
            expert_mode_enabled: true,
        };
        let probe = CapabilityProbe {
            has_mysql_status_access: true,
            has_mysql_variables_access: true,
            has_information_schema_access: true,
            has_replication_status_access: true,
            has_os_metrics_access: true,
            can_enable_slow_log_hot_switch: true,
            can_read_slow_log: true,
            can_read_error_log: true,
            performance_schema_enabled: false,
            has_performance_schema_access: false,
            has_sys_schema_access: false,
            can_capture_tcpdump_short_window: false,
            can_capture_perf_short_window: false,
            can_capture_strace_short_window: false,
            can_sample_innodb_status_high_frequency: false,
        };

        let result = negotiate_collection_level(&policy, &probe);
        assert_eq!(result.selected_level, CollectionLevel::Level1);
    }

    #[test]
    fn should_select_level2_when_customer_caps_at_level2() {
        let policy = CollectionPolicy::default();
        let probe = CapabilityProbe {
            has_mysql_status_access: true,
            has_mysql_variables_access: true,
            has_information_schema_access: true,
            has_replication_status_access: true,
            has_os_metrics_access: true,
            can_enable_slow_log_hot_switch: true,
            can_read_slow_log: true,
            can_read_error_log: true,
            performance_schema_enabled: true,
            has_performance_schema_access: true,
            has_sys_schema_access: true,
            can_capture_tcpdump_short_window: true,
            can_capture_perf_short_window: true,
            can_capture_strace_short_window: false,
            can_sample_innodb_status_high_frequency: true,
        };

        let result = negotiate_collection_level(&policy, &probe);
        assert_eq!(result.selected_level, CollectionLevel::Level2);
    }
}
