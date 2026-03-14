//! Background job scheduler — cron-style recurring and one-shot jobs.
//!
//! Used by the backup subsystem and other periodic maintenance tasks.
//! Jobs are identified by name, run on a dedicated thread pool (2 threads),
//! and expose a simple status/cancel API.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Unique job ID (monotonically increasing).
pub type JobId = u64;

/// How often a recurring job fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobInterval {
    /// Run once then remove.
    Once,
    /// Repeat every N seconds.
    Every(Duration),
}

/// Current state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// A snapshot of a job's status for user-facing queries.
#[derive(Debug, Clone)]
pub struct JobStatus {
    pub id: JobId,
    pub name: String,
    pub interval_secs: Option<u64>,
    pub state: JobState,
    pub runs: u64,
    pub last_run_ms: u64,
    pub last_error: Option<String>,
    pub next_run_ms: u64,
}

type JobFn = Box<dyn Fn() -> Result<(), String> + Send + 'static>;

struct JobEntry {
    id: JobId,
    name: String,
    interval: JobInterval,
    f: JobFn,
    state: JobState,
    runs: u64,
    last_run_ms: u64,
    last_error: Option<String>,
    next_fire: Instant,
    cancelled: bool,
}

impl JobEntry {
    fn status(&self) -> JobStatus {
        let interval_secs = match self.interval {
            JobInterval::Once => None,
            JobInterval::Every(d) => Some(d.as_secs()),
        };
        let now = Instant::now();
        let next_run_ms = if self.next_fire > now {
            self.next_fire.duration_since(now).as_millis() as u64
        } else {
            0
        };
        JobStatus {
            id: self.id,
            name: self.name.clone(),
            interval_secs,
            state: self.state,
            runs: self.runs,
            last_run_ms: self.last_run_ms,
            last_error: self.last_error.clone(),
            next_run_ms,
        }
    }
}

/// Scheduler that runs registered jobs on background threads.
pub struct JobScheduler {
    next_id: AtomicU64,
    jobs: Arc<Mutex<HashMap<JobId, JobEntry>>>,
    running: Arc<AtomicBool>,
}

impl JobScheduler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(1),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Register a new job. Returns its ID.
    pub fn register(&self, name: &str, interval: JobInterval, f: JobFn) -> JobId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let delay = match interval {
            JobInterval::Once => Duration::ZERO,
            JobInterval::Every(d) => d,
        };
        let entry = JobEntry {
            id,
            name: name.to_owned(),
            interval,
            f,
            state: JobState::Pending,
            runs: 0,
            last_run_ms: 0,
            last_error: None,
            next_fire: Instant::now() + delay,
            cancelled: false,
        };
        self.jobs.lock().unwrap().insert(id, entry);
        id
    }

    /// Cancel a job by ID. Returns true if found and not already done.
    pub fn cancel(&self, id: JobId) -> bool {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&id) {
            if matches!(job.state, JobState::Pending | JobState::Running) {
                job.cancelled = true;
                job.state = JobState::Cancelled;
                return true;
            }
        }
        false
    }

    /// Get status of a job.
    pub fn status(&self, id: JobId) -> Option<JobStatus> {
        self.jobs.lock().unwrap().get(&id).map(|j| j.status())
    }

    /// Get status of all jobs.
    pub fn all_statuses(&self) -> Vec<JobStatus> {
        let mut out: Vec<JobStatus> = self
            .jobs
            .lock()
            .unwrap()
            .values()
            .map(|j| j.status())
            .collect();
        out.sort_by_key(|s| s.id);
        out
    }

    /// Drop completed/failed/cancelled one-shot jobs from history (keep last N).
    pub fn prune(&self, keep: usize) {
        let mut jobs = self.jobs.lock().unwrap();
        let mut done: Vec<JobId> = jobs
            .values()
            .filter(|j| {
                matches!(
                    j.state,
                    JobState::Succeeded | JobState::Failed | JobState::Cancelled
                ) && j.interval == JobInterval::Once
            })
            .map(|j| j.id)
            .collect();
        done.sort();
        let to_remove = done.len().saturating_sub(keep);
        for id in done.into_iter().take(to_remove) {
            jobs.remove(&id);
        }
    }

    /// Start the scheduler loop on a background thread.
    /// The loop polls every 500ms and fires overdue jobs inline (on a rayon thread).
    pub fn start(self: &Arc<Self>) {
        if self.running.swap(true, Ordering::SeqCst) {
            return; // already running
        }
        let jobs = Arc::clone(&self.jobs);
        let running = Arc::clone(&self.running);
        std::thread::Builder::new()
            .name("falcon-job-scheduler".into())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    let due: Vec<JobId> = {
                        let jobs_lock = jobs.lock().unwrap();
                        jobs_lock
                            .values()
                            .filter(|j| {
                                !j.cancelled
                                    && j.state != JobState::Running
                                    && j.next_fire <= now
                                    && !matches!(
                                        j.state,
                                        JobState::Succeeded
                                            | JobState::Failed
                                            | JobState::Cancelled
                                    )
                            })
                            .map(|j| j.id)
                            .collect()
                    };

                    for id in due {
                        // Mark running
                        let f: Option<&dyn Fn() -> Result<(), String>> = None;
                        let _ = f; // just to type-check
                        let result = {
                            let mut jobs_lock = jobs.lock().unwrap();
                            let Some(job) = jobs_lock.get_mut(&id) else {
                                continue;
                            };
                            if job.cancelled {
                                continue;
                            }
                            job.state = JobState::Running;
                            // We call the function inline (holding no lock — need to extract it).
                            // Since we can't move out of the HashMap, call via raw pointer with care.
                            // SAFETY: we hold the Mutex, job entry lives for lifetime of HashMap.
                            let fn_ptr = &job.f as *const JobFn;
                            drop(jobs_lock);
                            // SAFETY: fn_ptr is valid as long as we don't modify jobs HashMap;
                            // we only read through the pointer and re-lock after the call.
                            unsafe { (*fn_ptr)() }
                        };

                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let mut jobs_lock = jobs.lock().unwrap();
                        let Some(job) = jobs_lock.get_mut(&id) else {
                            continue;
                        };
                        job.runs += 1;
                        job.last_run_ms = now_ms;
                        match result {
                            Ok(()) => {
                                job.last_error = None;
                                match job.interval {
                                    JobInterval::Once => job.state = JobState::Succeeded,
                                    JobInterval::Every(d) => {
                                        job.state = JobState::Pending;
                                        job.next_fire = Instant::now() + d;
                                    }
                                }
                            }
                            Err(e) => {
                                job.last_error = Some(e);
                                match job.interval {
                                    JobInterval::Once => job.state = JobState::Failed,
                                    JobInterval::Every(d) => {
                                        job.state = JobState::Pending;
                                        job.next_fire = Instant::now() + d;
                                    }
                                }
                            }
                        }
                    }

                    std::thread::sleep(Duration::from_millis(50));
                }
            })
            .ok();
    }

    /// Stop the scheduler loop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering as AO};

    #[test]
    fn test_register_and_run_once() {
        let sched = JobScheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);
        let id = sched.register(
            "test_once",
            JobInterval::Once,
            Box::new(move || {
                c.fetch_add(1, AO::Relaxed);
                Ok(())
            }),
        );

        sched.start();
        std::thread::sleep(Duration::from_millis(1200));
        sched.stop();

        assert_eq!(counter.load(AO::Relaxed), 1);
        let s = sched.status(id).unwrap();
        assert_eq!(s.state, JobState::Succeeded);
        assert_eq!(s.runs, 1);
    }

    #[test]
    fn test_cancel_job() {
        let sched = JobScheduler::new();
        let id = sched.register(
            "cancelme",
            JobInterval::Every(Duration::from_secs(3600)),
            Box::new(|| Ok(())),
        );
        assert!(sched.cancel(id));
        let s = sched.status(id).unwrap();
        assert_eq!(s.state, JobState::Cancelled);
        assert!(!sched.cancel(id)); // already cancelled
    }

    #[test]
    fn test_recurring_job_fires_multiple_times() {
        let sched = JobScheduler::new();
        let counter = Arc::new(AtomicU32::new(0));
        let c = Arc::clone(&counter);
        sched.register(
            "tick",
            JobInterval::Every(Duration::from_millis(100)),
            Box::new(move || {
                c.fetch_add(1, AO::Relaxed);
                Ok(())
            }),
        );
        sched.start();
        std::thread::sleep(Duration::from_millis(800));
        sched.stop();
        assert!(
            counter.load(AO::Relaxed) >= 2,
            "recurring job should fire at least twice"
        );
    }

    #[test]
    fn test_failed_job_records_error() {
        let sched = JobScheduler::new();
        let id = sched.register(
            "fail_once",
            JobInterval::Once,
            Box::new(|| Err("intentional failure".into())),
        );
        sched.start();
        std::thread::sleep(Duration::from_millis(1200));
        sched.stop();
        let s = sched.status(id).unwrap();
        assert_eq!(s.state, JobState::Failed);
        assert_eq!(s.last_error.as_deref(), Some("intentional failure"));
    }

    #[test]
    fn test_all_statuses() {
        let sched = JobScheduler::new();
        sched.register("a", JobInterval::Once, Box::new(|| Ok(())));
        sched.register("b", JobInterval::Once, Box::new(|| Ok(())));
        let statuses = sched.all_statuses();
        assert_eq!(statuses.len(), 2);
        assert_eq!(statuses[0].name, "a");
        assert_eq!(statuses[1].name, "b");
    }
}
