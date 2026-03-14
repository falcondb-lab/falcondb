//! Thread CPU affinity helpers — P1-3.
//!
//! Best-effort: silently no-ops on unsupported platforms or when the OS
//! rejects the request (e.g. insufficient privilege).  Never panics.
//!
//! Usage:
//! ```ignore
//! thread_affinity::pin_current_thread(Some(3)); // pin to logical core 3
//! thread_affinity::pin_current_thread(None);    // no-op
//! ```

/// Attempt to pin the calling thread to `core_id`.
/// Returns `true` if the affinity was set successfully, `false` otherwise.
/// Pass `None` to skip (useful when the caller holds an optional config value).
#[allow(unused_variables)]
pub fn pin_current_thread(core_id: Option<usize>) -> bool {
    let Some(core) = core_id else { return false };
    pin_impl(core)
}

/// Linux implementation via `nix::sched::sched_setaffinity`.
#[cfg(target_os = "linux")]
fn pin_impl(core: usize) -> bool {
    use nix::sched::{sched_setaffinity, CpuSet};
    use nix::unistd::Pid;

    let mut cpu_set = CpuSet::new();
    if let Err(e) = cpu_set.set(core) {
        tracing::debug!("thread_affinity: CpuSet::set(core={core}) failed: {e} (non-fatal)");
        return false;
    }
    match sched_setaffinity(Pid::from_raw(0), &cpu_set) {
        Ok(()) => {
            tracing::debug!("thread_affinity: pinned to core {core}");
            true
        }
        Err(e) => {
            tracing::debug!(
                "thread_affinity: sched_setaffinity(core={core}) failed: {e} (non-fatal)"
            );
            false
        }
    }
}

/// Windows implementation via `SetThreadAffinityMask`.
#[cfg(target_os = "windows")]
fn pin_impl(core: usize) -> bool {
    if core >= 64 {
        tracing::debug!(
            "thread_affinity: core={core} exceeds Windows 64-bit mask limit (non-fatal)"
        );
        return false;
    }
    use windows_sys::Win32::System::Threading::{GetCurrentThread, SetThreadAffinityMask};
    let mask: usize = 1usize << core;
    // SAFETY: GetCurrentThread() returns a pseudo-handle valid for this call;
    // SetThreadAffinityMask is safe when the mask has at least one valid bit.
    let prev = unsafe { SetThreadAffinityMask(GetCurrentThread(), mask) };
    if prev == 0 {
        let err = std::io::Error::last_os_error();
        tracing::debug!(
            "thread_affinity: SetThreadAffinityMask(core={core}) failed: {err} (non-fatal)"
        );
        return false;
    }
    tracing::debug!("thread_affinity: pinned to core {core}");
    true
}

/// Fallback for macOS and other platforms (no-op).
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
fn pin_impl(core: usize) -> bool {
    tracing::debug!("thread_affinity: platform unsupported, core={core} ignored");
    false
}
