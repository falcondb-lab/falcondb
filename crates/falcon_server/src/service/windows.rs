//! Windows Service Control Manager (SCM) integration.
//!
//! This module implements native Windows Service support using the
//! `windows-service` crate. When FalconDB is started by SCM (via
//! `falcon service dispatch`), this module:
//!
//! 1. Registers an event handler for STOP / SHUTDOWN
//! 2. Sets service status to Running
//! 3. Runs the actual server
//! 4. Sets service status to Stopped on exit

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

use crate::shutdown::ShutdownCoordinator;

/// Type alias for the async server runner function.
/// Signature: `async fn(config_path: &str, coordinator: Option<ShutdownCoordinator>) -> anyhow::Result<()>`
pub type ServerRunnerFn = Box<
    dyn Fn(String, Option<ShutdownCoordinator>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Global server runner set by the binary crate before dispatch.
static SERVER_RUNNER: OnceLock<ServerRunnerFn> = OnceLock::new();

/// Register the server runner function. Call from main() before dispatch.
pub fn set_server_runner(f: ServerRunnerFn) {
    let _ = SERVER_RUNNER.set(f);
}

/// Get the registered server runner.
pub fn get_server_runner() -> Option<&'static ServerRunnerFn> {
    SERVER_RUNNER.get()
}

#[cfg(windows)]
pub mod scm {
    use std::ffi::OsString;
    use std::sync::OnceLock;
    use std::time::Duration;

    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState,
        ServiceStatus, ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_dispatcher;
    use windows_service::Result as WinServiceResult;

    use crate::shutdown::{ShutdownCoordinator, ShutdownReason};

    /// Global config path passed from main → service entry point.
    static CONFIG_PATH: OnceLock<String> = OnceLock::new();

    /// Global shutdown coordinator passed from dispatch → event handler.
    static SHUTDOWN_COORDINATOR: OnceLock<ShutdownCoordinator> = OnceLock::new();

    /// Set the config path before dispatching to SCM.
    pub fn set_config_path(path: &str) {
        let _ = CONFIG_PATH.set(path.to_owned());
    }

    /// Entry point: register with SCM and block until service exits.
    pub fn dispatch() -> WinServiceResult<()> {
        windows_service::define_windows_service!(ffi_service_main, service_main);
        service_dispatcher::start("FalconDB", ffi_service_main)?;
        Ok(())
    }

    /// Called by SCM in a new thread. This is the actual service main.
    fn service_main(_arguments: Vec<OsString>) {
        if let Err(e) = run_service() {
            tracing::error!("Service main error: {}", e);
        }
    }

    fn run_service() -> WinServiceResult<()> {
        let coordinator = ShutdownCoordinator::new();
        let _ = SHUTDOWN_COORDINATOR.set(coordinator.clone());

        // Register event handler with SCM
        let event_handler = move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    tracing::info!(
                        event = ?control_event,
                        "SCM control event received — initiating shutdown"
                    );
                    if let Some(coord) = SHUTDOWN_COORDINATOR.get() {
                        coord.shutdown(ShutdownReason::ServiceStop);
                    }
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        };

        let status_handle =
            service_control_handler::register("FalconDB", event_handler)?;

        // Report Running
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Running,
            controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        })?;

        tracing::info!("FalconDB service status: RUNNING");

        // Get config path (set by main before dispatch)
        let config_path = CONFIG_PATH
            .get()
            .cloned()
            .unwrap_or_else(|| {
                super::super::paths::service_config_path()
                    .to_string_lossy()
                    .to_string()
            });

        // Run the actual server via the registered runner
        let runner = super::get_server_runner()
            .expect("Server runner must be registered before dispatch");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let exit_code = rt.block_on(async {
            match runner(config_path, Some(coordinator)).await {
                Ok(()) => 0u32,
                Err(e) => {
                    tracing::error!("Server error: {}", e);
                    1u32
                }
            }
        });

        // Report Stopped
        status_handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: ServiceState::Stopped,
            controls_accepted: ServiceControlAccept::empty(),
            exit_code: ServiceExitCode::Win32(exit_code),
            checkpoint: 0,
            wait_hint: Duration::default(),
            process_id: None,
        })?;

        tracing::info!("FalconDB service status: STOPPED");
        Ok(())
    }
}

/// Stub for non-Windows platforms.
#[cfg(not(windows))]
pub mod scm {
    pub fn set_config_path(_path: &str) {}

    pub fn dispatch() -> Result<(), String> {
        Err("Windows Service dispatch is only available on Windows".to_string())
    }
}
