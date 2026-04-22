use ferro_flow::events::EventDispatcher;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    process::Command,
    sync::{Mutex, OnceLock},
};

/// Serialize vcan setup/teardown across tests to avoid races when multiple tests
/// try to create/delete interfaces at the same time.
static VCAN_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

static VCAN_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// RAII guard that can delete the vcan interface on drop.
///
/// By default, teardown is performed to avoid leaking randomized vcan interfaces.
/// Disable teardown by setting `FERROFLOW_NO_TEARDOWN_VCAN=1`.
pub struct VcanGuard {
    iface: String,
    teardown: bool,
}

impl Drop for VcanGuard {
    fn drop(&mut self) {
        if !self.teardown {
            return;
        }

        // Prefer the setcap-based helper if available in PATH.
        // Fallback to the sudo-based shell script.
        let helper_ok = Command::new("ferroflow-vcan")
            .args(["down", &self.iface])
            .status()
            .is_ok_and(|s| s.success());

        if !helper_ok {
            let _ = Command::new("./scripts/teardown-vcan.sh")
                .arg(&self.iface)
                .status();
        }
    }
}

/// Ensure a vcan interface exists and is up on the host.
/// Notes:
/// - Requires CAP_NET_ADMIN on the host. The helper script will use `sudo` when needed.
/// - In CI (GitHub Actions ubuntu runners) `sudo` is typically passwordless.
///
/// Returns a guard that can optionally teardown the interface when dropped.
pub fn ensure_vcan(iface: &str) -> VcanGuard {
    // Avoid races when tests run concurrently.
    let lock = VCAN_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().expect("vcan setup lock poisoned");

    if Command::new("ip")
        .args(["link", "show", iface])
        .status()
        .is_ok_and(|s| s.success())
    {
        print!("vcan interface '{iface}' already exists, skipping setup");
    } else {
        // Prefer the setcap-based helper if available in PATH.
        // Fallback to the sudo-based shell script.
        let helper_ok = Command::new("ferroflow-vcan")
            .args(["up", iface])
            .status()
            .is_ok_and(|s| s.success());

        if !helper_ok {
            let status = Command::new("./scripts/setup-vcan.sh")
                .arg(iface)
                .status()
                .expect("failed to execute ./scripts/setup-vcan.sh");

            assert!(
                status.success(),
                "failed to setup vcan interface '{iface}'.\n\
                 Try running: ./scripts/setup-vcan.sh {iface}\n\
                 (may require sudo / CAP_NET_ADMIN)"
            );
        }

        // Verify it exists now.
        assert!(
            Command::new("ip")
                .args(["link", "show", iface])
                .status()
                .is_ok_and(|s| s.success()),
            "vcan interface '{iface}' still not present after setup"
        );
    }

    let teardown = std::env::var("FERROFLOW_NO_TEARDOWN_VCAN").ok().as_deref() != Some("1");

    VcanGuard {
        iface: iface.to_string(),
        teardown,
    }
}

/// RAII guard that dispatches `Event::Shutdown` when dropped, to trigger application shutdown in integration tests.
/// Used in integration tests with asserts to ensure that the app threads shutdown properly on an assert failure
pub struct ShutdownGuard<'a> {
    pub event_dispatcher: &'a EventDispatcher,
}

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {
        // Keep this infallible: it's used to ensure cleanup when asserts/panics happen.
        self.event_dispatcher
            .dispatch(ferro_flow::events::Event::Shutdown);
    }
}

/// Generate a unique vcan interface name for the current test process.
pub fn unique_vcan_iface() -> String {
    let pid = (std::process::id() % 10_000) as usize;
    let c = VCAN_COUNTER.fetch_add(1, Ordering::Relaxed) % 1_000_000;
    format!("vcan{pid:04}-{c:06}")
}
