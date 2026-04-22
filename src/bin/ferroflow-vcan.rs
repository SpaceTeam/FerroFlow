use std::ffi::OsStr;
use std::process::{Command, ExitCode};

fn usage() -> ! {
    eprintln!(
        "Usage:\n  ferroflow-vcan up [IFACE]\n  ferroflow-vcan down [IFACE]\n\nDefault IFACE is vcan0.\n\nThis helper is intended to be granted CAP_NET_ADMIN via setcap, e.g.:\n  sudo setcap cap_net_admin+ep target/release/ferroflow-vcan\n\nThen it can create/delete vcan interfaces without sudo.\n"
    );
    std::process::exit(2);
}

fn cmd_status<I, S>(args: I) -> std::io::Result<std::process::ExitStatus>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new("ip").args(args).status()
}

fn iface_exists(iface: &str) -> bool {
    cmd_status(["link", "show", iface]).is_ok_and(|s| s.success())
}

fn ensure_cap_net_admin_ambient() -> anyhow::Result<()> {
    use caps::{CapSet, Capability};

    // Must have CAP_NET_ADMIN in permitted set (provided by file capability).
    let permitted = caps::read(None, CapSet::Permitted)?;
    if !permitted.contains(&Capability::CAP_NET_ADMIN) {
        anyhow::bail!(
            "missing CAP_NET_ADMIN.\n\
             Fix (example): sudo setcap cap_net_admin+ep {}",
            std::env::current_exe()?.display()
        );
    }

    // Put CAP_NET_ADMIN into inheritable so we can raise it into ambient.
    let mut inheritable = caps::read(None, CapSet::Inheritable)?;
    if inheritable.insert(Capability::CAP_NET_ADMIN) {
        caps::set(None, CapSet::Inheritable, &inheritable)?;
    }

    // Raise ambient capability so it survives exec() into /sbin/ip.
    // SAFETY: prctl is called with documented constants.
    const PR_CAP_AMBIENT: libc::c_int = 47;
    const PR_CAP_AMBIENT_RAISE: libc::c_ulong = 2;

    let rc = unsafe {
        libc::prctl(
            PR_CAP_AMBIENT,
            PR_CAP_AMBIENT_RAISE,
            Capability::CAP_NET_ADMIN as libc::c_ulong,
            0,
            0,
        )
    };

    if rc != 0 {
        let err = std::io::Error::last_os_error();
        anyhow::bail!(
            "failed to raise ambient CAP_NET_ADMIN ({}).\n\
             You may need a newer kernel or to run as root.",
            err
        );
    }

    Ok(())
}

fn up(iface: &str) -> anyhow::Result<()> {
    ensure_cap_net_admin_ambient()?;

    // Try to load the vcan module; ignore errors (often needs extra privileges).
    let _ = Command::new("modprobe").arg("vcan").status();

    if !iface_exists(iface) {
        let st = cmd_status(["link", "add", "dev", iface, "type", "vcan"])?;
        if !st.success() {
            anyhow::bail!("failed to create vcan interface '{iface}'");
        }
    }

    let st = cmd_status(["link", "set", "up", iface])?;
    if !st.success() {
        anyhow::bail!("failed to bring up interface '{iface}'");
    }

    Ok(())
}

fn down(iface: &str) -> anyhow::Result<()> {
    ensure_cap_net_admin_ambient()?;

    if !iface_exists(iface) {
        return Ok(());
    }

    let st = cmd_status(["link", "del", iface])?;
    if !st.success() {
        anyhow::bail!("failed to delete interface '{iface}'");
    }

    Ok(())
}

fn main() -> ExitCode {
    // Avoid pulling clap just for this helper.
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        usage();
    };
    let iface = args.next().unwrap_or_else(|| "vcan0".to_string());

    let res = match cmd.as_str() {
        "up" => up(&iface),
        "down" => down(&iface),
        _ => {
            usage();
        }
    };

    if let Err(e) = res {
        eprintln!("{e:#}");
        return ExitCode::from(1);
    }

    ExitCode::SUCCESS
}
