//! Machine `deviceId` resolution for order and FCO payloads.
//!
//! SSI V3 binds orders and conditional orders to a `deviceId`. Following the official V3 SDKs,
//! the id is read once per process from the operating system:
//!
//! - macOS: `IOPlatformUUID` via `/usr/sbin/ioreg -rd1 -c IOPlatformExpertDevice`
//! - Linux: `/etc/machine-id` or `/var/lib/dbus/machine-id`
//! - Windows: `HKLM\SOFTWARE\Microsoft\Cryptography` `MachineGuid`
//! - Fallback: host name, else `"unknown-host"`
//!
//! Callers can override the device id via [`crate::Config::with_device_id`] (useful in tests or
//! containers without a stable OS machine id).

use std::sync::OnceLock;

static MACHINE_ID: OnceLock<String> = OnceLock::new();

/// Returns the cached machine identifier for this host.
pub fn machine_id() -> &'static str {
    MACHINE_ID.get_or_init(resolve_machine_id)
}

fn resolve_machine_id() -> String {
    os_machine_id()
        .or_else(hostname_fallback)
        .unwrap_or_else(|| "unknown-host".to_owned())
}

#[cfg(target_os = "macos")]
fn os_machine_id() -> Option<String> {
    let output = std::process::Command::new("/usr/sbin/ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    parse_macos_uuid(&text)
}

#[cfg(target_os = "linux")]
fn os_machine_id() -> Option<String> {
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Ok(text) = std::fs::read_to_string(path) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn os_machine_id() -> Option<String> {
    let output = std::process::Command::new("reg")
        .args([
            "query",
            r"HKLM\SOFTWARE\Microsoft\Cryptography",
            "/v",
            "MachineGuid",
            "/reg:64",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    parse_windows_guid(&text)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
const fn os_machine_id() -> Option<String> {
    None
}

fn hostname_fallback() -> Option<String> {
    for key in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    None
}

#[cfg(any(target_os = "macos", test))]
fn parse_macos_uuid(output: &str) -> Option<String> {
    for line in output.lines() {
        if line.contains("IOPlatformUUID")
            && let Some((_, value)) = line.split_once('=')
        {
            let cleaned = value.trim().trim_matches('"').trim();
            if !cleaned.is_empty() {
                return Some(cleaned.to_owned());
            }
        }
    }
    None
}

#[cfg(any(target_os = "windows", test))]
fn parse_windows_guid(output: &str) -> Option<String> {
    for line in output.lines() {
        let mut fields = line.split_whitespace();
        if fields.next() == Some("MachineGuid")
            && let Some(value) = fields.nth(1)
            && !value.is_empty()
        {
            return Some(value.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_platform_outputs() {
        let mac = r#"
            +-o Root  <class IORegistryEntry>
              "IOPlatformUUID" = "12345678-90AB-CDEF-1234-567890ABCDEF"
        "#;
        assert_eq!(
            parse_macos_uuid(mac).as_deref(),
            Some("12345678-90AB-CDEF-1234-567890ABCDEF")
        );

        let win = "\nHKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography\n    MachineGuid    REG_SZ    aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\n";
        assert_eq!(
            parse_windows_guid(win).as_deref(),
            Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        );
    }

    #[test]
    fn machine_id_is_non_empty() {
        assert_ne!(machine_id(), "");
    }
}
