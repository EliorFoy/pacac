//! Operating-system proxy detection.
//!
//! `server.upstream_proxy: "system"` makes pacac use the proxy configured in
//! the OS (Windows registry / environment variables). On Windows this is what
//! most users behind Clash, v2rayN or a corporate proxy expect: the browsers
//! already work, so the mirror should too.

/// Read the proxy configured in the OS, if any.
#[cfg(windows)]
pub fn detect() -> Option<String> {
    use winreg::enums::{HKEY_CURRENT_USER, KEY_READ};
    use winreg::RegKey;

    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let key = hkcu
        .open_subkey_with_flags(
            r"Software\Microsoft\Windows\CurrentVersion\Internet Settings",
            KEY_READ,
        )
        .ok()?;

    let enabled: u32 = key.get_value("ProxyEnable").unwrap_or(0);
    if enabled == 0 {
        return None;
    }
    let server: String = key.get_value("ProxyServer").ok()?;
    parse_proxy_server(&server)
}

#[cfg(not(windows))]
pub fn detect() -> Option<String> {
    for name in [
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        if let Ok(value) = std::env::var(name) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// Windows stores either `host:port` or a per-scheme list such as
/// `http=host:port;https=host:port`.
pub fn parse_proxy_server(server: &str) -> Option<String> {
    let server = server.trim();
    if server.is_empty() {
        return None;
    }

    let chosen = if server.contains('=') {
        let mut https = None;
        let mut http = None;
        for part in server.split(';') {
            let part = part.trim();
            if let Some(value) = part.strip_prefix("https=") {
                https = Some(value.trim().to_string());
            } else if let Some(value) = part.strip_prefix("http=") {
                http = Some(value.trim().to_string());
            }
        }
        https.or(http)?
    } else {
        server.to_string()
    };

    if chosen.is_empty() {
        return None;
    }
    if chosen.contains("://") {
        Some(chosen)
    } else {
        Some(format!("http://{chosen}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_host_port_gets_a_scheme() {
        assert_eq!(
            parse_proxy_server("127.0.0.1:7890").as_deref(),
            Some("http://127.0.0.1:7890")
        );
    }

    #[test]
    fn per_scheme_lists_prefer_https() {
        assert_eq!(
            parse_proxy_server("http=10.0.0.1:8080;https=10.0.0.1:8443").as_deref(),
            Some("http://10.0.0.1:8443")
        );
        assert_eq!(
            parse_proxy_server("http=10.0.0.1:8080").as_deref(),
            Some("http://10.0.0.1:8080")
        );
    }

    #[test]
    fn explicit_scheme_is_kept() {
        assert_eq!(
            parse_proxy_server("socks5://127.0.0.1:1080").as_deref(),
            Some("socks5://127.0.0.1:1080")
        );
    }

    #[test]
    fn empty_values_are_ignored() {
        assert_eq!(parse_proxy_server(""), None);
        assert_eq!(parse_proxy_server("   "), None);
        assert_eq!(parse_proxy_server("http=;https="), None);
    }
}
