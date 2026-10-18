//! Small helpers shared by the proxy, cache and downloader.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue};

/// Hop-by-hop headers (RFC 7230 §6.1) must not be forwarded by a proxy.
pub const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Format a unix timestamp as RFC 3339 (used by `/stats`).
pub fn format_unix(ts: i64) -> String {
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;
    OffsetDateTime::from_unix_timestamp(ts)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_else(|| ts.to_string())
}

/// Human readable byte size, e.g. `1.5 GB`.
pub fn format_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", n, UNITS[unit])
    } else {
        format!("{:.1} {}", value, UNITS[unit])
    }
}

/// Copy headers while skipping the given (case-insensitive) names.
pub fn copy_headers(src: &HeaderMap, skip: &[&str]) -> HeaderMap {
    let mut out = HeaderMap::with_capacity(src.len());
    for (name, value) in src.iter() {
        if skip.iter().any(|s| name.as_str().eq_ignore_ascii_case(s)) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

pub fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|h| name.eq_ignore_ascii_case(h))
}

pub fn header_value(s: &str) -> Option<HeaderValue> {
    HeaderValue::from_str(s).ok()
}

/// Best-effort LAN address of this machine.
///
/// Connecting a UDP socket performs a route lookup without sending a packet,
/// which is the portable way to find the address other machines should use.
pub fn local_ipv4() -> Option<std::net::IpAddr> {
    use std::net::{IpAddr, UdpSocket};

    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(IpAddr::V4(ip)),
        IpAddr::V6(ip) if !ip.is_loopback() && !ip.is_unspecified() => Some(IpAddr::V6(ip)),
        _ => None,
    }
}

/// A sorted, merged set of inclusive byte ranges.
///
/// Used for two things:
/// * tracking which parts of a partially downloaded file are already on disk
///   (resume), and
/// * telling a streaming reader how many bytes from its current offset are
///   contiguous and therefore safe to send.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeSet {
    ranges: Vec<(u64, u64)>,
}

impl RangeSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn ranges(&self) -> &[(u64, u64)] {
        &self.ranges
    }

    /// Number of bytes covered by the set.
    pub fn len(&self) -> u64 {
        self.ranges.iter().map(|&(s, e)| e - s + 1).sum()
    }

    pub fn insert(&mut self, start: u64, end: u64) {
        if end < start {
            return;
        }
        let mut new_start = start;
        let mut new_end = end;
        let mut out: Vec<(u64, u64)> = Vec::with_capacity(self.ranges.len() + 1);
        let mut placed = false;
        for &(s, e) in &self.ranges {
            if e.saturating_add(1) < new_start {
                out.push((s, e));
            } else if new_end.saturating_add(1) < s {
                if !placed {
                    out.push((new_start, new_end));
                    placed = true;
                }
                out.push((s, e));
            } else {
                new_start = new_start.min(s);
                new_end = new_end.max(e);
            }
        }
        if !placed {
            out.push((new_start, new_end));
        }
        out.sort_unstable();
        self.ranges = out;
    }

    /// Is `[start, end]` fully covered?
    pub fn covers(&self, start: u64, end: u64) -> bool {
        self.ranges.iter().any(|&(s, e)| s <= start && e >= end)
    }

    /// Exclusive end of the contiguous coverage starting at `pos`.
    ///
    /// Returns `pos` when no data at `pos` is available yet.
    pub fn end_from(&self, pos: u64) -> u64 {
        for &(s, e) in &self.ranges {
            if s <= pos && pos <= e {
                return e.saturating_add(1);
            }
        }
        pos
    }
}

impl FromIterator<(u64, u64)> for RangeSet {
    fn from_iter<T: IntoIterator<Item = (u64, u64)>>(iter: T) -> Self {
        let mut set = RangeSet::new();
        for (s, e) in iter {
            set.insert(s, e);
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_set_merges_adjacent() {
        let mut set = RangeSet::new();
        set.insert(0, 9);
        set.insert(10, 19);
        assert_eq!(set.ranges(), &[(0, 19)]);
        assert_eq!(set.len(), 20);
    }

    #[test]
    fn range_set_merges_out_of_order() {
        let mut set = RangeSet::new();
        set.insert(20, 29);
        set.insert(0, 9);
        assert_eq!(set.ranges(), &[(0, 9), (20, 29)]);
        set.insert(10, 19);
        assert_eq!(set.ranges(), &[(0, 29)]);
    }

    #[test]
    fn range_set_end_from_is_contiguous_only() {
        let mut set = RangeSet::new();
        set.insert(10, 19);
        assert_eq!(set.end_from(0), 0);
        assert_eq!(set.end_from(10), 20);
        assert_eq!(set.end_from(15), 20);
        set.insert(0, 9);
        assert_eq!(set.end_from(0), 20);
    }

    #[test]
    fn range_set_covers() {
        let mut set = RangeSet::new();
        set.insert(5, 15);
        assert!(set.covers(5, 15));
        assert!(set.covers(6, 14));
        assert!(!set.covers(4, 10));
        assert!(!set.covers(10, 16));
    }

    #[test]
    fn bytes_are_formatted() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }
}
