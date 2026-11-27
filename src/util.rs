//! Small helpers shared across modules.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use md5::{Digest, Md5};

pub fn random_bytes(buf: &mut [u8]) {
    let mut f = File::open("/dev/urandom").expect("/dev/urandom must be readable");
    f.read_exact(buf).expect("reading /dev/urandom");
}

pub fn random_token(len: usize) -> String {
    use base64::Engine;
    let mut b = vec![0u8; len];
    random_bytes(&mut b);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

/// MD5 of a file as lowercase hex (Drive's `md5Checksum` format).
/// Reads with a modest buffer; runs on a blocking thread.
pub fn md5_file(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut h = Md5::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex(&h.finalize()))
}

pub fn md5_bytes(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex(&h.finalize())
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn system_time_to_ns(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

pub fn ns_to_system_time(ns: i64) -> SystemTime {
    if ns >= 0 {
        UNIX_EPOCH + Duration::from_nanos(ns as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos((-ns) as u64)
    }
}

pub fn rfc3339_to_ns(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s).ok().and_then(|d| d.timestamp_nanos_opt())
}

pub fn ns_to_rfc3339(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    // Drive stores millisecond precision.
    let dt = chrono::DateTime::from_timestamp(secs, nanos - nanos % 1_000_000)
        .unwrap_or_default();
    dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// "report.docx" -> "report (conflicted copy host 2026-11-27 1015).docx"
pub fn conflict_name(name: &str, host: &str) -> String {
    let stamp = chrono::Local::now().format("%Y-%m-%d %H%M%S");
    let (stem, ext) = split_ext(name);
    format!("{stem} (conflicted copy {host} {stamp}){ext}")
}

/// Splits "a.tar.gz" into ("a.tar", ".gz"); dotfiles keep their name as stem.
pub fn split_ext(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(i) if i > 0 => (&name[..i], &name[i..]),
        _ => (name, ""),
    }
}

pub fn hostname() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "linux".into())
}

/// Drive names may contain '/', which is impossible locally. Use the same
/// substitution as rclone (FULLWIDTH SOLIDUS) so migrated trees match.
pub fn remote_to_local_name(name: &str) -> String {
    name.replace('/', "／").replace('\0', "")
}

pub fn local_to_remote_name(name: &str) -> String {
    name.replace('／', "/")
}

pub fn human_bytes(n: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// Lower our CPU and I/O priority so syncing never competes with the user.
pub fn lower_priority(nice: i32) {
    let nice = nice.clamp(0, 19);
    if nice == 0 {
        return;
    }
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, nice);
        // ioprio_set(IOPRIO_WHO_PROCESS, 0, IOPRIO_PRIO_VALUE(BE, 7))
        const IOPRIO_CLASS_SHIFT: i32 = 13;
        const IOPRIO_CLASS_BE: i32 = 2;
        const IOPRIO_WHO_PROCESS: i32 = 1;
        let prio = (IOPRIO_CLASS_BE << IOPRIO_CLASS_SHIFT) | 7;
        libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, prio);
    }
}

/// Free bytes on the filesystem holding `path`.
pub fn free_space(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_split() {
        assert_eq!(split_ext("a.tar.gz"), ("a.tar", ".gz"));
        assert_eq!(split_ext(".bashrc"), (".bashrc", ""));
        assert_eq!(split_ext("README"), ("README", ""));
    }

    #[test]
    fn conflict_names() {
        let n = conflict_name("report.docx", "box");
        assert!(n.starts_with("report (conflicted copy box "));
        assert!(n.ends_with(").docx"));
    }

    #[test]
    fn time_round_trip() {
        let ns = rfc3339_to_ns("2024-05-01T10:20:30.123Z").unwrap();
        assert_eq!(ns_to_rfc3339(ns), "2024-05-01T10:20:30.123Z");
    }

    #[test]
    fn md5_known() {
        assert_eq!(md5_bytes(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn slash_names() {
        assert_eq!(remote_to_local_name("a/b"), "a／b");
        assert_eq!(local_to_remote_name("a／b"), "a/b");
    }
}
