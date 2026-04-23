use clap::{Parser, ValueEnum};
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::style::{Attribute, Print, SetAttribute};
use crossterm::terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use std::mem;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ── SocketCAN constants & structs ──────────────────────────────────────────

const AF_CAN: i32 = 29;
const PF_CAN: i32 = AF_CAN;
const CAN_RAW: i32 = 1;
// libc::Ioctl is c_ulong on glibc, c_int on musl — use the alias for portability.
const SIOCGIFINDEX: libc::Ioctl = 0x8933 as libc::Ioctl;
const SOL_CAN_RAW: libc::c_int = 101;
const CAN_RAW_FD_FRAMES: libc::c_int = 5;

const CAN_EFF_FLAG: u32 = 0x8000_0000;
const CAN_RTR_FLAG: u32 = 0x4000_0000;
const CAN_ERR_FLAG: u32 = 0x2000_0000;
const CAN_EFF_MASK: u32 = 0x1FFF_FFFF;
const CAN_SFF_MASK: u32 = 0x0000_07FF;

// CAN-FD frame flags (in canfd_frame.flags)
const CANFD_BRS: u8 = 0x01;
const CANFD_ESI: u8 = 0x02;

// ECUconnect Logger protocol flags
const FRAME_FLAG_EXTENDED: u8 = 1 << 0;
const FRAME_FLAG_FD: u8 = 1 << 1;
const FRAME_FLAG_BRS: u8 = 1 << 2;
const FRAME_FLAG_ESI: u8 = 1 << 3;

const CANFD_FRAME_SIZE: usize = 72;
const CAN_FRAME_SIZE: usize = 16;

// SO_TIMESTAMPING flags (from linux/net_tstamp.h)
const SOF_TIMESTAMPING_SOFTWARE: u32 = 1 << 4;
const SOF_TIMESTAMPING_RX_HARDWARE: u32 = 1 << 2;
const SOF_TIMESTAMPING_RX_SOFTWARE: u32 = 1 << 3;
const SOF_TIMESTAMPING_RAW_HARDWARE: u32 = 1 << 6;

const AUTO_PORT_START: u16 = 42420;

#[repr(C)]
#[derive(Clone, Copy)]
struct CanFdFrame {
    can_id: u32,
    len: u8,
    flags: u8,
    __res0: u8,
    __res1: u8,
    data: [u8; 64],
}

#[repr(C)]
struct SockaddrCan {
    can_family: libc::sa_family_t,
    can_ifindex: libc::c_int,
    can_addr: [u8; 8],
}

#[repr(C)]
struct Ifreq {
    ifr_name: [u8; libc::IFNAMSIZ],
    ifr_ifindex: libc::c_int,
}

// ── Parsed CAN frame ──────────────────────────────────────────────────────

#[derive(Clone)]
struct RxFrame {
    can_id: u32,
    is_extended: bool,
    is_fd: bool,
    brs: bool,
    esi: bool,
    len: usize,
    data: [u8; 64],
    timestamp_us: u64,
}

// ── CLI ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TimestampMode {
    /// Absolute time (HH:MM:SS.mmm)
    Absolute,
    /// Delta since last frame
    Delta,
    /// No timestamp
    None,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Theme {
    /// Auto-detect from terminal background color (OSC 11, then COLORFGBG)
    Auto,
    /// Force the light-background palette
    Light,
    /// Force the dark-background palette
    Dark,
}

/// Enhanced candump for SocketCAN — rich colors, interactive scrollback/search,
/// auto theme detection. Optional CANcorder logger.
///
/// Reads CAN/CAN-FD frames from a SocketCAN interface and displays them with
/// stable per-ID colors, heat-mapped data bytes, and an interactive alternate-
/// screen viewer with scrollback, search, and visual-mode clipboard yank.
/// Pass --serve to also forward traffic to CANcorder clients via the
/// ECUconnect Logger binary protocol over TCP.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// CAN interface name (e.g. vcan0, can0)
    interface: String,

    /// Timestamp display mode
    #[arg(short = 't', long, value_enum, default_value_t = TimestampMode::Absolute)]
    timestamp: TimestampMode,

    /// Disable colored terminal output
    #[arg(long)]
    no_color: bool,

    /// Theme for the color palette: auto (query terminal), light, or dark
    #[arg(long, value_enum, default_value_t = Theme::Auto)]
    theme: Theme,

    /// Suppress terminal frame display (TCP forwarding only)
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Interactive terminal UI with scrollback and search
    #[arg(long, conflicts_with = "quiet")]
    interactive: bool,

    /// Write a candump-compatible log file on a background thread
    ///
    /// If PATH is omitted, uses candump-YYYY-MM-DD_HHMMSS.log in the current directory.
    #[arg(short = 'f', long = "log-file", value_name = "PATH", num_args = 0..=1)]
    log_file: Option<Option<PathBuf>>,

    /// Enable the CANcorder logger: bind a TCP server and announce it via
    /// Zeroconf so CANcorder clients can connect. Off by default — without
    /// this flag mcandump behaves like candump with no network side effects.
    #[arg(long)]
    serve: bool,

    /// Custom Zeroconf service name (default: "ECUconnect-Logger <hostname>:<port>"). Implies --serve.
    #[arg(long)]
    service_name: Option<String>,
}

// ── Theme detection ───────────────────────────────────────────────────────

/// Detect whether the terminal has a light background.  Returns `Some(true)`
/// for light, `Some(false)` for dark, or `None` if we can't tell.
///
/// First tries OSC 11 (`ESC ] 11 ; ? ESC \\` — the terminal answers with its
/// background color).  Falls back to parsing `$COLORFGBG` (older xterm-family
/// convention).  Returns `None` when neither is available; the caller picks a
/// default.
fn detect_is_light_theme() -> Option<bool> {
    if let Some(rgb) = query_bg_via_osc11() {
        return Some(is_light_rgb(rgb));
    }
    detect_via_colorfgbg()
}

fn is_light_rgb((r, g, b): (u8, u8, u8)) -> bool {
    let lum = 0.2126 * (r as f32 / 255.0)
        + 0.7152 * (g as f32 / 255.0)
        + 0.0722 * (b as f32 / 255.0);
    lum > 0.5
}

fn detect_via_colorfgbg() -> Option<bool> {
    let val = std::env::var("COLORFGBG").ok()?;
    // Format is "fg;bg" or "fg;default;bg" — take the last ';'-separated token.
    let bg: u32 = val.rsplit(';').next()?.parse().ok()?;
    // ANSI indices 0–6 are the dark half of the 16-color table, 7–15 the light.
    Some(bg >= 7)
}

fn query_bg_via_osc11() -> Option<(u8, u8, u8)> {
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::time::{Duration, Instant};

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let stdin_fd = stdin.as_raw_fd();
    let stdout_fd = stdout.as_raw_fd();
    if unsafe { libc::isatty(stdin_fd) } == 0 || unsafe { libc::isatty(stdout_fd) } == 0 {
        return None;
    }

    let mut orig: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(stdin_fd, &mut orig) } != 0 {
        return None;
    }
    let mut raw = orig;
    raw.c_lflag &= !(libc::ICANON | libc::ECHO);
    raw.c_cc[libc::VMIN] = 0;
    raw.c_cc[libc::VTIME] = 2; // 200 ms per read
    if unsafe { libc::tcsetattr(stdin_fd, libc::TCSANOW, &raw) } != 0 {
        return None;
    }

    // Restore termios on any exit path.
    struct RestoreTermios {
        fd: i32,
        orig: libc::termios,
    }
    impl Drop for RestoreTermios {
        fn drop(&mut self) {
            unsafe { libc::tcsetattr(self.fd, libc::TCSANOW, &self.orig) };
        }
    }
    let _restore = RestoreTermios { fd: stdin_fd, orig };

    {
        let mut out = stdout.lock();
        out.write_all(b"\x1b]11;?\x07").ok()?;
        out.flush().ok()?;
    }

    let deadline = Instant::now() + Duration::from_millis(500);
    let mut buf = Vec::with_capacity(64);
    let mut stdin = stdin.lock();
    while Instant::now() < deadline {
        let mut chunk = [0u8; 32];
        match stdin.read(&mut chunk) {
            Ok(0) => continue,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                // Response ends with BEL (0x07) or ST (ESC \\).
                if buf.contains(&0x07) || buf.windows(2).any(|w| w == [0x1b, b'\\']) {
                    break;
                }
            }
            Err(_) => continue,
        }
    }

    parse_osc11_response(&buf)
}

fn parse_osc11_response(buf: &[u8]) -> Option<(u8, u8, u8)> {
    let s = std::str::from_utf8(buf).ok()?;
    let payload_start = s.find("rgb:")? + 4;
    let rest = &s[payload_start..];
    let end = rest.find(['\x07', '\x1b']).unwrap_or(rest.len());
    let mut parts = rest[..end].split('/');
    let r = parse_osc11_channel(parts.next()?)?;
    let g = parse_osc11_channel(parts.next()?)?;
    let b = parse_osc11_channel(parts.next()?)?;
    Some((r, g, b))
}

fn parse_osc11_channel(s: &str) -> Option<u8> {
    let hex: String = s.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
    if hex.is_empty() {
        return None;
    }
    let v = u32::from_str_radix(&hex, 16).ok()?;
    // Terminals return 8-bit (RR), 12-bit (RRR), or 16-bit (RRRR) channels.
    let scaled = match hex.len() {
        1 => v * 0x11,
        2 => v,
        3 => v >> 4,
        4 => v >> 8,
        _ => v >> ((hex.len() - 2) * 4),
    };
    Some(scaled.min(255) as u8)
}

// ── ANSI color helpers ────────────────────────────────────────────────────

struct Colors {
    enabled: bool,
    is_light: bool,
}

impl Colors {
    fn new(enabled: bool, is_light: bool) -> Self {
        Self { enabled, is_light }
    }

    fn paint(&self, text: &str, code: &str) -> String {
        if self.enabled {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    fn tag(&self, label: &str, code: &str) -> String {
        self.paint(&format!("[{label}]"), code)
    }

    /// Pick a stable color for a CAN ID so the same ECU always appears in the
    /// same hue — makes it easy to visually track individual senders in scrolling
    /// traffic.  Palette avoids red (reserved for 0xFF data bytes) and dim gray
    /// (reserved for 0x00).  A theme-specific variant replaces bright cyan /
    /// yellow / green on light backgrounds, where those hues clash with
    /// paper-colored terminal schemes.
    fn id_color(&self, can_id: u32) -> &'static str {
        const PALETTE_DARK: &[&str] = &[
            "1;36", "1;32", "1;33", "1;35", "1;34",
            "36",   "32",   "33",   "35",   "34",
        ];
        const PALETTE_LIGHT: &[&str] = &[
            "38;5;18",  // DarkBlue
            "38;5;22",  // DarkGreen
            "38;5;90",  // DarkMagenta
            "38;5;130", // DarkOrange3
            "38;5;24",  // DeepSkyBlue4a
            "38;5;94",  // Orange4 (brown)
            "38;5;54",  // Purple4a
            "38;5;23",  // DarkCyan
            "38;5;58",  // Yellow4 (olive)
            "38;5;26",  // DeepSkyBlue4c
        ];
        let palette = if self.is_light { PALETTE_LIGHT } else { PALETTE_DARK };
        let idx = ((can_id as u64).wrapping_mul(2654435761) % palette.len() as u64) as usize;
        palette[idx]
    }

    /// Color a data byte by value — a heat-map that makes patterns jump out.
    /// The light-theme palette uses saturated dark 256-color indices so the
    /// gradient stays legible on paper-colored backgrounds.
    fn data_byte(&self, b: u8) -> String {
        if !self.enabled {
            return format!("{b:02X}");
        }
        let code = if self.is_light {
            match b {
                0x00 => "38;5;246",        // grey58
                0x01..=0x1F => "38;5;24",  // DeepSkyBlue4a
                0x20..=0x7E => "38;5;22",  // DarkGreen
                0x7F..=0xBF => "38;5;130", // DarkOrange3
                0xC0..=0xFE => "38;5;88",  // DarkRed
                0xFF => "1;38;5;124",      // bold Red3a
            }
        } else {
            match b {
                0x00 => "2;37",      // dim white (gray)
                0x01..=0x1F => "36", // cyan
                0x20..=0x7E => "32", // green
                0x7F..=0xBF => "33", // yellow
                0xC0..=0xFE => "31", // red
                0xFF => "1;31",      // bold red
            }
        };
        format!("\x1b[{code}m{b:02X}\x1b[0m")
    }

    fn ascii_char(&self, b: u8) -> String {
        if !self.enabled {
            let ch = if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            };
            return format!("{ch}");
        }
        if b.is_ascii_graphic() || b == b' ' {
            format!("\x1b[{}m{}\x1b[0m", self.ascii_printable_code(), b as char)
        } else {
            format!("\x1b[{}m.\x1b[0m", self.ascii_nonprintable_code())
        }
    }

    // Theme-aware SGR code snippets for callers that write escape sequences
    // directly (format_frame_interactive, format_frame).  Dark-theme variants
    // use the `2` (faint) attribute, which WezTerm and friends render by
    // blending the foreground toward the background — unreadable on paper-
    // colored themes, so the light variants use explicit 256-color grays /
    // dark hues instead.
    fn dim_code(&self) -> &'static str {
        if self.is_light { "38;5;240" } else { "2" }
    }
    fn iface_code(&self) -> &'static str {
        if self.is_light { "38;5;24" } else { "2;34" }
    }
    fn fd_code(&self) -> &'static str {
        if self.is_light { "38;5;91" } else { "2;35" }
    }
    fn data_interactive_code(&self) -> &'static str {
        if self.is_light { "38;5;24" } else { "36" }
    }
    fn ascii_printable_code(&self) -> &'static str {
        if self.is_light { "38;5;22" } else { "32" }
    }
    fn ascii_nonprintable_code(&self) -> &'static str {
        if self.is_light { "38;5;248" } else { "2;37" }
    }
    fn dlc_code(&self) -> &'static str {
        if self.is_light { "38;5;244" } else { "38;5;245" }
    }
}

// ── Socket helpers ────────────────────────────────────────────────────────

/// Open a CAN socket. Returns `(fd, hw_timestamps)` where `hw_timestamps`
/// is true if hardware timestamping was successfully enabled.
fn open_can_socket(ifname: &str, enable_fd: bool) -> io::Result<(i32, bool)> {
    unsafe {
        let fd = libc::socket(PF_CAN, libc::SOCK_RAW, CAN_RAW);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // Resolve interface index
        let mut ifr: Ifreq = mem::zeroed();
        let name_bytes = ifname.as_bytes();
        if name_bytes.len() >= libc::IFNAMSIZ {
            libc::close(fd);
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interface name too long",
            ));
        }
        ifr.ifr_name[..name_bytes.len()].copy_from_slice(name_bytes);

        if libc::ioctl(fd, SIOCGIFINDEX, &mut ifr as *mut Ifreq) < 0 {
            let e = io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }

        // Enable CAN-FD reception
        if enable_fd {
            let enable: libc::c_int = 1;
            if libc::setsockopt(
                fd,
                SOL_CAN_RAW,
                CAN_RAW_FD_FRAMES,
                &enable as *const libc::c_int as *const libc::c_void,
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            ) < 0
            {
                // Non-fatal: kernel may not support FD on this interface
                eprintln!(
                    "warning: could not enable CAN-FD reception: {}",
                    io::Error::last_os_error()
                );
            }
        }

        let mut addr: SockaddrCan = mem::zeroed();
        addr.can_family = AF_CAN as libc::sa_family_t;
        addr.can_ifindex = ifr.ifr_ifindex;

        if libc::bind(
            fd,
            &addr as *const SockaddrCan as *const libc::sockaddr,
            mem::size_of::<SockaddrCan>() as libc::socklen_t,
        ) < 0
        {
            let e = io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }

        // Increase receive buffer for high bus loads
        let rcvbuf: libc::c_int = 1 << 20; // 1 MiB
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &rcvbuf as *const libc::c_int as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        );

        // Request hardware timestamps if available, otherwise kernel timestamps.
        let ts_flags: u32 = SOF_TIMESTAMPING_RX_HARDWARE
            | SOF_TIMESTAMPING_RX_SOFTWARE
            | SOF_TIMESTAMPING_SOFTWARE
            | SOF_TIMESTAMPING_RAW_HARDWARE;
        let got_timestamping = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPING,
            &ts_flags as *const u32 as *const libc::c_void,
            mem::size_of::<u32>() as libc::socklen_t,
        ) == 0;

        if !got_timestamping {
            // Fallback: kernel nanosecond software timestamps
            let enable: libc::c_int = 1;
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPNS,
                &enable as *const libc::c_int as *const libc::c_void,
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        // Set receive timeout so the read loop can check the stop flag
        let tv = libc::timeval {
            tv_sec: 0,
            tv_usec: 500_000, // 500ms
        };
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            mem::size_of::<libc::timeval>() as libc::socklen_t,
        );

        Ok((fd, got_timestamping))
    }
}

/// Read a single CAN or CAN-FD frame from the socket.
///
/// Returns `None` for error/RTR frames (silently skipped) or on timeout.
fn read_frame(fd: i32) -> io::Result<Option<RxFrame>> {
    let mut buf = [0u8; CANFD_FRAME_SIZE];
    let mut cmsg_buf = [0u8; 256]; // room for ancillary timestamp data
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: CANFD_FRAME_SIZE,
    };
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len() as _;

    let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let n = n as usize;
    if n < CAN_FRAME_SIZE {
        return Ok(None); // runt frame
    }

    // Extract timestamp from ancillary data.  Prefer hardware timestamps
    // (SO_TIMESTAMPING with RAW_HARDWARE) over kernel software timestamps
    // (SO_TIMESTAMPNS).  Fall back to userspace SystemTime if neither is
    // available.
    let timestamp_us = extract_timestamp_us(&msg).unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64
    });

    // Both can_frame (16 bytes) and canfd_frame (72 bytes) share the same
    // first-8-byte layout: can_id(4) + len(1) + flags(1) + res(2).
    let frame: CanFdFrame = unsafe { std::ptr::read(buf.as_ptr() as *const CanFdFrame) };
    let raw_id = frame.can_id;

    // Skip error frames and remote transmission requests
    if raw_id & CAN_ERR_FLAG != 0 || raw_id & CAN_RTR_FLAG != 0 {
        return Ok(None);
    }

    let is_extended = raw_id & CAN_EFF_FLAG != 0;
    let can_id = if is_extended {
        raw_id & CAN_EFF_MASK
    } else {
        raw_id & CAN_SFF_MASK
    };

    let is_fd = n == CANFD_FRAME_SIZE;
    let max_len: usize = if is_fd { 64 } else { 8 };
    let len = (frame.len as usize).min(max_len);

    let brs = is_fd && (frame.flags & CANFD_BRS != 0);
    let esi = is_fd && (frame.flags & CANFD_ESI != 0);

    let mut data = [0u8; 64];
    data[..len].copy_from_slice(&frame.data[..len]);

    Ok(Some(RxFrame {
        can_id,
        is_extended,
        is_fd,
        brs,
        esi,
        len,
        data,
        timestamp_us,
    }))
}

/// Walk the cmsg chain from recvmsg and return the best available timestamp
/// in microseconds since the Unix epoch.
///
/// Priority: SO_TIMESTAMPING hardware → SO_TIMESTAMPING software → SO_TIMESTAMPNS.
fn extract_timestamp_us(msg: &libc::msghdr) -> Option<u64> {
    let mut best: Option<u64> = None;

    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(msg);
        while !cmsg.is_null() {
            let hdr = &*cmsg;
            if hdr.cmsg_level == libc::SOL_SOCKET {
                if hdr.cmsg_type == libc::SO_TIMESTAMPING {
                    // SO_TIMESTAMPING delivers 3 consecutive timespecs:
                    //   [0] software, [1] deprecated, [2] hardware
                    let data = libc::CMSG_DATA(cmsg) as *const libc::timespec;
                    let hw = &*data.add(2);
                    if hw.tv_sec != 0 || hw.tv_nsec != 0 {
                        return Some(hw.tv_sec as u64 * 1_000_000 + hw.tv_nsec as u64 / 1_000);
                    }
                    let sw = &*data;
                    if sw.tv_sec != 0 || sw.tv_nsec != 0 {
                        best = Some(sw.tv_sec as u64 * 1_000_000 + sw.tv_nsec as u64 / 1_000);
                    }
                } else if hdr.cmsg_type == libc::SO_TIMESTAMPNS && best.is_none() {
                    let ts = &*(libc::CMSG_DATA(cmsg) as *const libc::timespec);
                    best = Some(ts.tv_sec as u64 * 1_000_000 + ts.tv_nsec as u64 / 1_000);
                }
            }
            cmsg = libc::CMSG_NXTHDR(msg, cmsg);
        }
    }

    best
}

// ── ECUconnect Logger binary protocol ─────────────────────────────────────

fn pack_ecuconnect_frame(frame: &RxFrame) -> Vec<u8> {
    let mut flags: u8 = 0;
    if frame.is_extended {
        flags |= FRAME_FLAG_EXTENDED;
    }
    if frame.is_fd {
        flags |= FRAME_FLAG_FD;
        if frame.brs {
            flags |= FRAME_FLAG_BRS;
        }
        if frame.esi {
            flags |= FRAME_FLAG_ESI;
        }
    }

    // Header: timestamp(8) + can_id(4) + flags(1) + dlc(1) = 14 bytes
    let mut pkt = Vec::with_capacity(14 + frame.len);
    pkt.extend_from_slice(&frame.timestamp_us.to_be_bytes());
    pkt.extend_from_slice(&frame.can_id.to_be_bytes());
    pkt.push(flags);
    pkt.push(frame.len as u8);
    pkt.extend_from_slice(&frame.data[..frame.len]);
    pkt
}

fn format_candump_frame(frame: &RxFrame) -> String {
    let id = if frame.is_extended {
        format!("{:08X}", frame.can_id)
    } else {
        format!("{:03X}", frame.can_id)
    };

    let mut out = String::with_capacity(16 + frame.len * 2);
    out.push_str(&id);
    out.push('#');

    if frame.is_fd {
        let mut flags = 0u8;
        if frame.brs {
            flags |= CANFD_BRS;
        }
        if frame.esi {
            flags |= CANFD_ESI;
        }
        out.push('#');
        out.push(char::from(b"0123456789ABCDEF"[(flags & 0x0F) as usize]));
    }

    for &byte in &frame.data[..frame.len] {
        out.push_str(&format!("{byte:02X}"));
    }

    out
}

fn format_candump_log_line(frame: &RxFrame, iface: &str) -> String {
    let seconds = frame.timestamp_us / 1_000_000;
    let micros = frame.timestamp_us % 1_000_000;
    format!(
        "({seconds}.{micros:06}) {iface} {}",
        format_candump_frame(frame)
    )
}

fn run_log_writer(rx: mpsc::Receiver<RxFrame>, iface: String, file: File) -> io::Result<()> {
    unsafe {
        libc::nice(10);
    }

    let mut writer = io::BufWriter::new(file);
    for frame in rx {
        writeln!(writer, "{}", format_candump_log_line(&frame, &iface))?;
    }
    writer.flush()
}

fn resolve_log_file_path(arg: Option<Option<PathBuf>>) -> Option<PathBuf> {
    match arg {
        None => None,
        Some(Some(path)) => Some(path),
        Some(None) => Some(PathBuf::from(default_candump_log_filename())),
    }
}

fn default_candump_log_filename() -> String {
    unsafe {
        let now = libc::time(std::ptr::null_mut());
        let mut tm: libc::tm = mem::zeroed();
        if libc::localtime_r(&now, &mut tm).is_null() {
            return "candump.log".to_string();
        }

        format!(
            "candump-{year:04}-{month:02}-{day:02}_{hour:02}{minute:02}{second:02}.log",
            year = tm.tm_year + 1900,
            month = tm.tm_mon + 1,
            day = tm.tm_mday,
            hour = tm.tm_hour,
            minute = tm.tm_min,
            second = tm.tm_sec,
        )
    }
}

// ── TCP client management (per-client buffered channels) ──────────────────

struct ClientHandle {
    tx: mpsc::Sender<Arc<Vec<u8>>>,
    addr: String,
}

/// Shared counters between the ClientManager and per-client writer threads.
struct Stats {
    frames_sent: AtomicU64,
    frames_dropped: AtomicU64,
    bytes_sent: AtomicU64,
}

struct ClientManager {
    clients: Mutex<Vec<ClientHandle>>,
    stats: Arc<Stats>,
    colors: Colors,
    log_runtime: bool,
}

impl ClientManager {
    fn new(colors: Colors, log_runtime: bool) -> Self {
        Self {
            clients: Mutex::new(Vec::new()),
            stats: Arc::new(Stats {
                frames_sent: AtomicU64::new(0),
                frames_dropped: AtomicU64::new(0),
                bytes_sent: AtomicU64::new(0),
            }),
            colors,
            log_runtime,
        }
    }

    /// Register a new client: spawn a dedicated writer thread with an unbounded
    /// channel so a slow client never blocks the recording pipeline.
    fn add_client(&self, mut stream: TcpStream) {
        if let Err(e) = stream.set_nodelay(true) {
            eprintln!("warning: TCP_NODELAY failed: {e}");
        }
        let addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "?".into());

        let (tx, rx) = mpsc::channel::<Arc<Vec<u8>>>();
        let writer_addr = addr.clone();
        let stats = Arc::clone(&self.stats);
        let log_runtime = self.log_runtime;

        // Per-client writer thread — drains the unbounded channel and writes to
        // the TCP socket.  Blocking write_all here only stalls this one client.
        thread::Builder::new()
            .name(format!("client-{addr}"))
            .spawn(move || {
                for pkt in rx {
                    match stream.write_all(&pkt) {
                        Ok(()) => {
                            stats.frames_sent.fetch_add(1, Ordering::Relaxed);
                            stats
                                .bytes_sent
                                .fetch_add(pkt.len() as u64, Ordering::Relaxed);
                        }
                        Err(_) => {
                            stats.frames_dropped.fetch_add(1, Ordering::Relaxed);
                            break; // socket dead — exit writer loop
                        }
                    }
                }
                if log_runtime {
                    let tag = "\x1b[33m[server]\x1b[0m";
                    eprintln!(
                        "{} {tag} Client writer exited: {writer_addr}",
                        timestamp_now(),
                    );
                }
            })
            .expect("cannot spawn client writer thread");

        let mut clients = self.clients.lock().unwrap();
        clients.push(ClientHandle {
            tx,
            addr: addr.clone(),
        });
        if self.log_runtime {
            eprintln!(
                "{} {} Client connected: {addr} (total: {})",
                timestamp_now(),
                self.colors.tag("server", "32"),
                clients.len(),
            );
        }
    }

    /// Fan out a packed frame to every client's unbounded channel.
    /// This never blocks — the per-client writer threads drain independently.
    fn broadcast(&self, data: Arc<Vec<u8>>) {
        let mut clients = self.clients.lock().unwrap();
        let mut i = 0;
        while i < clients.len() {
            if clients[i].tx.send(Arc::clone(&data)).is_ok() {
                i += 1;
            } else {
                // Receiver dropped — writer thread has exited
                let dead = clients.swap_remove(i);
                if self.log_runtime {
                    eprintln!(
                        "{} {} Client disconnected: {} (total: {})",
                        timestamp_now(),
                        self.colors.tag("server", "33"),
                        dead.addr,
                        clients.len(),
                    );
                }
            }
        }
    }

    fn client_count(&self) -> usize {
        self.clients.lock().unwrap().len()
    }

    fn close_all(&self) {
        let mut clients = self.clients.lock().unwrap();
        // Dropping senders signals writer threads to exit.
        clients.clear();
    }
}

// ── Recording thread ──────────────────────────────────────────────────────

fn run_recorder(rx: mpsc::Receiver<RxFrame>, manager: Arc<ClientManager>) {
    for frame in rx {
        if manager.client_count() > 0 {
            let pkt = Arc::new(pack_ecuconnect_frame(&frame));
            manager.broadcast(pkt);
        }
    }
}

// ── Display thread (low priority) ─────────────────────────────────────────

fn run_display(rx: mpsc::Receiver<RxFrame>, iface: String, ts_mode: TimestampMode, colors: Colors) {
    // Lower scheduling priority so display never starves CAN reading or recording
    unsafe {
        libc::nice(10);
    }

    let stdout = io::stdout();
    let mut previous_timestamp_us = None;

    for frame in rx {
        let line = format_frame(&frame, &iface, ts_mode, previous_timestamp_us, &colors, true);
        previous_timestamp_us = Some(frame.timestamp_us);
        let mut out = stdout.lock();
        let _ = writeln!(out, "{line}");
    }
}

// ── Interactive display ───────────────────────────────────────────────────

#[derive(Clone)]
enum SearchQuery {
    Bytes(Vec<u8>),
    ArbitrationId(u32),
}

impl SearchQuery {
    fn label(&self) -> String {
        match self {
            SearchQuery::Bytes(bytes) => format!(
                "bytes {}",
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02X}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            SearchQuery::ArbitrationId(can_id) => format!("arbitration ID 0x{can_id:X}"),
        }
    }
}

#[derive(Clone, Copy)]
enum PromptKind {
    Bytes,
    ArbitrationId,
}

impl PromptKind {
    fn title(self) -> &'static str {
        match self {
            PromptKind::Bytes => "Find bytes (hex, e.g. DE AD BE EF)",
            PromptKind::ArbitrationId => "Find arbitration ID (hex)",
        }
    }
}

struct PromptState {
    kind: PromptKind,
    input: String,
}

#[derive(Clone, Copy)]
enum YankFormat {
    /// Full candump log line: `(sec.us) iface ID#DATA`
    Candump,
    /// Compact `ID#DATA` with no timestamp or interface.
    Hex,
}

impl YankFormat {
    fn label(self) -> &'static str {
        match self {
            YankFormat::Candump => "candump",
            YankFormat::Hex => "hex",
        }
    }
}

struct InteractiveState {
    frames: Vec<RxFrame>,
    selected: usize,
    follow_tail: bool,
    search: Option<SearchQuery>,
    status: String,
    prompt: Option<PromptState>,
    /// Tail pane size in rows (0 = not yet initialized, set on first split).
    tail_size: usize,
    /// When set, a vim-style visual selection is active and the range
    /// `[min(anchor, selected), max(anchor, selected)]` is highlighted.
    select_anchor: Option<usize>,
}

impl InteractiveState {
    fn new() -> Self {
        Self {
            frames: Vec::new(),
            selected: 0,
            follow_tail: true,
            search: None,
            status: String::new(),
            prompt: None,
            tail_size: 0,
            select_anchor: None,
        }
    }

    /// The inclusive range of selected frames. If no visual-mode anchor is
    /// set the "range" is just the cursor frame, so `y` always has something
    /// sensible to copy.
    fn selection_range(&self) -> Option<(usize, usize)> {
        if self.frames.is_empty() {
            return None;
        }
        let last = self.frames.len() - 1;
        let cursor = self.selected.min(last);
        let anchor = match self.select_anchor {
            Some(a) => a.min(last),
            None => return Some((cursor, cursor)),
        };
        Some((anchor.min(cursor), anchor.max(cursor)))
    }

    fn clear_buffer(&mut self) {
        let n = self.frames.len();
        self.frames.clear();
        self.selected = 0;
        self.follow_tail = true;
        self.select_anchor = None;
        self.status = format!("Cleared {n} frame{}.", if n == 1 { "" } else { "s" });
    }

    fn toggle_visual(&mut self) {
        if self.frames.is_empty() {
            self.status = "Nothing to select yet.".to_string();
            return;
        }
        if self.select_anchor.take().is_some() {
            self.status = "Selection cleared.".to_string();
        } else {
            self.follow_tail = false;
            self.select_anchor = Some(self.selected);
            self.status =
                "Visual mode · extend with arrows/PgUp/PgDn/n/N · y copy · Y hex · Esc cancel."
                    .to_string();
        }
    }

    fn yank_selection(&mut self, iface: &str, fmt: YankFormat) {
        let Some((a, b)) = self.selection_range() else {
            self.status = "Nothing to copy.".to_string();
            return;
        };
        let lines: Vec<String> = (a..=b)
            .map(|i| match fmt {
                YankFormat::Candump => format_candump_log_line(&self.frames[i], iface),
                YankFormat::Hex => format_candump_frame(&self.frames[i]),
            })
            .collect();
        self.deliver_yank(&lines, fmt, "Copied");
        self.select_anchor = None;
    }

    fn yank_all_matches(&mut self, iface: &str, fmt: YankFormat) {
        let Some(search) = self.search.clone() else {
            self.status = "No active search — press / or i first.".to_string();
            return;
        };
        let lines: Vec<String> = self
            .frames
            .iter()
            .filter(|frame| frame_matches(frame, &search))
            .map(|frame| match fmt {
                YankFormat::Candump => format_candump_log_line(frame, iface),
                YankFormat::Hex => format_candump_frame(frame),
            })
            .collect();
        if lines.is_empty() {
            self.status = format!("No matches for {} to copy.", search.label());
            return;
        }
        self.deliver_yank(&lines, fmt, "Copied matches for");
    }

    fn deliver_yank(&mut self, lines: &[String], fmt: YankFormat, prefix: &str) {
        let text: String = lines
            .iter()
            .flat_map(|line| [line.as_str(), "\n"])
            .collect();
        let count = lines.len();
        let bytes = text.len();
        match copy_to_clipboard_osc52(&text) {
            Ok(()) => {
                self.status = format!(
                    "{prefix} {count} frame{plural} ({bytes} bytes, {fmt_label}) → clipboard (OSC 52).",
                    plural = if count == 1 { "" } else { "s" },
                    fmt_label = fmt.label(),
                );
            }
            Err(err) => {
                self.status = format!("Clipboard copy failed: {err}");
            }
        }
    }

    fn push_frame(&mut self, frame: RxFrame) {
        self.frames.push(frame);
        if self.follow_tail {
            self.selected = self.frames.len().saturating_sub(1);
        }
    }

    fn scroll_up(&mut self, lines: usize) {
        if self.frames.is_empty() {
            return;
        }
        self.follow_tail = false;
        self.selected = self.selected.saturating_sub(lines);
    }

    fn scroll_down(&mut self, lines: usize) {
        if self.frames.is_empty() {
            return;
        }
        let last = self.frames.len().saturating_sub(1);
        self.selected = self.selected.saturating_add(lines).min(last);
        self.follow_tail = self.selected == last;
    }

    fn jump_top(&mut self) {
        if self.frames.is_empty() {
            return;
        }
        self.follow_tail = false;
        self.selected = 0;
    }

    fn jump_bottom(&mut self) {
        if self.frames.is_empty() {
            return;
        }
        self.follow_tail = true;
        self.selected = self.frames.len().saturating_sub(1);
    }

    fn start_prompt(&mut self, kind: PromptKind) {
        self.prompt = Some(PromptState {
            kind,
            input: String::new(),
        });
        self.status = kind.title().to_string();
    }

    fn repeat_search(&mut self, forward: bool) {
        let Some(search) = self.search.clone() else {
            self.status = "No active search.".to_string();
            return;
        };
        if self.find_match(&search, forward, false) {
            self.status = format!("Found {}.", search.label());
        } else {
            self.status = format!("No match for {}.", search.label());
        }
    }

    fn submit_prompt(&mut self) {
        let Some(prompt) = self.prompt.take() else {
            return;
        };

        let query = match prompt.kind {
            PromptKind::Bytes => parse_hex_bytes(&prompt.input).map(SearchQuery::Bytes),
            PromptKind::ArbitrationId => {
                parse_can_id(&prompt.input).map(SearchQuery::ArbitrationId)
            }
        };

        match query {
            Ok(search) => {
                let label = search.label();
                self.search = Some(search.clone());
                if self.find_match(&search, true, true) {
                    self.status = format!("Found {label}.");
                } else {
                    self.status = format!("No match for {label}.");
                }
            }
            Err(err) => {
                self.status = err;
            }
        }
    }

    fn find_match(&mut self, search: &SearchQuery, forward: bool, include_current: bool) -> bool {
        if self.frames.is_empty() {
            return false;
        }

        let len = self.frames.len();
        let current = self.selected.min(len.saturating_sub(1));
        let start = if include_current { 0 } else { 1 };

        for step in start..len {
            let idx = if forward {
                (current + step) % len
            } else {
                (current + len - (step % len)) % len
            };
            if frame_matches(&self.frames[idx], search) {
                self.selected = idx;
                self.follow_tail = idx + 1 == len;
                return true;
            }
        }

        false
    }

    fn handle_key(
        &mut self,
        key: KeyEvent,
        page_rows: usize,
        body_rows: usize,
        iface: &str,
        stop: &AtomicBool,
    ) {
        if let Some(prompt) = self.prompt.as_mut() {
            match key.code {
                KeyCode::Esc => {
                    self.prompt = None;
                    self.status = "Search cancelled.".to_string();
                }
                KeyCode::Enter => self.submit_prompt(),
                KeyCode::Backspace => {
                    prompt.input.pop();
                }
                KeyCode::Char(c)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    prompt.input.push(c);
                }
                _ => {}
            }
            return;
        }

        match key.code {
            KeyCode::Up if key.modifiers.contains(KeyModifiers::SHIFT) => {
                // Leave at least 3 rows for the main pane + 1 for separator
                let max_tail = body_rows.saturating_sub(4);
                self.tail_size = (self.tail_size + 1).min(max_tail);
            }
            KeyCode::Down if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.tail_size = self.tail_size.saturating_sub(1).max(3);
            }
            KeyCode::Up => self.scroll_up(1),
            KeyCode::Down => self.scroll_down(1),
            KeyCode::PageUp => self.scroll_up(page_rows.max(1)),
            KeyCode::PageDown => self.scroll_down(page_rows.max(1)),
            KeyCode::Home => self.jump_top(),
            KeyCode::End => self.jump_bottom(),
            KeyCode::Char('/') => self.start_prompt(PromptKind::Bytes),
            KeyCode::Char('i') => self.start_prompt(PromptKind::ArbitrationId),
            KeyCode::Char('n') => self.repeat_search(true),
            KeyCode::Char('N') => self.repeat_search(false),
            KeyCode::Char('v') => self.toggle_visual(),
            KeyCode::Char('y') => self.yank_selection(iface, YankFormat::Candump),
            KeyCode::Char('Y') => self.yank_selection(iface, YankFormat::Hex),
            KeyCode::Char('V') => self.yank_all_matches(iface, YankFormat::Candump),
            KeyCode::Esc => {
                if self.select_anchor.take().is_some() {
                    self.status = "Selection cancelled.".to_string();
                }
            }
            KeyCode::Char('q') => {
                self.status = "Exiting interactive mode...".to_string();
                stop.store(true, Ordering::SeqCst);
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.status = "Exiting interactive mode...".to_string();
                stop.store(true, Ordering::SeqCst);
            }
            KeyCode::Char('c') => self.clear_buffer(),
            _ => {}
        }
    }

    fn status_line(&self, iface: &str) -> String {
        let frames = self.frames.len();
        let position = if frames == 0 {
            "0/0".to_string()
        } else {
            format!("{}/{}", self.selected + 1, frames)
        };
        let mode = if self.select_anchor.is_some() {
            "visual"
        } else if self.follow_tail {
            "live"
        } else {
            "browsing"
        };
        let selection = match (self.select_anchor, self.selection_range()) {
            (Some(_), Some((a, b))) => format!("  sel: {}", b - a + 1),
            _ => String::new(),
        };
        let search = self
            .search
            .as_ref()
            .map(|search| format!("  search: {}", search.label()))
            .unwrap_or_default();
        let prefix = if self.status.is_empty() {
            String::new()
        } else {
            format!("{}  ", self.status)
        };
        format!("{prefix}pos: {position}  {iface}: {mode}{selection}{search}")
    }
}

struct InteractiveTerminal {
    stdout: io::Stdout,
}

impl InteractiveTerminal {
    fn enter() -> io::Result<Self> {
        let mut stdout = io::stdout();
        terminal::enable_raw_mode()?;
        if let Err(err) = execute!(stdout, EnterAlternateScreen, Hide) {
            let _ = terminal::disable_raw_mode();
            return Err(err);
        }
        Ok(Self { stdout })
    }
}

impl Drop for InteractiveTerminal {
    fn drop(&mut self) {
        let _ = execute!(self.stdout, Show, LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

fn run_interactive_display(
    rx: mpsc::Receiver<RxFrame>,
    iface: String,
    ts_mode: TimestampMode,
    colors: Colors,
    stop: Arc<AtomicBool>,
) {
    unsafe {
        libc::nice(10);
    }

    let mut terminal = match InteractiveTerminal::enter() {
        Ok(terminal) => terminal,
        Err(err) => {
            eprintln!("warning: interactive mode unavailable: {err}");
            run_display(rx, iface, ts_mode, colors);
            return;
        }
    };

    let mut state = InteractiveState::new();
    let tick = Duration::from_millis(50);
    let mut needs_redraw = true;

    loop {
        loop {
            match rx.try_recv() {
                Ok(frame) => {
                    state.push_frame(frame);
                    needs_redraw = true;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => return,
            }
        }

        if needs_redraw {
            if let Err(err) =
                draw_interactive(&mut terminal.stdout, &mut state, &iface, ts_mode, &colors)
            {
                state.status = format!("render error: {err}");
            } else {
                needs_redraw = false;
            }
        }

        if stop.load(Ordering::Relaxed) {
            break;
        }

        match event::poll(tick) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) => {
                    let rows = terminal::size().map(|(_, rows)| rows).unwrap_or(24) as usize;
                    let body_rows = rows.saturating_sub(2).max(1);
                    let tail_rows = if !state.follow_tail && body_rows >= 12 {
                        if state.tail_size == 0 {
                            body_rows / 5
                        } else {
                            state.tail_size
                        }
                    } else {
                        0
                    };
                    let sep_rows = if tail_rows > 0 { 1 } else { 0 };
                    let page_rows = body_rows - tail_rows - sep_rows;
                    state.handle_key(key, page_rows, body_rows, &iface, &stop);
                    needs_redraw = true;
                }
                Ok(Event::Resize(_, _)) => needs_redraw = true,
                Ok(_) => {}
                Err(err) => {
                    state.status = format!("input error: {err}");
                    needs_redraw = true;
                    thread::sleep(Duration::from_millis(100));
                }
            },
            Ok(false) => {}
            Err(err) => {
                state.status = format!("poll error: {err}");
                needs_redraw = true;
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn draw_interactive(
    stdout: &mut io::Stdout,
    state: &mut InteractiveState,
    iface: &str,
    ts_mode: TimestampMode,
    colors: &Colors,
) -> io::Result<()> {
    let (cols, rows) = terminal::size()?;
    let plain = Colors::new(false, false);

    // Fixed chrome: help bar + status bar = 2 rows.
    // When scrolled away, add a separator + tail pane (~20% of body).
    let body_rows = rows.saturating_sub(2).max(1) as usize;

    let tail_rows = if !state.follow_tail && body_rows >= 12 {
        // Initialize tail size on first split appearance
        if state.tail_size == 0 {
            state.tail_size = (body_rows / 5).max(3);
        }
        state.tail_size
    } else {
        0
    };
    let sep_rows = if tail_rows > 0 { 1 } else { 0 };
    let main_rows = body_rows - tail_rows - sep_rows;

    let help_row = body_rows as u16;
    let status_row = help_row + 1;

    let total = state.frames.len();
    let selected = if total == 0 {
        0
    } else {
        state.selected.min(total.saturating_sub(1))
    };

    // Viewport for the main (scrollable) pane
    let top = if total <= main_rows {
        0
    } else if state.follow_tail {
        total.saturating_sub(main_rows)
    } else {
        let half = main_rows / 2;
        let mut top = selected.saturating_sub(half);
        let max_top = total.saturating_sub(main_rows);
        if top > max_top {
            top = max_top;
        }
        top
    };

    queue!(stdout, MoveTo(0, 0), Clear(ClearType::All))?;

    let selection_range = state.selection_range();

    // ── Main pane (scrollable, colored) ──────────────────────────────────
    for row in 0..main_rows {
        let idx = top + row;
        queue!(stdout, MoveTo(0, row as u16))?;
        if idx < total {
            let previous_timestamp_us = idx
                .checked_sub(1)
                .and_then(|prev_idx| state.frames.get(prev_idx))
                .map(|frame| frame.timestamp_us);
            let is_cursor = idx == selected;
            let in_selection = state.select_anchor.is_some()
                && selection_range.is_some_and(|(a, b)| idx >= a && idx <= b);
            let prefix = if is_cursor {
                '>'
            } else if in_selection {
                '*'
            } else {
                ' '
            };
            if is_cursor || in_selection {
                // Plain text + reverse video — no ANSI codes to interfere
                let line = format!(
                    "{prefix}{}",
                    format_frame(
                        &state.frames[idx],
                        iface,
                        ts_mode,
                        previous_timestamp_us,
                        &plain,
                        false,
                    )
                );
                let padded = format!("{line:<width$}", width = cols as usize);
                let truncated = truncate_to_width(&padded, cols as usize);
                queue!(stdout, SetAttribute(Attribute::Reverse))?;
                if is_cursor && state.select_anchor.is_some() {
                    // The cursor within a visual range gets an extra bold hint
                    // so users can always see where the anchor-moving end is.
                    queue!(stdout, SetAttribute(Attribute::Bold))?;
                }
                queue!(stdout, Print(truncated), SetAttribute(Attribute::Reset))?;
            } else {
                let line = format!(
                    "{prefix}{}",
                    format_frame_interactive(
                        &state.frames[idx],
                        ts_mode,
                        previous_timestamp_us,
                        colors,
                    )
                );
                let truncated = truncate_to_visible_width(&line, cols as usize);
                queue!(stdout, Print(truncated))?;
            }
        }
    }

    // ── Separator + tail pane ────────────────────────────────────────────
    if tail_rows > 0 {
        // Draw separator line
        let sep_row = main_rows as u16;
        let separator: String = "\u{2500}".repeat(cols as usize);
        queue!(
            stdout,
            MoveTo(0, sep_row),
            SetAttribute(Attribute::Dim),
            Print(truncate_to_width(&separator, cols as usize)),
            SetAttribute(Attribute::Reset)
        )?;

        // Draw tail pane (latest frames, colored)
        let tail_start = total.saturating_sub(tail_rows);
        for row in 0..tail_rows {
            let screen_row = (main_rows + 1 + row) as u16; // +1 for separator
            queue!(stdout, MoveTo(0, screen_row))?;
            let idx = tail_start + row;
            if idx < total {
                let previous_timestamp_us = idx
                    .checked_sub(1)
                    .and_then(|prev_idx| state.frames.get(prev_idx))
                    .map(|frame| frame.timestamp_us);
                let line = format!(
                    " {}",
                    format_frame_interactive(
                        &state.frames[idx],
                        ts_mode,
                        previous_timestamp_us,
                        colors,
                    )
                );
                let truncated = truncate_to_visible_width(&line, cols as usize);
                queue!(stdout, Print(truncated))?;
            }
        }
    }

    // ── Help bar ─────────────────────────────────────────────────────────
    let help =
        "/ bytes  i ID  n/N next  v sel  y copy  Y hex  V all-matches  c clear  arrows scroll  q quit";
    queue!(
        stdout,
        MoveTo(0, help_row),
        SetAttribute(Attribute::Reset),
        SetAttribute(Attribute::Reverse),
        Print(truncate_to_width(help, cols as usize)),
        SetAttribute(Attribute::Reset)
    )?;

    // ── Status bar ───────────────────────────────────────────────────────
    queue!(
        stdout,
        MoveTo(0, status_row),
        SetAttribute(Attribute::Reset),
        Clear(ClearType::CurrentLine)
    )?;
    if let Some(prompt) = &state.prompt {
        let prompt_text = format!("{}: {}", prompt.kind.title(), prompt.input);
        queue!(
            stdout,
            Print(truncate_to_width(&prompt_text, cols as usize)),
            Show
        )?;
        let cursor_col = prompt_text.len().min(cols.saturating_sub(1) as usize) as u16;
        queue!(stdout, MoveTo(cursor_col, status_row))?;
    } else {
        queue!(
            stdout,
            Print(truncate_to_width(&state.status_line(iface), cols as usize)),
            Hide
        )?;
    }

    stdout.flush()
}

fn parse_hex_bytes(input: &str) -> Result<Vec<u8>, String> {
    let normalized = input.trim();
    if normalized.is_empty() {
        return Err("Enter at least one byte.".to_string());
    }

    let split_tokens: Vec<&str> = normalized
        .split(|c: char| c.is_ascii_whitespace() || matches!(c, ':' | '-' | ','))
        .filter(|token| !token.is_empty())
        .collect();

    if split_tokens.len() > 1 {
        let mut bytes = Vec::with_capacity(split_tokens.len());
        for token in split_tokens {
            let token = token
                .strip_prefix("0x")
                .or_else(|| token.strip_prefix("0X"))
                .unwrap_or(token);
            if token.is_empty() || token.len() > 2 {
                return Err(format!("Invalid byte token '{token}'."));
            }
            let byte = u8::from_str_radix(token, 16)
                .map_err(|_| format!("Invalid byte token '{token}'."))?;
            bytes.push(byte);
        }
        return Ok(bytes);
    }

    let packed = split_tokens[0]
        .strip_prefix("0x")
        .or_else(|| split_tokens[0].strip_prefix("0X"))
        .unwrap_or(split_tokens[0]);
    if !packed.len().is_multiple_of(2) {
        return Err("Packed byte searches need an even number of hex digits.".to_string());
    }

    let mut bytes = Vec::with_capacity(packed.len() / 2);
    for idx in (0..packed.len()).step_by(2) {
        let byte = u8::from_str_radix(&packed[idx..idx + 2], 16)
            .map_err(|_| "Invalid hex byte sequence.".to_string())?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn parse_can_id(input: &str) -> Result<u32, String> {
    let token = input.trim();
    if token.is_empty() {
        return Err("Enter an arbitration ID in hex.".to_string());
    }
    let token = token
        .strip_prefix("0x")
        .or_else(|| token.strip_prefix("0X"))
        .unwrap_or(token);
    let can_id = u32::from_str_radix(token, 16)
        .map_err(|_| "Arbitration ID must be hexadecimal.".to_string())?;
    if can_id > CAN_EFF_MASK {
        return Err(format!(
            "Arbitration ID 0x{can_id:X} exceeds the 29-bit CAN range."
        ));
    }
    Ok(can_id)
}

fn frame_matches(frame: &RxFrame, search: &SearchQuery) -> bool {
    match search {
        SearchQuery::Bytes(bytes) => {
            !bytes.is_empty()
                && frame.data[..frame.len]
                    .windows(bytes.len())
                    .any(|win| win == bytes)
        }
        SearchQuery::ArbitrationId(can_id) => frame.can_id == *can_id,
    }
}

fn truncate_to_width(text: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    text.chars().take(width).collect()
}

// ── Clipboard via OSC 52 ──────────────────────────────────────────────────
//
// OSC 52 (\x1b]52;c;<base64>\x07) asks the terminal emulator to put the
// decoded payload into the system clipboard.  Works across kitty, WezTerm,
// Alacritty, iTerm2, modern gnome-terminal, and tmux (when configured with
// `set -g set-clipboard on`), including over SSH.  Terminals that don't
// implement OSC 52 silently drop the sequence.

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(BASE64_ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(BASE64_ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() >= 2 {
            out.push(BASE64_ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() >= 3 {
            out.push(BASE64_ALPHABET[(n & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn copy_to_clipboard_osc52(payload: &str) -> io::Result<()> {
    let encoded = base64_encode(payload.as_bytes());
    let mut stdout = io::stdout().lock();
    write!(stdout, "\x1b]52;c;{encoded}\x07")?;
    stdout.flush()
}

// ── TCP server ────────────────────────────────────────────────────────────

fn run_tcp_server(
    listener: TcpListener,
    manager: Arc<ClientManager>,
    stop: Arc<AtomicBool>,
    log_errors: bool,
) {
    listener
        .set_nonblocking(true)
        .expect("cannot set non-blocking");

    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => manager.add_client(stream),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                if log_errors && !stop.load(Ordering::Relaxed) {
                    eprintln!("accept error: {e}");
                }
            }
        }
    }
}

// ── Zeroconf ──────────────────────────────────────────────────────────────

const SERVICE_TYPE: &str = "_ecuconnect-log._tcp.local.";
const SERVICE_NAME_PREFIX: &str = "ECUconnect-Logger";

struct ZeroconfHandle {
    daemon: mdns_sd::ServiceDaemon,
    fullname: String,
}

fn start_zeroconf(
    port: u16,
    interface: &str,
    service_name: Option<&str>,
    colors: &Colors,
    log_success: bool,
) -> Option<ZeroconfHandle> {
    let hostname = hostname();
    let instance_name = service_name
        .map(String::from)
        .unwrap_or_else(|| format!("{SERVICE_NAME_PREFIX} {hostname}:{port}"));

    let daemon = match mdns_sd::ServiceDaemon::new() {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "{} {} Failed to create mDNS daemon: {e}",
                timestamp_now(),
                colors.tag("zeroconf", "35"),
            );
            return None;
        }
    };

    let mut properties = HashMap::from([
        ("process".to_string(), "mcandump".to_string()),
        ("interface".to_string(), "socketcan".to_string()),
        ("channel".to_string(), interface.to_string()),
    ]);
    if let Some(bitrate) = get_can_interface_bitrate(interface) {
        properties.insert("bitrate".to_string(), bitrate.to_string());
    }

    let info = match mdns_sd::ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &format!("{hostname}.local."),
        "",
        port,
        properties,
    )
    .map(|info| {
        // Empty address lists only work when addr_auto is enabled.
        info.enable_addr_auto()
    }) {
        Ok(info) => info,
        Err(e) => {
            eprintln!(
                "{} {} Failed to create service info: {e}",
                timestamp_now(),
                colors.tag("zeroconf", "35"),
            );
            return None;
        }
    };

    let fullname = info.get_fullname().to_string();
    if let Err(e) = daemon.register(info) {
        eprintln!(
            "{} {} Failed to register service: {e}",
            timestamp_now(),
            colors.tag("zeroconf", "35"),
        );
        return None;
    }

    if log_success {
        eprintln!(
            "{} {} Service '{}' published on port {}",
            timestamp_now(),
            colors.tag("zeroconf", "35"),
            instance_name,
            port,
        );
    }

    Some(ZeroconfHandle { daemon, fullname })
}

fn stop_zeroconf(handle: ZeroconfHandle, colors: &Colors) {
    let _ = handle.daemon.unregister(&handle.fullname);
    let _ = handle.daemon.shutdown();
    eprintln!(
        "{} {} Service removed.",
        timestamp_now(),
        colors.tag("zeroconf", "35"),
    );
}

// ── Display helpers ───────────────────────────────────────────────────────

fn timestamp_now() -> String {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let ms = dur.subsec_millis();
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe { libc::localtime_r(&secs, &mut tm) };
    format!(
        "{:02}:{:02}:{:02}.{ms:03}",
        tm.tm_hour, tm.tm_min, tm.tm_sec
    )
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn get_can_interface_bitrate(ifname: &str) -> Option<u32> {
    let ip_path = ["/usr/sbin/ip", "/sbin/ip"]
        .into_iter()
        .find(|candidate| Path::new(candidate).is_file())
        .unwrap_or("ip");
    let output = Command::new(ip_path)
        .args(["-details", "-json", "link", "show", "dev", ifname])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_can_bitrate_from_ip_json(std::str::from_utf8(&output.stdout).ok()?)
}

fn parse_can_bitrate_from_ip_json(json: &str) -> Option<u32> {
    let key = "\"bitrate\"";
    let start = json.find(key)? + key.len();
    let value = json[start..].split_once(':')?.1.trim_start();
    let digits: String = value.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

/// Subtle per-column coloring for interactive mode.  Each column gets one
/// muted color so columns are visually distinct without the heat-map noise.
/// Colors are switched directly without intermediate resets to avoid flicker.
fn format_frame_interactive(
    frame: &RxFrame,
    ts_mode: TimestampMode,
    previous_timestamp_us: Option<u64>,
    colors: &Colors,
) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(512);

    let dim = colors.dim_code();

    // Timestamp — dim, no brackets
    match ts_mode {
        TimestampMode::Absolute => {
            let _ = write!(out, "\x1b[{dim}m{} ", timestamp_from_us(frame.timestamp_us));
        }
        TimestampMode::Delta => {
            let delta_us = frame
                .timestamp_us
                .saturating_sub(previous_timestamp_us.unwrap_or(frame.timestamp_us));
            let delta = Duration::from_micros(delta_us);
            let _ = write!(
                out,
                "\x1b[{dim}m{:6}.{:06} ",
                delta.as_secs(),
                delta.subsec_micros()
            );
        }
        TimestampMode::None => {}
    }

    // (Interface name is shown once in the status bar for the interactive TUI
    // — it's a constant for the life of the session, so repeating it per row
    // just wastes screen real estate.)

    // CAN ID — stable per-ID color
    let id_code = colors.id_color(frame.can_id);
    if frame.is_extended {
        let _ = write!(out, "\x1b[{id_code}m{:08X}", frame.can_id);
    } else {
        let _ = write!(out, "\x1b[{id_code}m{:>8X}", frame.can_id);
    }

    // FD indicator
    if frame.is_fd {
        let _ = write!(out, "\x1b[{}m  ##", colors.fd_code());
        if frame.brs {
            out.push('B');
        }
        if frame.esi {
            out.push('E');
        }
    }

    // DLC — dim
    let _ = write!(out, "\x1b[{dim}m  [{:2}]  ", frame.len);

    // Data bytes — uniform muted color
    let max_bytes: usize = if frame.is_fd { 64 } else { 8 };
    let _ = write!(out, "\x1b[{}m", colors.data_interactive_code());
    for (i, &b) in frame.data[..frame.len].iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        let _ = write!(out, "{b:02X}");
    }
    // Pad remaining columns (reset color first so padding is plain)
    out.push_str("\x1b[0m");
    for _ in frame.len..max_bytes {
        out.push_str("   ");
    }

    // ASCII
    let _ = write!(out, "  \x1b[{}m'", colors.ascii_printable_code());
    for &b in &frame.data[..frame.len] {
        if b.is_ascii_graphic() || b == b' ' {
            out.push(b as char);
        } else {
            out.push('.');
        }
    }
    out.push_str("'\x1b[0m");

    out
}

/// Truncate a string containing ANSI escapes to a given visible width.
/// Always appends a reset sequence to prevent color bleed into subsequent output.
fn truncate_to_visible_width(text: &str, width: usize) -> String {
    if width == 0 {
        return "\x1b[0m".to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut visible = 0;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            // Copy the entire CSI escape sequence verbatim.
            // Format: ESC [ <params> <final byte>
            // The '[' (0x5B) is the CSI introducer — skip it, then skip
            // parameter/intermediate bytes (0x20..0x3F) until the final
            // byte (0x40..0x7E, typically a letter like 'm').
            out.push(ch);
            if chars.peek() == Some(&'[') {
                out.push(chars.next().unwrap()); // consume '['
                for inner in chars.by_ref() {
                    out.push(inner);
                    if ('@'..='~').contains(&inner) {
                        break;
                    }
                }
            }
        } else {
            if visible >= width {
                break;
            }
            out.push(ch);
            visible += 1;
        }
    }
    out.push_str("\x1b[0m");
    out
}

fn format_frame(
    frame: &RxFrame,
    iface: &str,
    ts_mode: TimestampMode,
    previous_timestamp_us: Option<u64>,
    colors: &Colors,
    show_iface: bool,
) -> String {
    let mut out = String::with_capacity(512);

    let dim = colors.dim_code();

    // Timestamp
    match ts_mode {
        TimestampMode::Absolute => {
            out.push_str(
                &colors.paint(&format!("{} ", timestamp_from_us(frame.timestamp_us)), dim),
            );
        }
        TimestampMode::Delta => {
            let delta_us = frame
                .timestamp_us
                .saturating_sub(previous_timestamp_us.unwrap_or(frame.timestamp_us));
            let delta = Duration::from_micros(delta_us);
            out.push_str(&colors.paint(
                &format!("{:6}.{:06} ", delta.as_secs(), delta.subsec_micros()),
                dim,
            ));
        }
        TimestampMode::None => {}
    }

    // Interface
    if show_iface {
        out.push_str(&colors.paint(&format!("{iface:>8}"), colors.iface_code()));
        out.push_str("  ");
    }

    // CAN ID — always 8 chars wide (padded), stable per-ID color
    let id_code = colors.id_color(frame.can_id);
    let id_str = if frame.is_extended {
        format!("{:08X}", frame.can_id)
    } else {
        format!("{:>8X}", frame.can_id)
    };
    out.push_str(&colors.paint(&id_str, id_code));

    // FD indicator
    if frame.is_fd {
        let mut tag = String::from("  ##");
        if frame.brs {
            tag.push('B');
        }
        if frame.esi {
            tag.push('E');
        }
        out.push_str(&colors.paint(&tag, colors.fd_code()));
    }

    // DLC
    out.push_str("  ");
    out.push_str(&colors.paint(&format!("[{:2}]", frame.len), colors.dlc_code()));
    out.push_str("  ");

    // Data bytes — heat-mapped by value, padded to max 64 bytes width
    let max_bytes: usize = if frame.is_fd { 64 } else { 8 };
    for (i, &b) in frame.data[..frame.len].iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&colors.data_byte(b));
    }
    // Pad remaining columns so ASCII always starts at the same position
    let printed = frame.len;
    for _ in printed..max_bytes {
        out.push_str("   ");
    }

    // ASCII representation
    out.push_str("  ");
    out.push_str(&colors.paint("'", dim));
    for &b in &frame.data[..frame.len] {
        out.push_str(&colors.ascii_char(b));
    }
    out.push_str(&colors.paint("'", dim));

    out
}

fn timestamp_from_us(timestamp_us: u64) -> String {
    let secs = (timestamp_us / 1_000_000) as i64;
    let ms = (timestamp_us % 1_000_000) / 1_000;
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe { libc::localtime_r(&secs, &mut tm) };
    format!(
        "{:02}:{:02}:{:02}.{ms:03}",
        tm.tm_hour, tm.tm_min, tm.tm_sec
    )
}

// ── Port selection ────────────────────────────────────────────────────────

fn bind_tcp() -> io::Result<(TcpListener, u16)> {
    for p in AUTO_PORT_START..=65535 {
        match TcpListener::bind(("0.0.0.0", p)) {
            Ok(listener) => return Ok((listener, p)),
            Err(_) => continue,
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AddrNotAvailable,
        "no free TCP port found",
    ))
}

// ── Signal handling ───────────────────────────────────────────────────────

/// Global stop flag for the signal handler.
static STOP: std::sync::OnceLock<Arc<AtomicBool>> = std::sync::OnceLock::new();

extern "C" fn signal_handler(_sig: libc::c_int) {
    if let Some(flag) = STOP.get() {
        flag.store(true, Ordering::SeqCst);
    }
}

fn install_signal_handlers(stop: Arc<AtomicBool>) {
    STOP.set(stop).ok();
    unsafe {
        libc::signal(
            libc::SIGINT,
            signal_handler as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            signal_handler as *const () as libc::sighandler_t,
        );
    }
}

// ── main ──────────────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();
    let is_light = match cli.theme {
        Theme::Light => true,
        Theme::Dark => false,
        Theme::Auto => detect_is_light_theme().unwrap_or(false),
    };
    let log_colors = Colors::new(!cli.no_color, is_light);
    let log_file_path = resolve_log_file_path(cli.log_file.clone());
    // --service-name is only meaningful when the logger is enabled; treat it
    // as an implicit --serve so users don't have to spell both out.
    let serve = cli.serve || cli.service_name.is_some();

    // Open CAN socket (do this early to fail fast on permission errors)
    let (fd, hw_timestamps) = match open_can_socket(&cli.interface, true) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: cannot open {}: {e}", cli.interface);
            if e.raw_os_error() == Some(libc::EPERM) {
                eprintln!("hint: try running with CAP_NET_RAW or as root");
            }
            std::process::exit(1);
        }
    };

    // Optional: bind TCP server when the CANcorder logger is enabled.
    let (listener, port) = if serve {
        match bind_tcp() {
            Ok((l, p)) => (Some(l), Some(p)),
            Err(e) => {
                eprintln!("error: cannot bind TCP port: {e}");
                std::process::exit(1);
            }
        }
    } else {
        (None, None)
    };

    match port {
        Some(p) => eprintln!(
            "{} {} mcandump starting — interface: {}, TCP port: {p}, timestamps: {}",
            timestamp_now(),
            log_colors.tag("init", "34"),
            cli.interface,
            if hw_timestamps {
                "hardware"
            } else {
                "software"
            },
        ),
        None => eprintln!(
            "{} {} mcandump starting — interface: {}, timestamps: {} (logger off; pass --serve to enable CANcorder)",
            timestamp_now(),
            log_colors.tag("init", "34"),
            cli.interface,
            if hw_timestamps {
                "hardware"
            } else {
                "software"
            },
        ),
    }

    let stop = Arc::new(AtomicBool::new(false));

    install_signal_handlers(stop.clone());

    // TCP server + recorder (only when --serve).
    let (manager, rec_tx) = if let Some(listener) = listener {
        let manager = Arc::new(ClientManager::new(
            Colors::new(!cli.no_color, is_light),
            !cli.interactive,
        ));
        {
            let manager = manager.clone();
            let stop = stop.clone();
            let log_errors = !cli.interactive;
            thread::Builder::new()
                .name("tcp-server".into())
                .spawn(move || run_tcp_server(listener, manager, stop, log_errors))
                .expect("cannot spawn TCP server thread");
        }

        // Recording thread — drains the unbounded channel, packs frames, and
        // fans out to per-client writer threads.  Never blocks on slow clients.
        let (rec_tx, rec_rx) = mpsc::channel::<RxFrame>();
        {
            let manager = manager.clone();
            thread::Builder::new()
                .name("recorder".into())
                .spawn(move || run_recorder(rec_rx, manager))
                .expect("cannot spawn recorder thread");
        }
        (Some(manager), Some(rec_tx))
    } else {
        (None, None)
    };

    // Background candump-compatible log writer.
    let (log_tx, log_handle) = if let Some(ref path) = log_file_path {
        let file = match File::create(path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("error: cannot open log file {}: {e}", path.display());
                std::process::exit(1);
            }
        };

        if !cli.interactive {
            eprintln!(
                "{} {} Writing candump log to {}",
                timestamp_now(),
                log_colors.tag("log", "36"),
                path.display(),
            );
        }

        let (tx, rx) = mpsc::channel::<RxFrame>();
        let iface = cli.interface.clone();
        let handle = thread::Builder::new()
            .name("log-writer".into())
            .spawn(move || run_log_writer(rx, iface, file))
            .expect("cannot spawn log writer thread");
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // Display thread — low priority (nice +10) so it never starves CAN reading
    // or recording.  Gets its own unbounded channel.
    let (disp_tx, display_handle) = if !cli.quiet {
        let (tx, rx) = mpsc::channel::<RxFrame>();
        let iface = cli.interface.clone();
        let ts_mode = cli.timestamp;
        let colors = Colors::new(!cli.no_color, is_light);
        let stop = stop.clone();
        let interactive = cli.interactive;
        let handle = thread::Builder::new()
            .name("display".into())
            .spawn(move || {
                if interactive {
                    run_interactive_display(rx, iface, ts_mode, colors, stop);
                } else {
                    run_display(rx, iface, ts_mode, colors);
                }
            })
            .expect("cannot spawn display thread");
        (Some(tx), Some(handle))
    } else {
        (None, None)
    };

    // Zeroconf (only when the CANcorder logger is enabled).
    let zeroconf = if let Some(port) = port {
        match start_zeroconf(
            port,
            &cli.interface,
            cli.service_name.as_deref(),
            &log_colors,
            !cli.interactive,
        ) {
            Some(h) => Some(h),
            None => {
                eprintln!("error: zeroconf service registration failed — cannot proceed");
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    if !cli.interactive {
        eprintln!(
            "{} {} Ready. Press Ctrl+C to stop.",
            timestamp_now(),
            log_colors.tag("ready", "32"),
        );
    }

    // ── Main CAN receive loop ─────────────────────────────────────────────
    //
    // This is the highest-priority path.  It only reads from the kernel and
    // pushes into unbounded channels — it never touches TCP sockets or stdout.
    let mut frame_count: u64 = 0;
    let mut error_count: u64 = 0;
    let start_time = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        let frame = match read_frame(fd) {
            Ok(Some(f)) => f,
            Ok(None) => continue, // timeout, error frame, or RTR — just loop
            Err(e) => {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                // EAGAIN/EWOULDBLOCK = SO_RCVTIMEO fired, EINTR = signal
                match e.raw_os_error() {
                    Some(libc::EAGAIN) | Some(libc::EINTR) => continue,
                    _ => {
                        error_count += 1;
                        if error_count <= 5 {
                            eprintln!("read error: {e}");
                        }
                        continue;
                    }
                }
            }
        };

        frame_count += 1;

        // Push to recorder (unbounded — never blocks)
        if let Some(ref rtx) = rec_tx {
            let _ = rtx.send(frame.clone());
        }

        // Push to log writer (unbounded — never blocks)
        if let Some(ref ltx) = log_tx {
            let _ = ltx.send(frame.clone());
        }

        // Push to display (unbounded — never blocks)
        if let Some(ref dtx) = disp_tx {
            let _ = dtx.send(frame);
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────────

    // Drop senders to signal recorder and display threads to drain and exit.
    drop(rec_tx);
    drop(log_tx);
    drop(disp_tx);
    if let Some(handle) = log_handle {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("warning: log writer failed: {e}"),
            Err(_) => eprintln!("warning: log writer thread panicked"),
        }
    }
    if let Some(handle) = display_handle {
        let _ = handle.join();
    }

    eprintln!(
        "\n{} {} Shutting down...",
        timestamp_now(),
        log_colors.tag("exit", "33"),
    );

    let elapsed = start_time.elapsed();

    eprintln!(
        "{} {} Frames received: {frame_count}, duration: {:.1}s",
        timestamp_now(),
        log_colors.tag("stats", "34"),
        elapsed.as_secs_f64(),
    );
    if let Some(ref manager) = manager {
        let tcp_sent = manager.stats.frames_sent.load(Ordering::Relaxed);
        let tcp_dropped = manager.stats.frames_dropped.load(Ordering::Relaxed);
        let tcp_bytes = manager.stats.bytes_sent.load(Ordering::Relaxed);
        eprintln!(
            "{} {} TCP: sent: {tcp_sent}, dropped: {tcp_dropped}, bytes: {tcp_bytes}",
            timestamp_now(),
            log_colors.tag("stats", "34"),
        );
    }

    if let Some(z) = zeroconf {
        stop_zeroconf(z, &log_colors);
    }
    if let Some(manager) = manager {
        manager.close_all();
        // Give writer threads a moment to drain and exit after close_all drops senders.
        thread::sleep(Duration::from_millis(100));
    }
    unsafe {
        libc::close(fd);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_frame(can_id: u32, data: &[u8]) -> RxFrame {
        let mut payload = [0u8; 64];
        payload[..data.len()].copy_from_slice(data);
        RxFrame {
            can_id,
            is_extended: false,
            is_fd: false,
            brs: false,
            esi: false,
            len: data.len(),
            data: payload,
            timestamp_us: 0,
        }
    }

    #[test]
    fn formats_classic_candump_log_line() {
        let mut frame = sample_frame(0x123, &[0xDE, 0xAD, 0xBE, 0xEF]);
        frame.timestamp_us = 1_712_345_678_901_234;
        assert_eq!(
            format_candump_log_line(&frame, "can0"),
            "(1712345678.901234) can0 123#DEADBEEF"
        );
    }

    #[test]
    fn formats_extended_fd_candump_log_line() {
        let mut frame = sample_frame(0x18FF50E5, &[0x11, 0x22, 0x33]);
        frame.is_extended = true;
        frame.is_fd = true;
        frame.brs = true;
        frame.esi = true;
        frame.timestamp_us = 42;
        assert_eq!(
            format_candump_log_line(&frame, "vcan0"),
            "(0.000042) vcan0 18FF50E5##3112233"
        );
    }

    #[test]
    fn default_log_filename_looks_like_candump() {
        let filename = default_candump_log_filename();
        assert!(filename.starts_with("candump-"));
        assert!(filename.ends_with(".log"));
        assert_eq!(filename.len(), "candump-2026-04-02_123456.log".len());
    }

    #[test]
    fn clap_accepts_log_file_without_path() {
        let cli = Cli::try_parse_from(["mcandump", "can0", "--log-file"]).unwrap();
        assert!(matches!(cli.log_file, Some(None)));
    }

    #[test]
    fn clap_accepts_log_file_with_path() {
        let cli = Cli::try_parse_from(["mcandump", "can0", "--log-file", "capture.log"]).unwrap();
        assert_eq!(
            resolve_log_file_path(cli.log_file),
            Some(PathBuf::from("capture.log"))
        );
    }

    #[test]
    fn parses_spaced_hex_bytes() {
        assert_eq!(
            parse_hex_bytes("DE AD BE EF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }

    #[test]
    fn parses_packed_hex_bytes() {
        assert_eq!(
            parse_hex_bytes("deadbeef").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
    }

    #[test]
    fn rejects_odd_packed_hex_bytes() {
        assert!(parse_hex_bytes("ABC").is_err());
    }

    #[test]
    fn matches_byte_subsequence() {
        let frame = sample_frame(0x123, &[0x01, 0xDE, 0xAD, 0x02]);
        assert!(frame_matches(&frame, &SearchQuery::Bytes(vec![0xDE, 0xAD])));
    }

    #[test]
    fn matches_arbitration_id() {
        let frame = sample_frame(0x456, &[0x00]);
        assert!(frame_matches(&frame, &SearchQuery::ArbitrationId(0x456)));
        assert!(!frame_matches(&frame, &SearchQuery::ArbitrationId(0x123)));
    }

    #[test]
    fn base64_encode_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn selection_range_cursor_only_without_anchor() {
        let mut state = InteractiveState::new();
        state.frames.push(sample_frame(0x100, &[0x00]));
        state.frames.push(sample_frame(0x200, &[0x00]));
        state.selected = 1;
        assert_eq!(state.selection_range(), Some((1, 1)));
    }

    #[test]
    fn selection_range_spans_anchor_to_cursor() {
        let mut state = InteractiveState::new();
        for id in [0x100, 0x200, 0x300, 0x400] {
            state.frames.push(sample_frame(id, &[0x00]));
        }
        state.select_anchor = Some(3);
        state.selected = 1;
        assert_eq!(state.selection_range(), Some((1, 3)));
    }

    #[test]
    fn parses_can_bitrate_from_ip_json() {
        let json = r#"
            [{
                "ifname": "can0",
                "linkinfo": {
                    "info_kind": "can",
                    "info_data": {
                        "bitrate": 500000,
                        "sample_point": 0.875
                    }
                }
            }]
        "#;
        assert_eq!(parse_can_bitrate_from_ip_json(json), Some(500_000));
    }

    #[test]
    fn ignores_missing_can_bitrate_in_ip_json() {
        let json = r#"
            [{
                "ifname": "can0",
                "linkinfo": {
                    "info_kind": "can",
                    "info_data": {
                        "dbitrate": 2000000
                    }
                }
            }]
        "#;
        assert_eq!(parse_can_bitrate_from_ip_json(json), None);
    }
}
