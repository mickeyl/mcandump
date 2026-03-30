use clap::{Parser, ValueEnum};
use std::io::{self, Write};
use std::mem;
use std::net::{TcpListener, TcpStream};
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

/// mcandump — Mickey's CAN dump + CANcorder logger proxy
///
/// Reads CAN/CAN-FD frames from a SocketCAN interface, displays them on the
/// terminal (like candump), and simultaneously forwards them to CANcorder
/// clients via the ECUconnect Logger binary protocol over TCP.
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

    /// Suppress terminal frame display (TCP forwarding only)
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Custom Zeroconf service name (default: "ECUconnect-Logger <hostname>:<port>")
    #[arg(long)]
    service_name: Option<String>,
}

// ── ANSI color helpers ────────────────────────────────────────────────────

struct Colors {
    enabled: bool,
}

impl Colors {
    fn new(enabled: bool) -> Self {
        Self { enabled }
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
    /// traffic.  The palette avoids red (reserved for 0xFF data bytes) and dim
    /// gray (reserved for 0x00).
    fn id_color(can_id: u32) -> &'static str {
        const PALETTE: &[&str] = &[
            "1;36", // bold cyan
            "1;32", // bold green
            "1;33", // bold yellow
            "1;35", // bold magenta
            "1;34", // bold blue
            "36",   // cyan
            "32",   // green
            "33",   // yellow
            "35",   // magenta
            "34",   // blue
        ];
        // Simple hash: multiply by a prime, take modulo palette length.
        let idx = ((can_id as u64).wrapping_mul(2654435761) % PALETTE.len() as u64) as usize;
        PALETTE[idx]
    }

    /// Color a data byte by value — a heat-map that makes patterns jump out:
    ///   0x00        dim gray       (null bytes are "cold")
    ///   0x01–0x1F   dark cyan      (low / control range)
    ///   0x20–0x7E   green          (printable ASCII range)
    ///   0x7F–0xBF   yellow         (upper half)
    ///   0xC0–0xFE   light red      (high range)
    ///   0xFF        bold bright red (saturated)
    fn data_byte(&self, b: u8) -> String {
        if !self.enabled {
            return format!("{b:02X}");
        }
        let code = match b {
            0x00 => "2;37",      // dim white (gray)
            0x01..=0x1F => "36", // cyan
            0x20..=0x7E => "32", // green
            0x7F..=0xBF => "33", // yellow
            0xC0..=0xFE => "31", // red
            0xFF => "1;31",      // bold red
        };
        format!("\x1b[{code}m{b:02X}\x1b[0m")
    }

    /// Color an ASCII character: printable → green, non-printable dot → dim.
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
            format!("\x1b[32m{}\x1b[0m", b as char)
        } else {
            "\x1b[2;37m.\x1b[0m".to_string()
        }
    }
}

// ── Socket helpers ────────────────────────────────────────────────────────

fn open_can_socket(ifname: &str, enable_fd: bool) -> io::Result<i32> {
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

        Ok(fd)
    }
}

/// Read a single CAN or CAN-FD frame from the socket.
///
/// Returns `None` for error/RTR frames (silently skipped) or on timeout.
fn read_frame(fd: i32) -> io::Result<Option<RxFrame>> {
    let mut buf = [0u8; CANFD_FRAME_SIZE];
    let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, CANFD_FRAME_SIZE) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    let n = n as usize;
    if n < CAN_FRAME_SIZE {
        return Ok(None); // runt frame
    }

    let timestamp_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

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
}

impl ClientManager {
    fn new(colors: Colors) -> Self {
        Self {
            clients: Mutex::new(Vec::new()),
            stats: Arc::new(Stats {
                frames_sent: AtomicU64::new(0),
                frames_dropped: AtomicU64::new(0),
                bytes_sent: AtomicU64::new(0),
            }),
            colors,
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
                let tag = "\x1b[33m[server]\x1b[0m";
                eprintln!(
                    "{} {tag} Client writer exited: {writer_addr}",
                    timestamp_now(),
                );
            })
            .expect("cannot spawn client writer thread");

        let mut clients = self.clients.lock().unwrap();
        clients.push(ClientHandle {
            tx,
            addr: addr.clone(),
        });
        eprintln!(
            "{} {} Client connected: {addr} (total: {})",
            timestamp_now(),
            self.colors.tag("server", "32"),
            clients.len(),
        );
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
    let mut last_ts = Instant::now();

    for frame in rx {
        let line = format_frame(&frame, &iface, ts_mode, &mut last_ts, &colors);
        let mut out = stdout.lock();
        let _ = writeln!(out, "{line}");
    }
}

// ── TCP server ────────────────────────────────────────────────────────────

fn run_tcp_server(listener: TcpListener, manager: Arc<ClientManager>, stop: Arc<AtomicBool>) {
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
                if !stop.load(Ordering::Relaxed) {
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

    let properties = [
        ("system", hostname.as_str()),
        ("process", "mcandump"),
        ("interface", "socketcan"),
        ("channel", interface),
    ];

    let info = match mdns_sd::ServiceInfo::new(
        SERVICE_TYPE,
        &instance_name,
        &format!("{hostname}.local."),
        "",
        port,
        &properties[..],
    ) {
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

    eprintln!(
        "{} {} Service '{}' published on port {}",
        timestamp_now(),
        colors.tag("zeroconf", "35"),
        instance_name,
        port,
    );

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
    let secs = dur.as_secs();
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    let ms = dur.subsec_millis();
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn format_frame(
    frame: &RxFrame,
    iface: &str,
    ts_mode: TimestampMode,
    last_ts: &mut Instant,
    colors: &Colors,
) -> String {
    let mut out = String::with_capacity(512);

    // Timestamp
    match ts_mode {
        TimestampMode::Absolute => {
            out.push_str(&colors.paint(&format!("({}) ", timestamp_now()), "2"));
        }
        TimestampMode::Delta => {
            let now = Instant::now();
            let delta = now.duration_since(*last_ts);
            *last_ts = now;
            out.push_str(&colors.paint(
                &format!("({:6}.{:06}) ", delta.as_secs(), delta.subsec_micros()),
                "2",
            ));
        }
        TimestampMode::None => {}
    }

    // Interface
    out.push_str(&colors.paint(&format!("{iface:>8}"), "1;34"));
    out.push_str("  ");

    // CAN ID — always 8 chars wide (padded), stable per-ID color
    let id_code = Colors::id_color(frame.can_id);
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
        out.push_str(&colors.paint(&tag, "1;35"));
    }

    // DLC — middle gray
    out.push_str("  ");
    out.push_str(&colors.paint(&format!("[{:2}]", frame.len), "38;5;245"));
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
    out.push_str(&colors.paint("'", "2"));
    for &b in &frame.data[..frame.len] {
        out.push_str(&colors.ascii_char(b));
    }
    out.push_str(&colors.paint("'", "2"));

    out
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
    let log_colors = Colors::new(!cli.no_color);

    // Open CAN socket (do this early to fail fast on permission errors)
    let fd = match open_can_socket(&cli.interface, true) {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("error: cannot open {}: {e}", cli.interface);
            if e.raw_os_error() == Some(libc::EPERM) {
                eprintln!("hint: try running with CAP_NET_RAW or as root");
            }
            std::process::exit(1);
        }
    };

    // Bind TCP server
    let (listener, port) = match bind_tcp() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: cannot bind TCP port: {e}");
            std::process::exit(1);
        }
    };

    eprintln!(
        "{} {} mcandump starting — interface: {}, TCP port: {port}",
        timestamp_now(),
        log_colors.tag("init", "34"),
        cli.interface,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let manager = Arc::new(ClientManager::new(Colors::new(!cli.no_color)));

    install_signal_handlers(stop.clone());

    // TCP server thread
    {
        let manager = manager.clone();
        let stop = stop.clone();
        thread::Builder::new()
            .name("tcp-server".into())
            .spawn(move || run_tcp_server(listener, manager, stop))
            .expect("cannot spawn TCP server thread");
    }

    // Recording thread — drains the unbounded channel, packs frames, and fans
    // out to per-client writer threads.  Never blocks on slow clients.
    let (rec_tx, rec_rx) = mpsc::channel::<RxFrame>();
    {
        let manager = manager.clone();
        thread::Builder::new()
            .name("recorder".into())
            .spawn(move || run_recorder(rec_rx, manager))
            .expect("cannot spawn recorder thread");
    }

    // Display thread — low priority (nice +10) so it never starves CAN reading
    // or recording.  Gets its own unbounded channel.
    let disp_tx = if !cli.quiet {
        let (tx, rx) = mpsc::channel::<RxFrame>();
        let iface = cli.interface.clone();
        let ts_mode = cli.timestamp;
        let colors = Colors::new(!cli.no_color);
        thread::Builder::new()
            .name("display".into())
            .spawn(move || run_display(rx, iface, ts_mode, colors))
            .expect("cannot spawn display thread");
        Some(tx)
    } else {
        None
    };

    // Zeroconf (mandatory)
    let zeroconf = match start_zeroconf(
        port,
        &cli.interface,
        cli.service_name.as_deref(),
        &log_colors,
    ) {
        Some(h) => h,
        None => {
            eprintln!("error: zeroconf service registration failed — cannot proceed");
            std::process::exit(1);
        }
    };

    eprintln!(
        "{} {} Ready. Press Ctrl+C to stop.",
        timestamp_now(),
        log_colors.tag("ready", "32"),
    );

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
        let _ = rec_tx.send(frame.clone());

        // Push to display (unbounded — never blocks)
        if let Some(ref dtx) = disp_tx {
            let _ = dtx.send(frame);
        }
    }

    // ── Shutdown ──────────────────────────────────────────────────────────

    // Drop senders to signal recorder and display threads to drain and exit.
    drop(rec_tx);
    drop(disp_tx);

    eprintln!(
        "\n{} {} Shutting down...",
        timestamp_now(),
        log_colors.tag("exit", "33"),
    );

    let elapsed = start_time.elapsed();
    let tcp_sent = manager.stats.frames_sent.load(Ordering::Relaxed);
    let tcp_dropped = manager.stats.frames_dropped.load(Ordering::Relaxed);
    let tcp_bytes = manager.stats.bytes_sent.load(Ordering::Relaxed);

    eprintln!(
        "{} {} Frames received: {frame_count}, duration: {:.1}s",
        timestamp_now(),
        log_colors.tag("stats", "34"),
        elapsed.as_secs_f64(),
    );
    eprintln!(
        "{} {} TCP: sent: {tcp_sent}, dropped: {tcp_dropped}, bytes: {tcp_bytes}",
        timestamp_now(),
        log_colors.tag("stats", "34"),
    );

    stop_zeroconf(zeroconf, &log_colors);
    manager.close_all();
    // Give writer threads a moment to drain and exit after close_all drops senders.
    thread::sleep(Duration::from_millis(100));
    unsafe {
        libc::close(fd);
    }
}
