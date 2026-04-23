# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

mcandump is a CAN bus logger proxy for Linux, written in Rust. It reads CAN and CAN-FD frames from a SocketCAN interface and displays them on the terminal (like `candump`). The CANcorder logger side (TCP server + Zeroconf/mDNS advertisement) is opt-in via `--serve`; without that flag the tool has no network side effects and behaves like plain `candump`. When `--serve` is passed it also forwards frames to CANcorder clients via the ECUconnect Logger binary protocol over TCP and registers itself as a Zeroconf/mDNS service so CANcorder discovers it automatically.

## Build and test

```bash
cargo build --release          # release build (LTO, stripped)
cargo build                    # debug build
make build                     # same as cargo build --release
cargo clippy                   # lint
cargo fmt                      # format
```

There is no test suite — testing requires a SocketCAN interface. Set up virtual CAN for local testing:

```bash
sudo ip link add dev vcan0 type vcan && sudo ip link set up vcan0
make test                      # run on vcan0 (no zeroconf)
```

Use `mcangen` (companion tool) to generate traffic on the virtual interface:

```bash
mcangen vcan0 -r 100           # 100 fps on vcan0
```

The binary requires `CAP_NET_RAW` or root to open raw CAN sockets.

## Architecture

Single-file application: everything is in `src/main.rs`. No modules, no library crate.

**Sections in order:**

1. **SocketCAN FFI** — `CanFdFrame`, `SockaddrCan`, `Ifreq` structs matching kernel layout. `open_can_socket()` creates, configures (FD, rcvbuf, timeout), and binds. `read_frame()` reads into a canfd_frame-sized buffer and returns `Option<RxFrame>` (None for error/RTR/timeout).

2. **ECUconnect protocol** — `pack_ecuconnect_frame()` serializes `RxFrame` into the binary wire format: `[timestamp:8][id:4][flags:1][dlc:1][data:0-64]`, big-endian. Flags: bit0=extended, bit1=FD, bit2=BRS, bit3=ESI.

3. **CLI** — clap derive macros. `TimestampMode` enum (absolute/delta/none). Options for color, quiet, service name. Port is always auto-picked; zeroconf is mandatory.

4. **TCP client management** — `ClientManager` holds a `Mutex<Vec<ClientHandle>>`. Each client gets a dedicated writer thread with its own unbounded `mpsc` channel. `broadcast()` fans out an `Arc<Vec<u8>>` to every client channel — never blocks, never drops frames. A slow client only backs up its own queue. Shared `Stats` (frames_sent/dropped/bytes) are tracked via `Arc<Stats>` with atomics.

5. **Recording thread** — `run_recorder()` drains the unbounded recorder channel, packs each `RxFrame` into the ECUconnect binary format, wraps it in `Arc`, and fans out to all client channels via `broadcast()`.

6. **Display thread** — `run_display()` runs at `nice(10)` (lower priority than all other threads). Drains its own unbounded channel and formats frames to stdout. Never interferes with CAN reading or TCP recording.

7. **TCP server** — Non-blocking accept loop in a background thread. Each accepted client is registered via `add_client()`, which spawns its per-client writer thread.

8. **Zeroconf** — Uses `mdns-sd` crate. Registers service type `_ecuconnect-log._tcp.local.` with name prefix `ECUconnect-Logger`. TXT records carry system, process, interface, channel metadata. Only active when `--serve` (or `--service-name`, which implies it) is passed; when active, startup fails hard if registration fails.

9. **Display formatting** — Rich candump-style terminal output. CAN IDs get a stable per-ID color (hash-based palette) so the same ECU always appears in the same hue. Data bytes are heat-mapped by value (dim gray for 0x00, cyan/green/yellow/red gradient, bold red for 0xFF). ASCII column colors printable chars green, non-printable dim.

    Interactive mode adds a vim-style visual selection (`select_anchor: Option<usize>`): pressing `v` drops the anchor at the cursor, and every existing navigation key extends the highlighted range. `y` yanks the range as candump log lines, `Y` as compact `ID#DATA`, `V` yanks every frame matching the current search. The clipboard transport is an OSC 52 escape sequence (`\x1b]52;c;<base64>\x07`) written straight to stdout — no external tool needed, works over SSH. Base64 is hand-rolled (~25 lines) to keep the dependency count low.

10. **Signal handling** — Global `OnceLock<Arc<AtomicBool>>` + libc signal handler for clean SIGINT/SIGTERM shutdown. SO_RCVTIMEO on the CAN socket ensures the read loop checks the stop flag every 500ms.

11. **main()** — Opens CAN socket, binds TCP, installs signals, starts server/recorder/display threads, starts zeroconf. The main loop only reads CAN frames and pushes to unbounded channels — it never touches TCP or stdout.

**Threading model:**

```
Main thread (default pri) : read CAN socket → push to recorder + display channels
Recorder thread (default pri) : recv frames → pack → fan out to per-client channels
Per-client writer threads : drain own channel → write_all to TCP socket
Display thread (nice +10) : drain channel → format → print to stdout
TCP server thread : accept connections → spawn per-client writers
```

**Key design choices:**
- The CAN reader never blocks on slow consumers — it pushes to unbounded `mpsc` channels.
- Each TCP client has its own unbounded queue and writer thread — a slow client only stalls itself, never other clients or the CAN reader.
- The display thread is `nice(10)` so it yields to CAN reading and recording under load.
- CAN-FD is always enabled on the socket (graceful fallback if kernel doesn't support it).
- SO_RCVTIMEO avoids blocking forever in read() so Ctrl-C is responsive.
- Error/RTR frames are silently skipped (matching CANcorder Python loggers).

## Dependencies

Four crates: `clap` (CLI), `libc` (syscalls), `mdns-sd` (Zeroconf/mDNS). The release profile enables LTO and single codegen unit.
