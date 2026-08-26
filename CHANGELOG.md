# Changelog

This changelog records user-visible changes in versions that were actually
published as GitHub releases. Changes made under intermediate version numbers
without a published release are included in the next version users could
download.

## [2.8.0] - 2026-08-26

- Added content-aware end-to-end quality diagnostics for the deterministic
  Classic CAN and CAN-FD formats shared with `mcangen` and ESPenlaub hwtest.
- Added detection and reporting of missing, duplicate, reordered, corrupt, and
  locally dropped frames, plus sender/receiver jitter and relay round-trip
  latency.
- Added quality filters, periodic reports, and automation-friendly strict mode
  with exit status 2 for failed or absent matching traffic.
- Added an automatic OSC 0 terminal title containing the SocketCAN interface,
  link/CAN state, nominal bitrate, sample point, and CAN-FD data timing when
  available. Redirected output remains clean and supported terminals restore
  the previous title on exit.

## [2.6.1] - 2026-05-06

- Added vim-style visual selection to the interactive viewer and OSC 52
  clipboard copy for the cursor, a selected range, all search matches, or the
  entire capture buffer.
- Added candump-line and compact `ID#DATA` copy formats plus configurable
  `--yank-to clipboard|primary|both` selection targets.
- Added automatic light/dark terminal theme detection via OSC 11 with a
  `COLORFGBG` fallback and explicit `--theme` overrides.
- Added `c` to clear the live capture buffer, clearer browsing/visual status,
  and keyboard resizing for the live tail pane.
- Added the resolved candump log path to shutdown statistics.

## [2.3.0] - 2026-04-23

- Made the CANcorder TCP server and Zeroconf advertisement opt-in via
  `--serve`. A default invocation now has no network side effects.
- Made `--service-name` imply `--serve`.

## [2.2.2] - 2026-04-03

- No runtime behavior changed. This release corrected the pinned Rust
  toolchain and tag-triggered automation used to produce release artifacts.

## [2.2.1] - 2026-04-03

- No runtime behavior changed. This release repaired Rust toolchain selection
  for CI and cross-built release artifacts.

## [2.2.0] - 2026-04-03

- Added the active SocketCAN bitrate to the CANcorder Zeroconf TXT metadata
  when it can be obtained from `iproute2`.

## [2.1.0] - 2026-04-03

- Added SocketCAN hardware receive timestamps with software timestamp
  fallback.
- Added a live tail pane so current CAN traffic remains visible while browsing
  older frames, including keyboard resizing.
- Improved the interactive viewer with stable per-column coloring, a compact
  interface/status display, and better viewport rendering.

## [2.0.0] - 2026-04-02

- Added the alternate-screen interactive viewer with unbounded scrollback,
  cursor/page navigation, and search by payload bytes or arbitration ID.
- Added candump-compatible background log writing through
  `-f`/`--log-file`, with either an explicit path or an automatic candump-style
  filename.
- Added the `mcandump(1)` manual page.

## [1.0.0] - 2026-03-30

- Initial release with Classic CAN and CAN-FD SocketCAN capture.
- Added candump-style colored terminal output, timestamp modes, and quiet mode.
- Added ECUconnect Logger protocol forwarding over TCP with Zeroconf/mDNS
  discovery for CANcorder clients.
- Added non-blocking fan-out with dedicated recorder, display, and per-client
  writer threads.

[2.8.0]: https://github.com/mickeyl/mcandump/releases/tag/v2.8.0
[2.6.1]: https://github.com/mickeyl/mcandump/releases/tag/v2.6.1
[2.3.0]: https://github.com/mickeyl/mcandump/releases/tag/v2.3.0
[2.2.2]: https://github.com/mickeyl/mcandump/releases/tag/v2.2.2
[2.2.1]: https://github.com/mickeyl/mcandump/releases/tag/v2.2.1
[2.2.0]: https://github.com/mickeyl/mcandump/releases/tag/v2.2.0
[2.1.0]: https://github.com/mickeyl/mcandump/releases/tag/v2.1.0
[2.0.0]: https://github.com/mickeyl/mcandump/releases/tag/v2.0.0
[1.0.0]: https://github.com/mickeyl/mcandump/releases/tag/v1.0.0
