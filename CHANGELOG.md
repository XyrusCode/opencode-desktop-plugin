# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-15

### Added
- `opencode-bridge` console script: spawns `opencode serve`, waits for it to
  answer `/global/health`, then serves the phone-facing WebSocket API.
- Session list, create, attach with history, prompt, and abort, mapped onto
  opencode's HTTP endpoints.
- Live streaming: opencode's `/event` SSE stream is translated into
  `output_delta` frames and fanned out to every paired phone.
- 64 tests. Unit tests for the frame protocol, config resolution, and event
  translation, plus end-to-end tests that run a real WebSocket client against a
  real bridge over real sockets with only opencode replaced by an in-process
  fake.

### Changed
- `BRIDGE_WORKDIR` defaults to your home directory instead of the process
  working directory. opencode scopes its session list to the directory it
  serves, so the old default showed the phone an empty list whenever the bridge
  was started from an arbitrary directory.

### Removed
- The old TUI-piping design, which spawned `opencode --pure` and wrote
  newline-delimited text to its stdin. opencode is a full-screen TUI on that
  path, so no output ever came back, and the phone parsed stdout as JSON.

## [0.1.0] - 2026-10-14

### Added
- Initial scaffolding: a WebSocket server that spawned opencode as a subprocess.
