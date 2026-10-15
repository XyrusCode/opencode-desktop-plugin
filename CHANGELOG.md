# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2026-10-15

### Added
- Rich response rendering parity with the desktop client: all finalized opencode
  parts (tool calls, tool results, file uploads, snapshot/patch diffs, errors)
  are forwarded to the phone as structured `part` frames instead of being
  dropped. Text and reasoning still stream live as deltas; everything else
  arrives as completed objects the phone can render as cards.
- Model and agent selectors on the phone. Per-message `model`/`agent` override on
  `POST /session/{id}/prompt_async` (the session-level setters return 500 on this
  opencode build, so selection is staged and applied to the next prompt).
- Project picker: `pick_project` stages a worktree, the next `create_session`
  lands there, and catalogs (`providers`, `agents`, `projects`) are pushed on
  auth/attach so the phone's pickers are populated up front.
- Permission (`once`/`always`/`reject`) and question (multi-select) prompts now
  surface on the phone instead of deadlocking the locked-away laptop. The bridge
  polls opencode every 2s as the reliable floor and also forwards any such events
  pushed over the `/event` stream as a fast path.
- Structured history: `attach` now sends structured parts (`role` + `parts`)
  rather than flattened lines, so the phone can render rich history (tool cards,
  diffs) too.

### Changed
- History frame shape: `type` is now `history` with a `messages: [{role, parts}]`
  payload. This is a coordinated desktop+mobile contract bump.

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
