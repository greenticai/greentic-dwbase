# dwbase-cli Commands

Targets a running `dwbase-node` (default `http://127.0.0.1:8080`). Override with `--api <url>`.

## Install
- Prebuilt binaries: `cargo binstall dwbase-cli` (binary name: `dwbase`; macOS arm/x86, Linux x86_64-gnu + arm64-gnu, Windows x86_64-msvc + arm64-msvc).
- From source: `cargo build -p dwbase-cli --release` then use `target/release/dwbase`.

## list-worlds
Worlds remembered since the node started (kept in-memory).
```bash
cargo run -p dwbase-cli -- list-worlds
```
Example:
```json
["demo","alpha","beta"]
```

## ask
Ask within a world; prompts if `--question` is omitted.
```bash
cargo run -p dwbase-cli -- ask demo --question "Why is checkout slow?"
```
Output shape:
```json
{
  "world": "demo",
  "text": "answer-pending",
  "supporting_atoms": [
    {"id":"atom-2","timestamp":"2024-01-01T00:00:01Z","importance":0.7,"payload_json":"{\"text\":\"...\"}","labels":["..."],"...":"..."}
  ]
}
```
Atoms are sorted by timestamp desc, then importance.

## replay
Replay ordered atoms for a world with optional limit/since filters.
```bash
cargo run -p dwbase-cli -- replay demo --limit 5 --since "2024-01-01T00:00:00Z"
```
Output example:
```json
[
  {"id":"atom-1","world":"demo","timestamp":"2024-01-01T00:00:00Z","kind":"observation","payload_json":"{\"text\":\"checkout latency 820ms\"}","labels":["latency"],"...":"..."}
]
```

## inspect (helper)
Fetch a single atom by id.
```bash
cargo run -p dwbase-cli -- inspect atom-1
```

## world create/archive/resume
Create and manage world metadata (archived worlds are hidden from `list-worlds`).
```bash
cargo run -p dwbase-cli -- world create tenant:demo/chat --description "chat" --labels "team:demo,env:dev"
cargo run -p dwbase-cli -- world archive tenant:demo/chat
cargo run -p dwbase-cli -- world resume tenant:demo/chat
```

## world policy (set)
Emit policy atoms for retention/importance/replication. By default, policies are stored in `policy:<world>`; pass `--world tenant:<id>/policy` to seed tenant-wide policy for WASI/component deployments.
```bash
cargo run -p dwbase-cli -- world policy --world tenant:demo/chat --retention-days 30 --min-importance 0.3 --replication-allow "tenant:demo/"
cargo run -p dwbase-cli -- world policy --world tenant:demo/policy --replication-deny "tenant:demo/private"
```
At least one policy field must be provided.
