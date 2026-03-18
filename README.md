# DWBase Workspace

DWBase is an agent-first memory store built around immutable "atoms", scoped worlds, recall-oriented queries, local durability, and Greentic capability-pack packaging. In practice, you run it as a small stateful service per environment or tenant boundary, optionally add swarm replication between nodes, and expose it either through the HTTP node API or the packaged Greentic component/gtpack.

## Why it’s different
- Agent-first atoms: immutable, world-scoped records with labels/flags/links and recency-weighted recall.
- Reflex pipeline: remember → embed (optional) → index → streams → swarm replication hooks.
- Greentic-compatible: ships `component-dwbase` as a self-describing Greentic `component@0.6.0` package with QA/i18n support, plus `packs/dwbase-gtpack` as a capability-driven gtpack that can request public ingress.
- Observe streams: backpressure-aware subscriptions with drop/backoff metrics and replay protection.
- Storage: sled-backed append-only log with secondary indexes, repair, and retention policy hooks.
- Vector: optional HNSW per-world ANN for reranking.
- Security: capability-aware policies, rate limits, and replication allow/deny lists.
- Swarm: durable peer membership, capability-aware replication filters, and replay protection.
- Ops/UX: Prometheus-compatible metrics via `/metrics`, CLI + HTTP node, local/devnet deploy helpers, backup/restore, and gtpack packaging.

## Production shape
- Single node: the common production shape is one `dwbase-node` process with a persistent `data_dir`, fronted by normal ingress or an internal service mesh.
- Multi-node: add NATS and swarm subscriptions only when you need selected worlds replicated between DWBase instances.
- Integration: use the HTTP API directly for services and workers, or ship the DWBase capability pack when DWBase needs to plug into Greentic flows/operators.
- Isolation: model tenancy through world naming and enforce access with capability/rate-limit policy.

## Install the CLI fast (cargo-binstall)
Prebuilt binaries are published for macOS (arm64 + x86_64), Linux (x86_64-gnu), and Windows (x86_64-msvc).
```bash
cargo binstall dwbase-cli

# binary name
dwbase --help
```

From source:
```bash
rustup target add wasm32-wasip2
cargo build -p dwbase-cli
```

## Quick start (remember, ask, replay)
```bash
# install CLI with prebuilt binaries (macOS/Linux/Windows)
cargo binstall dwbase-cli -y

# minimal config
cp config.example.toml config.toml

# start the node using the CLI launcher
dwbase deploy local --config ./config.toml

# remember (HTTP). Leave timestamp empty to auto-fill; importance must be 0.0–1.0.
curl -X POST http://127.0.0.1:8080/remember \
  -H "content-type: application/json" \
  -d '{"world":"demo","worker":"cli","kind":"observation","timestamp":"","importance":0.9,"payload_json":"{\"text\":\"checkout latency 820ms\"}","vector":null,"flags":[],"labels":["latency"],"links":[]}'

# ask (CLI); prompts for text if omitted
dwbase ask demo --question "Why is checkout slow?"

# replay most recent atoms
dwbase replay demo --limit 5

# list known worlds tracked since node start
dwbase list-worlds
```

Expected ask output (pretty JSON): supporting atoms sorted by timestamp, then importance; `text` is `"answer-pending"` because the engine returns raw supporting atoms.

For deployment patterns beyond a single local node, see [docs/deployment.md](docs/deployment.md).

## Concepts in action
- **Atom shape** (immutable): 
  ```json
  {"id":"atom-123","world":"demo","worker":"cli","kind":"observation","timestamp":"2024-01-01T00:00:01Z","importance":0.7,"payload_json":"{\"text\":\"checkout latency 820ms\"}","vector":null,"flags":["hot"],"labels":["latency"],"links":["incident-42"]}
  ```
- **Remember** (creates an atom, auto-fills timestamp if empty): call the HTTP API (no CLI subcommand):
  ```bash
  curl -X POST http://127.0.0.1:8080/remember \
    -H "content-type: application/json" \
    -d '{"world":"demo","worker":"cli","kind":"observation","timestamp":"","importance":0.9,"payload_json":"{\"text\":\"web latency 820ms\"}","vector":null,"flags":[],"labels":["latency"],"links":[]}'
  ```
- **Ask** (recency-first, importance-second; uses reflex index + optional ANN):
  ```bash
  dwbase ask demo --question "what happened to checkout?"
  ```
  Returns supporting atoms; you decide how to answer.
- **Replay** (ordered scan with filters):
  ```bash
  dwbase replay demo --limit 20 --since "2024-01-01T00:00:00Z"
  ```
- **Observe streams** (HTTP API; CLI does not expose observe endpoints):
  ```bash
  curl -X POST http://127.0.0.1:8080/observe/start \
    -H "content-type: application/json" \
    -d '{"world_key":"demo","kinds":["observation"],"labels":["latency"]}'
  # capture the handle from the JSON response, then poll:
  curl -X POST http://127.0.0.1:8080/observe/poll \
    -H "content-type: application/json" \
    -d '{"handle":1,"max":10}'
  ```
  Streams are bounded with backpressure; duplicates are dropped via per-peer replay protection.
- **Metrics snapshot** (WASI-friendly):
  ```bash
  # metrics: scrape http://127.0.0.1:8080/metrics (Prometheus text) when built with `metrics` feature
  ```

## Swarms: how nodes find and trust each other
- Enable presence/replication: set `DWBASE_SWARM_ENABLE=1` (component) or configure NATS in node env (`NATS_URL=...`). For offline demos, use `DWBASE_SWARM_MOCK=1`.
- Discovery: nodes broadcast on `dwbase.node.hello`, keep a TTL peer table, and persist swarm state to `<DATA_DIR>/swarm.json` so membership survives restarts.
- Subscriptions: set `DWBASE_SUBSCRIBE_WORLDS=demo,tenant:*` to declare what you want; peers only send worlds you’re allowed to receive (policy-aware filters in `dwbase-swarm-nats`).
- Replay protection/backpressure: inboxes are bounded; per-peer counters drop duplicates and stale batches.
- Current state: presence + capability-aware replication skeleton are implemented; full multi-node consistency/ANN sync/quarantine is intentionally out of scope for v1.

## Ignoring bad digital workers (isolation)
- Capabilities: configure `read_worlds` / `write_worlds` and importance caps in `config.toml` to fence what a worker can touch; out-of-policy writes are rejected.
- Rate limits: per-worker remember/ask rate caps protect hot paths.
- Replication policy: allow/deny prefixes (`replication_allow`/`replication_deny`) gate what peers can send/receive.
- Not yet: automated “quarantine” of misbehaving workers is a planned future addition; today enforcement is via the caps above and operator policy atoms.

## Concepts (v1)
- **Atom:** immutable record with `id`, `world`, `worker`, `kind`, ISO-8601 `timestamp`, `importance` (0.0–1.0), `payload_json` string, optional vector, flags/labels/links.
- **World:** namespace for atoms (e.g., `incident:42`); storage, vector, and streams are world-scoped.
- **remember:** append an atom; vector is embedded if missing; published to streams and reflex index.
- **ask:** return ranked atoms for a question (recency-first, then importance) using reflex index, storage scan, and optional ANN vector search.
- **observe:** publish an already-built atom to subscribers (no persistence).
- **replay:** ordered scan of stored atoms with filters (world, kinds, labels, flags, since/until, limit).

## What DWBase is not
- Not a relational or document database.
- Not a general-purpose vector store (ANN is in-memory per world; no durability).
- Not a fully automatic distributed database; replication is selective and policy-driven, and the simplest production setup is still a single node with persistent local storage.

## Workspace layout
- `crates/dwbase-core` — atom model.
- `crates/dwbase-engine` — orchestrator for remember/ask/observe/replay (+ optional metrics).
- `crates/dwbase-storage-sled` — sled-backed append-only storage with secondary index.
- `crates/dwbase-vector-hnsw` — in-memory HNSW ANN per world.
- `crates/dwbase-stream-local` — in-process pub/sub with backpressure metrics.
- `crates/dwbase-security` — capabilities + rate limits; trust scores recorded to metrics only.
- `crates/dwbase-node` — Axum HTTP API.
- `crates/dwbase-cli` — HTTP client (binary name: `dwbase`).
- `crates/dwbase-wit-host` / `crates/dwbase-wit-guest` — WIT bindings.
- `crates/dwbase-embedder-dummy`, `dwbase-metrics`, `dwbase-swarm` — adapters and scaffolding.
- `crates/component-dwbase` — Greentic `component@0.6.0` package scaffold with self-describe, QA, ingress-aware setup, and i18n support.
- `packs/dwbase-gtpack` — capability-driven gtpack for the Greentic component, including `greentic.ext.capabilities.v1` and ingress-control dependency metadata.

## Learn more
- `docs/overview.md` — DWBase in one page.
- `docs/cli.md` — CLI commands with real outputs.
- `docs/deployment.md` — local, edge, and devnet deployment patterns.
- `docs/component-dwbase.md` — current Greentic component model, wizard flow, and build pipeline.
- `docs/developer/` — architecture, engine, storage, vector, security, WIT details.
- `examples/` — runnable scenarios (`basic_memory`, `observe_stream`, `replay_simulation`, `worlds_and_forks`, `wasm-worker`).

## Building and testing
```bash
cargo fmt
cargo clippy -- -D warnings
cargo test

# Greentic component + gtpack pipeline
rustup target add wasm32-wasip2
greentic-component build --manifest crates/component-dwbase/component.manifest.json
greentic-component doctor \
  crates/component-dwbase/target/wasm32-wasip2/release/component_dwbase.wasm \
  --manifest crates/component-dwbase/component.manifest.json
packc build --in packs/dwbase-gtpack --gtpack-out packs/dwbase-gtpack/dist/dwbase.gtpack
# sign/verify with your own Ed25519 keypair (packc verify requires the public key)
packc sign --pack packs/dwbase-gtpack --manifest packs/dwbase-gtpack/dist/manifest.cbor --key <private-key>
packc verify --pack packs/dwbase-gtpack --manifest packs/dwbase-gtpack/dist/manifest.cbor --key <public-key> --offline
```
