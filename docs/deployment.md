# Deployment

This guide covers running DWBase locally, on constrained edge devices, and as a small devnet of isolated nodes. No external orchestrators or managed services are required.

For Greentic deployments, DWBase is now best thought of as a capability pack plus a stateful service. The service still runs as `dwbase-node`, but the Greentic-facing packaging in `packs/dwbase-gtpack` now advertises a DWBase capability offer and expects public ingress to provide `public_base_url` when externally reachable APIs are enabled.

## Greentic Capability-Pack Deployment

High-level shape:

- `dwbase-node` remains the stateful process that owns sled data and HTTP APIs.
- `component-dwbase` is the self-describing Greentic component that validates and normalizes DWBase setup.
- `packs/dwbase-gtpack` exposes `greentic.ext.capabilities.v1` with capability `greentic.cap.dwbase.memory.v1`.
- The pack also declares a dependency on `routing.ingress.control.chain` and requires capability `greentic.cap.ingress.control.v1`.

Expected setup inputs for the pack:

- `data_dir`
- `default_tenant`
- `public_base_url`
- optional `public_path_prefix`
- optional `nats_url`
- optional `swarm_enable`

For internal-only DWBase deployments, you can still run `dwbase-node` directly without Greentic ingress. For public Greentic deployments, `greentic-start` / `greentic-operator` should allocate ingress and pass `public_base_url` into the DWBase component setup flow.

## Commands (dwbase-cli)

Local (single node):
```bash
cargo run -p dwbase-cli -- deploy local --config deploy/local.toml
```

Edge (offline-first):
```bash
cargo run -p dwbase-cli -- deploy local --config deploy/edge.toml
```

Devnet:
```bash
# spawn 3 nodes on ports 7000,7001,7002 with per-node data dirs under .dwbase/devnet
cargo run -p dwbase-cli -- deploy devnet --nodes 3 --base-port 7000

# stop running devnet nodes (uses recorded PIDs in .dwbase/devnet/state.json)
cargo run -p dwbase-cli -- deploy stop

# remove devnet data dirs
cargo run -p dwbase-cli -- deploy clean --yes
```

Outputs now include listen address, data directory, PID, config path, and log path. Devnet prints a table of nodes with ids, ports, and data dirs plus the state file location. Pressing Ctrl+C during `deploy devnet` will terminate any nodes that were spawned in that run.

## Directory layout
- `<DWBASE_DATA_DIR|.dwbase>/local` — logs for local runs.
- `<DWBASE_DATA_DIR|.dwbase>/devnet` — per-node data/log/config plus `state.json` with PIDs.
- `deploy/*.toml` — config templates for local/devnet/edge (documenting ports, storage paths, health thresholds).

`DWBASE_DATA_DIR` overrides the base `.dwbase` path used by the deploy helper; per-node `data_dir` in configs remains unchanged unless you edit the template.

## Configuration templates
- `deploy/local.toml` — single-node default (127.0.0.1:8080, data in .dwbase/local, permissive caps).
- `deploy/devnet.toml` — base template; ports/data dirs are rewritten per node by the deploy helper.
- `deploy/edge.toml` — tighter caps, lower rate limits, bind 0.0.0.0 for edge use, data under .dwbase/edge.

Templates document storage paths, rate limits, and health thresholds. Adjust `data_dir` to point at an attached volume on edge devices. Resource caps can be provided via env (`DWBASE_MAX_RAM_MB`, `DWBASE_MAX_DISK_MB`) and are forwarded to nodes (best-effort).

## Environment overrides
- `DWBASE_DATA_DIR` — root directory for deploy helper state/logs/configs (`.dwbase` by default).
- `DWBASE_MAX_RAM_MB`, `DWBASE_MAX_DISK_MB` — forwarded to node processes (best-effort; node does not enforce limits in v1).

## Offline / edge notes
- Nodes have no hard dependency on swarm/peers; they operate standalone and start cleanly without network access.
- Use the edge template for constrained hosts (lower rate limits, 0.0.0.0 binding) and point `data_dir` to persistent storage.
- Health endpoint `/health` reports disk usage and degraded state if usage exceeds `disk_usage_degraded`.
- ANN indexes are in-memory; restarting an edge node clears vector data (storage is durable via sled).
- Keep `data_dir` on persistent storage; logs are written beside configs via deploy helper.

## Failure modes and recovery
- **Port in use:** adjust `listen` in config (or `--base-port` for devnet). The deploy helper rewrites devnet ports automatically.
- **Disk pressure:** `/health` returns `degraded`; free space or raise `disk_usage_degraded` temporarily.
- **Stale devnet state or orphan PIDs:** `dwbase deploy stop` clears running nodes; `deploy devnet` prunes dead PIDs before starting. Use `dwbase deploy clean --yes` to remove old dirs/state.
- **Offline start:** configs work without network; avoid pointing `--api` at remote hosts when offline.

## Safe reset
- For a single node: stop the process, optionally move `data_dir` aside, and restart with a fresh `data_dir`.
- For devnet: `dwbase deploy stop` → `dwbase deploy clean --yes` → restart with `deploy devnet ...`.

## Backups and restore
- Snapshot format: `.tzst` (tar + zstd) containing `data/` (sled files) and `metadata.json` (format version, creation time, index metadata from `/index/status` when reachable).
- Create:
  ```bash
  cargo run -p dwbase-cli -- backup create --data-dir ./data --out snapshot.tzst --api http://127.0.0.1:8080
  ```
  If the node is offline, the snapshot still succeeds with empty index metadata.
- Restore (overwrites target dir with `--force`):
  ```bash
  cargo run -p dwbase-cli -- backup restore --input snapshot.tzst --data-dir ./data --force
  ```
  After restore, a `.snapshot_metadata.json` is written under the data dir for audit. Start the node normally; if vector index files are missing, the node will rebuild automatically on first ask/remember.

## Observability
- Metrics: enable the default `metrics` feature and scrape `http://<node>/metrics` (Prometheus text format). Counters/gauges include remember/ask latency, disk usage, GC evictions, trust distribution.
- Health:
  - `/healthz` — liveness (always 200 when process is up).
  - `/readyz` — readiness; returns 503 when disk usage exceeds the configured threshold, storage is unavailable, or any index is not ready.
- Tracing: every request gets an `x-request-id` (generated if missing) attached to the tracing span; handlers emit spans for remember/ask/replay to aid correlation.

## Soak / chaos testing
- Run a local mixed workload and failure injection:
  ```bash
  cargo run -p dwbase-cli -- test soak --duration 5m --report soak-report.json
  ```
- The harness generates a report (latency p50/p95, disk delta, restart/corruption injection flags). Data is stored in a temp dir unless `--data-dir` is provided. Default CI does not run this test.
