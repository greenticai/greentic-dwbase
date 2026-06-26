# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## What this is

`greentic-dwbase` (DWBase) is an agent-first memory store for Greentic Digital Workers. It runs as a small stateful service per environment/tenant boundary and exposes itself two ways:

- **HTTP node** — `dwbase-node` binary serving `/remember`, `/ask`, `/replay`, `/worlds*`, `/atoms/{id}`, `/health[z]`, `/readyz`, `/metrics`.
- **WIT component** — `component-dwbase` (`greentic:component@0.6.0`) callable from Greentic flows, plus the capability-driven `packs/dwbase-gtpack`.

Core model: immutable, world-scoped **atoms** with labels/flags/links and recency-weighted recall. Adapters are pluggable: sled storage, optional HNSW vectors, local stream, optional NATS swarm replication, capability-aware gatekeeper.

This repo is **standalone** — it does not depend on the other `greentic-dw*` repos at runtime. It plugs in via the gtpack and the HTTP API.

## Build, test, verify

Canonical local CI (run from repo root):

```bash
bash ci/local_check.sh
```

That script: installs missing tools (`cargo-binstall`, `greentic-component`, `packc`, `cargo-component`) unless `CARGO_NET_OFFLINE=true`, then runs fmt → clippy (`--workspace --exclude component-dwbase --exclude dwbase-pack-runner -- -D warnings`) → tests (same exclusions) → adds wasm32-wasip2 → builds the component → hashes/stages/builds/signs/verifies the gtpack → schema-sync check → cargo package dry-run for publishable crates.

Set `CARGO_NET_OFFLINE=true` to skip tool installs, component build, gtpack steps, and packaging.

Day-to-day:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --exclude component-dwbase --exclude dwbase-pack-runner -- -D warnings
cargo test  --workspace --exclude component-dwbase --exclude dwbase-pack-runner
cargo test  -p dwbase-engine <name> -- --nocapture

# Run the node locally
dwbase deploy local --config ./config.toml
# or
cargo run -p dwbase-node -- --config ./config.toml
```

Toolchain pinned to **Rust 1.95.0** via canonical `rust-toolchain.toml`. The component crates target `wasm32-wasip2` — install with `rustup target add wasm32-wasip2`.

`Cargo.toml` declares **edition 2021** (intentional — older edition than the host repos because of WIT/wasm tooling constraints). Don't bump without coordinating component build.

## Workspace map

Workspace at root, members under `crates/*`:

| Crate | Role |
|-------|------|
| `dwbase-core` | Immutable data model — `AtomId`, `WorldKey`, `WorkerKey`, `AtomKind`, `Importance` (clamped 0..1), `Atom` builder/accessors |
| `dwbase-engine` | Engine traits (storage/vector/stream/embedder/gatekeeper), `ReflexIndex`, `DWBaseEngine` orchestrator, world lifecycle, retention/policy parsing, conflict marking |
| `dwbase-storage-sled` | Sled append-only storage with frame checksums, log recovery, optional encryption-at-rest hooks |
| `dwbase-vector-hnsw` | Per-world HNSW ANN, dim fixed on first insert, Euclidean distance |
| `dwbase-stream-local` | mpsc poll-based pub/sub with world/kind/label/flag/time filters |
| `dwbase-security` | Capabilities, trust, token-bucket rate limits, `LocalGatekeeper` |
| `dwbase-swarm`, `dwbase-swarm-nats` | Peer membership + NATS-backed presence, inbox messaging, selective replication, per-world atom event broadcast |
| `dwbase-metrics` | Counters/histograms/gauges for remember/ask latency, GC, trust; tracing alongside |
| `dwbase-embedder-dummy` | Test embedder returning `None` |
| `dwbase-wit-host`, `dwbase-wit-guest` | WIT bindings scaffolding for the engine world |
| `component-dwbase` | Production WASI component (LLM tool surface) — local persistence, observe streams, optional NATS, Prometheus snapshot |
| `dwbase-node` | Axum HTTP node binary |
| `dwbase-cli` | `dwbase` CLI launcher |
| `dwbase-bench` | Criterion + load tests for remember/ask/observe/replay |
| `dwbase-pack-runner` | gtpack runtime helper (excluded from default lint/test set) |

WIT contracts live in `wit/` (`dwbase-types.wit`, `dwbase-core.wit`). The gtpack lives in `packs/dwbase-gtpack/` (signed and verified by `local_check.sh`).

## Source-of-truth order

1. `.codex/repo_overview.md` and `.codex/STATE.json`.
2. `docs/` — `overview.md`, `architecture.md`, `cli.md`, `component-dwbase.md`, `deployment.md`, `production.md`, `wit.md`, `roadmap.md`, `performance.md`.
3. `crates/*/src/` and `wit/*.wit` — current code/schema beats stale prose.
4. `examples/`, `demo/`, `dwbase-data/`.

`SECURITY.md` documents the disclosure path. Don't reword it without security review.

## `.codex/` workflow (mandatory)

`.codex/global_rules.md` is present. The PRE-PR / POST-PR sync routine is the same as other Greentic repos:

1. Refresh `.codex/repo_overview.md` and `.codex/STATE.json` against current state before any change.
2. Implement; reuse shared Greentic crates where applicable.
3. Refresh again post-change, run `bash ci/local_check.sh`, and document any out-of-scope failures in the PR.

`.codex/PR-01.md` … `PR-50.md` capture the PR roadmap — read the relevant entry before scoping work.

## Reuse-first

DWBase intentionally has its own data model (`Atom`, `WorldKey`, …) — this is the canonical store, so don't redefine these elsewhere. But for capability declarations, errors, and packaging contracts, reuse:

- `greentic-cap-types` / `greentic-cap-*` for capability/pack patterns
- `greentic-component` and `greentic-pack` toolchain (consumed by `local_check.sh`)
- `greentic-types` for shared cross-repo DTOs when integrating from outside

Don't import other `greentic-dw*` crates here — DWBase ships independently.

## Style guardrails

- License: **Apache-2.0** (the only repo in this trio not under MIT). Keep `LICENSE` and crate metadata aligned.
- English only in source, tests, comments, commits, tracing.
- `#![forbid(unsafe_code)]` at crate roots.
- No `unwrap()` / `panic!()` in production paths; `anyhow`/`thiserror`.
- Conventional Commits.
- **Do not** add Claude co-authorship trailers or "Generated with Claude Code" lines on commits or PR bodies.
- `Cargo.lock` is committed.
- Husky / `.githooks/` may run `local_check.sh` — never bypass with `--no-verify`.

## Branching and release

`main` is default; `develop` exists. Releases are tag-driven: `release.yml` publishes OCI artifacts and `tag-on-version-bump.yml` auto-tags on `Cargo.toml` version diff. If `release.yml` fails, bump the version (don't re-tag) — the auto-tag workflow only fires on a version diff. The OCI image tag convention is `:stable` (not `:latest`) — see the meta-workspace memory on the May 2026 migration before changing publish targets.
