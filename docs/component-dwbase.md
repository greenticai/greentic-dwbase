# component-dwbase Runbook

`component-dwbase` now follows the Greentic `component@0.6.0` wizard scaffold model instead of the older `node@0.5.0` shim. The important changes are:

- The component is self-describing through `describe` + `invoke`.
- The build path is `greentic-component build --manifest ...`, which wraps the `node@0.6.0` export into the full component contract and embeds the manifest section used by `doctor`.
- QA and configuration prompts live in source code through `qa-spec`, `apply-answers`, `dwbase.configure`, and `i18n-keys`.
- Localized strings are embedded at build time from `assets/i18n/*.json`.
- The paired gtpack now exposes `greentic.ext.capabilities.v1` and expects Greentic ingress to inject `public_base_url`.

## Wizard Flow

Interactive dry-run with answer recording:

```bash
greentic-component wizard --dry-run --emit-answers /tmp/myanswers.json
```

The wizard prompts for component name, output directory, operations, runtime capabilities, and config fields. In dry-run mode it emits a plan and can also emit replayable answers.

Non-interactive replay from recorded answers:

```bash
greentic-component wizard --answers /tmp/myanswers.json
```

That recreates the same scaffold automatically.

For automated runs, the wizard also accepts QA-style input files:

```bash
greentic-component wizard \
  --mode create \
  --dry-run \
  --qa-answers /tmp/dwbase-wizard.qa.json \
  --emit-answers /tmp/dwbase-wizard.answers.json \
  --plan-out /tmp/dwbase-wizard.plan.json
```

The emitted `answers.json` is the replayable form used with `--answers`.

## Repository Layout

`crates/component-dwbase` now contains the same moving parts the wizard generates:

- `src/lib.rs`: v0.6 `describe`/`invoke` entrypoint and operation dispatcher.
- `src/qa.rs`: `qa-spec` and `apply-answers` logic.
- `src/i18n.rs`: runtime lookup over the embedded locale bundle.
- `src/i18n_bundle.rs`: build-time/runtime CBOR pack and unpack helpers.
- `assets/i18n/en.json`: source translation keys for QA/setup text.
- `build.rs`: embeds the locale bundle into the final wasm.
- `component.manifest.json`: declarative manifest used by `greentic-component build`, `hash`, and `doctor`.

## Build And Validate

Build the self-describing wasm component:

```bash
greentic-component build --manifest crates/component-dwbase/component.manifest.json
```

`component-dwbase` carries its own local cargo target dir at
`crates/component-dwbase/target`, so the manifest can keep the wizard-standard
`target/wasm32-wasip2/...` artifact path even inside the workspace.

Refresh the manifest hash against the produced wasm:

```bash
greentic-component hash \
  --wasm crates/component-dwbase/target/wasm32-wasip2/release/component_dwbase.wasm \
  crates/component-dwbase/component.manifest.json
```

Run doctor against the built wasm plus source manifest:

```bash
greentic-component doctor \
  crates/component-dwbase/target/wasm32-wasip2/release/component_dwbase.wasm \
  --manifest crates/component-dwbase/component.manifest.json
```

`ci/local_check.sh` now uses that flow directly.

## Current DWBase-Specific Surface

The migrated component currently exposes:

- `dwbase.configure`
- `dwbase.requirements`
- `dwbase.echo`
- `qa-spec`
- `apply-answers`
- `i18n-keys`

The QA prompts are DWBase-oriented and cover:

- `data_dir`
- `default_tenant`
- `public_base_url`
- `public_path_prefix`
- `nats_url`
- `swarm_enable`

`dwbase.configure` returns normalized config for the DWBase capability pack, including derived ingress metadata such as `public_api_base_url`. `dwbase.requirements` exposes the pack-level requirement summary that the gtpack mirrors in `greentic.ext.capabilities.v1`.

## GTPack Shape

`packs/dwbase-gtpack/pack.yaml` now treats DWBase as a capability-driven pack:

- capability offer: `greentic.cap.dwbase.memory.v1`
- provider op: `dwbase.configure`
- setup source: component QA via `setup.qa_ref: components`
- ingress requirement: pack metadata declares `requires_http_ingress: true`
- ingress control dependency: `routing.ingress.control.chain` with required capability `greentic.cap.ingress.control.v1`

That means this repository now expects `greentic-start` / `greentic-operator` to provide `public_base_url` during setup for public DWBase deployments.
