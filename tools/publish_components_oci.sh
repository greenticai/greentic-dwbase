#!/usr/bin/env bash
set -euo pipefail

# Publish component-dwbase + the DWBase gtpack to GHCR as OCI artifacts, and emit lockfiles.
#
# Requirements:
# - `oras` available on PATH
# - `jq` available on PATH
#
# Auth:
# - expects `GITHUB_ACTOR` and `GITHUB_TOKEN` (or `GHCR_USER`/`GHCR_TOKEN`)
#
# Usage:
#   TAG=v0.1.0 ./tools/publish_components_oci.sh \
#     --component-wasm target/wasm32-wasip2/release/component_dwbase.wasm \
#     --component-manifest crates/component-dwbase/component.manifest.json \
#     --pack packs/dwbase-gtpack/dist/dwbase.gtpack

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TAG="${TAG:-}"
COMPONENT_WASM=""
COMPONENT_MANIFEST=""
PACK_FILE=""
OUT_DIR="${OUT_DIR:-$ROOT/dist}"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      TAG="$2"
      shift 2
      ;;
    --component-wasm)
      COMPONENT_WASM="$2"
      shift 2
      ;;
    --component-manifest)
      COMPONENT_MANIFEST="$2"
      shift 2
      ;;
    --pack)
      PACK_FILE="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift 1
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$TAG" ]]; then
  echo "ERROR: TAG not set (use --tag or env TAG=...)" >&2
  exit 2
fi
if [[ -z "$COMPONENT_WASM" || -z "$COMPONENT_MANIFEST" || -z "$PACK_FILE" ]]; then
  echo "ERROR: missing required args. Need --component-wasm, --component-manifest, --pack" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

if [[ ! -f "$COMPONENT_WASM" ]]; then
  echo "ERROR: missing component wasm: $COMPONENT_WASM" >&2
  exit 2
fi
if [[ ! -f "$COMPONENT_MANIFEST" ]]; then
  echo "ERROR: missing component manifest: $COMPONENT_MANIFEST" >&2
  exit 2
fi
if [[ ! -f "$PACK_FILE" ]]; then
  echo "ERROR: missing pack file: $PACK_FILE" >&2
  exit 2
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  OWNER="${GITHUB_REPOSITORY_OWNER:-${GHCR_OWNER:-owner}}"
  REGISTRY="${REGISTRY:-ghcr.io}"
  COMPONENT_REPO="${COMPONENT_REPO:-$REGISTRY/$OWNER/component-dwbase}"
  PACK_REPO="${PACK_REPO:-$REGISTRY/$OWNER/packs/dwbase/dwbase}"
  COMPONENT_REF="$COMPONENT_REPO:$TAG"
  PACK_REF="$PACK_REPO:$TAG"
  echo "==> Dry run"
  echo "Would publish:"
  echo " - component: $COMPONENT_REF"
  echo "   - $COMPONENT_WASM (application/wasm)"
  echo "   - $COMPONENT_MANIFEST (application/json)"
  echo " - pack: $PACK_REF"
  echo "   - $PACK_FILE (application/zip)"
  echo "Output lockfiles to: $OUT_DIR"
  exit 0
fi

GHCR_USER="${GHCR_USER:-${GITHUB_ACTOR:-}}"
GHCR_TOKEN="${GHCR_TOKEN:-${GITHUB_TOKEN:-}}"
if [[ -z "$GHCR_USER" || -z "$GHCR_TOKEN" ]]; then
  echo "ERROR: missing auth. Set GITHUB_ACTOR+GITHUB_TOKEN (recommended) or GHCR_USER+GHCR_TOKEN." >&2
  exit 2
fi

OWNER="${GITHUB_REPOSITORY_OWNER:-${GHCR_OWNER:-}}"
if [[ -z "$OWNER" ]]; then
  # best-effort fallback: infer from user
  OWNER="$GHCR_USER"
fi

REGISTRY="${REGISTRY:-ghcr.io}"

# Repos (OCI namespaces) for the artifacts.
COMPONENT_REPO="${COMPONENT_REPO:-$REGISTRY/$OWNER/component-dwbase}"
PACK_REPO="${PACK_REPO:-$REGISTRY/$OWNER/packs/dwbase/dwbase}"

COMPONENT_REF="$COMPONENT_REPO:$TAG"
PACK_REF="$PACK_REPO:$TAG"

if ! command -v oras >/dev/null 2>&1; then
  echo "ERROR: oras not found on PATH" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq not found on PATH" >&2
  exit 2
fi

echo "==> Logging into $REGISTRY as $GHCR_USER"
oras login "$REGISTRY" -u "$GHCR_USER" -p "$GHCR_TOKEN" >/dev/null

echo "==> Publishing component: $COMPONENT_REF"
oras push "$COMPONENT_REF" \
  --artifact-type "application/vnd.greentic.component.v1" \
  --annotation "org.opencontainers.image.title=component-dwbase" \
  --annotation "org.opencontainers.image.version=$TAG" \
  "$COMPONENT_WASM:application/wasm" \
  "$COMPONENT_MANIFEST:application/json"

echo "==> Publishing pack: $PACK_REF"
oras push "$PACK_REF" \
  --artifact-type "application/vnd.greentic.pack.v1" \
  --annotation "org.opencontainers.image.title=dwbase" \
  --annotation "org.opencontainers.image.version=$TAG" \
  "$PACK_FILE:application/zip"

COMPONENT_DESC="$(oras manifest fetch --descriptor "$COMPONENT_REF")"
PACK_DESC="$(oras manifest fetch --descriptor "$PACK_REF")"
COMPONENT_DIGEST="$(printf '%s' "$COMPONENT_DESC" | jq -r '.digest')"
PACK_DIGEST="$(printf '%s' "$PACK_DESC" | jq -r '.digest')"

GENERATED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

COMPONENT_LOCK="$OUT_DIR/components.lock.json"
PACK_LOCK="$OUT_DIR/packs.lock.json"

cat >"$COMPONENT_LOCK" <<JSON
{
  "generated_at": "$GENERATED_AT",
  "registry": "$REGISTRY",
  "components": [
    {
      "name": "component-dwbase",
      "ref": "$COMPONENT_REF",
      "digest": "$COMPONENT_DIGEST",
      "artifact_type": "application/vnd.greentic.component.v1"
    }
  ]
}
JSON

cat >"$PACK_LOCK" <<JSON
{
  "generated_at": "$GENERATED_AT",
  "registry": "$REGISTRY",
  "packs": [
    {
      "name": "dwbase",
      "ref": "$PACK_REF",
      "digest": "$PACK_DIGEST",
      "artifact_type": "application/vnd.greentic.pack.v1"
    }
  ]
}
JSON

echo "==> Wrote lockfiles:"
echo " - $COMPONENT_LOCK"
echo " - $PACK_LOCK"
