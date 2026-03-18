#!/usr/bin/env python3
"""Best-effort manifest sanity check for component-dwbase.

Validates:
- component.manifest.json parses as JSON
- world matches greentic:component/component@0.6.0
- component artifact path is present
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "crates/component-dwbase/component.manifest.json"
EXPECTED_WORLD = "greentic:component/component@0.6.0"


def main() -> int:
    try:
        data = json.loads(MANIFEST.read_text())
    except json.JSONDecodeError as exc:
        print(f"manifest JSON invalid: {exc}", file=sys.stderr)
        return 1

    if not isinstance(data, dict):
        print("manifest JSON is not an object", file=sys.stderr)
        return 1

    world = data.get("world")
    if world != EXPECTED_WORLD:
        print(f"world mismatch: expected {EXPECTED_WORLD}, found {world}", file=sys.stderr)
        return 1

    artifact = (
        data.get("artifacts", {})
        if isinstance(data.get("artifacts"), dict)
        else {}
    )
    wasm = None
    if isinstance(artifact, dict):
        wasm = artifact.get("component_wasm")
    if not wasm or not isinstance(wasm, str):
        print("component_wasm missing from manifest", file=sys.stderr)
        return 1

    print(f"manifest ok: world={world}, component_wasm={wasm}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
