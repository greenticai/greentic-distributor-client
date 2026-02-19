#!/usr/bin/env bash
set -euo pipefail

TOOLCHAIN=${TOOLCHAIN:-1.91.0}

run_cargo() {
  cargo +"$TOOLCHAIN" "$@"
}

echo ">> canonical-interfaces-guard"
if rg -n "greentic_interfaces::bindings::|greentic_interfaces_guest::bindings::|\\bbindings::greentic::" src tests README.md; then
  echo "ERROR: use canonical/facade modules instead of bindings::* paths"
  exit 1
fi

echo ">> fmt"
run_cargo fmt --all -- --check

echo ">> clippy"
run_cargo clippy --workspace --all-targets --all-features -- -D warnings

echo ">> tests"
export OCI_E2E=${OCI_E2E:-0}
export OCI_E2E_REF=${OCI_E2E_REF:-ghcr.io/greentic-ai/components/templates:latest}
run_cargo test --workspace --all-features
