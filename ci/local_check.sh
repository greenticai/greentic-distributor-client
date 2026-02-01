#!/usr/bin/env bash
set -euo pipefail

TOOLCHAIN=${TOOLCHAIN:-1.90.0}

run_cargo() {
  cargo +"$TOOLCHAIN" "$@"
}

echo ">> fmt"
run_cargo fmt --all -- --check

echo ">> clippy"
run_cargo clippy --workspace --all-targets --all-features -- -D warnings

echo ">> tests"
export OCI_E2E=${OCI_E2E:-1}
export OCI_E2E_REF=${OCI_E2E_REF:-ghcr.io/greentic-ai/components/templates:latest}
run_cargo test --workspace --all-features
