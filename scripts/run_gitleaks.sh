#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${ROOT_DIR}/.gitleaks.toml"

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "gitleaks is required but not installed."
  echo "Install it before running this scan, for example: brew install gitleaks"
  exit 1
fi

cd "${ROOT_DIR}"

if [[ "${1:-}" == "--staged" ]]; then
  if [[ -z "$(git diff --cached --name-only --diff-filter=ACMR)" ]]; then
    echo "gitleaks: no staged files to scan."
    exit 0
  fi

  gitleaks protect \
    --staged \
    --redact \
    --config "${CONFIG_PATH}"
  exit 0
fi

gitleaks detect \
  --source . \
  --no-git \
  --redact \
  --config "${CONFIG_PATH}"
