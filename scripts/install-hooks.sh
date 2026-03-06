#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT_DIR}"

git config core.hooksPath .githooks
chmod +x "${ROOT_DIR}/.githooks/pre-commit"
chmod +x "${ROOT_DIR}/scripts/"*.sh

echo "Configured git hooks to use ${ROOT_DIR}/.githooks."

if ! command -v gitleaks >/dev/null 2>&1; then
  echo "gitleaks is not installed. Install it before committing, for example:"
  echo "  brew install gitleaks"
fi
