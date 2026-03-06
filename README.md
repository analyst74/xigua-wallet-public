# Xigua Wallet

A collection of tools built around Lunch Money.

## Layout

- `pocketlunch/`: local LLM harness for reviewing and fixing Lunch Money transaction categories with full audit history.
- `.githooks/`: committed Git hooks for local checks
- `.github/workflows/`: CI workflows

Each tool or project should keep its implementation details and usage docs inside its own folder.

See [pocketlunch/README.md](pocketlunch/README.md) for `pocketlunch` usage and stack details.

## Development

Install the local commit hook path once per clone:

```bash
./scripts/install-hooks.sh
```

This configures Git to use the committed pre-commit hook in `.githooks/`.

## Repo Scans

Install `gitleaks` locally before committing:

```bash
brew install gitleaks
```

Run the full repo scan manually from the repository root:

```bash
./scripts/run_repo_scans.sh
```

The pre-commit hook runs these checks on staged content before local commits:

- privacy scan via `./scripts/run_privacy_scan.sh --staged`
- secret scan via `./scripts/run_gitleaks.sh --staged`
