#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
import re


SKIP_DIRS = {
    ".git",
    ".venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "node_modules",
    "target",
}

CONTENT_SCAN_EXEMPT_PATHS = {
    Path("scripts/privacy_scan.py"),
}

PATH_RULES = [
    (
        "env file",
        re.compile(r"(^|/)\.env($|\.local$|\.development$|\.production$|\.test$|\.staging$)"),
    ),
    (
        "database file",
        re.compile(r"(^|/).+\.(?:db|sqlite|sqlite3|sqlitedb)(?:-(?:shm|wal))?$"),
    ),
    (
        "database sidecar file",
        re.compile(r"(^|/).+\.db-(?:shm|wal)$"),
    ),
    (
        "generated result bundle",
        re.compile(
            r"(^|/)(?:scan-results|scan_artifacts|artifacts)(/|$)|"
            r"(^|/).*(?:gitleaks|privacy|secret|scan|findings|report|results?|bundle).*\."
            r"(?:sarif|json|csv|zip|tar|tgz|tar\.gz)$"
        ),
    ),
]

CONTENT_RULES = [
    (
        "local filesystem path",
        re.compile(
            r"(/Users/[^\s\"'`]+|/home/[^\s\"'`]+|C:\\Users\\[^\s\"'`]+|"
            r"/private/var/folders/[^\s\"'`]+|file:///[^ \n\r\t\"'`]+)"
        ),
    ),
    (
        "personal email address",
        re.compile(
            r"\b[A-Z0-9._%+-]+@(?:gmail|googlemail|yahoo|ymail|hotmail|outlook|live|msn|"
            r"icloud|me|mac|proton(?:mail)?|pm\.me|fastmail|hey)\.[A-Z]{2,}\b",
            re.IGNORECASE,
        ),
    ),
]


@dataclass
class Finding:
    kind: str
    path: Path
    detail: str


def run_git(repo_root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout


def git_paths(repo_root: Path, staged: bool) -> list[Path]:
    if staged:
        raw = run_git(repo_root, "diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z")
        return [Path(p) for p in raw.decode("utf-8").split("\0") if p]

    tracked = run_git(repo_root, "ls-files", "-z")
    untracked = run_git(repo_root, "ls-files", "--others", "--exclude-standard", "-z")
    paths = {
        Path(p)
        for raw in (tracked, untracked)
        for p in raw.decode("utf-8").split("\0")
        if p
    }
    return sorted(paths)


def filesystem_paths(repo_root: Path) -> list[Path]:
    paths: list[Path] = []
    for path in repo_root.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.is_file():
            paths.append(path.relative_to(repo_root))
    return sorted(paths)


def is_probably_binary(path: Path) -> bool:
    try:
        data = path.read_bytes()
    except OSError:
        return False
    if b"\0" in data:
        return True
    try:
        data.decode("utf-8")
    except UnicodeDecodeError:
        return True
    return False


def content_excerpt(text: str, match: re.Match[str]) -> str:
    start = max(0, match.start() - 30)
    end = min(len(text), match.end() + 30)
    return text[start:end].replace("\n", "\\n")


def scan_path(repo_root: Path, rel_path: Path) -> list[Finding]:
    findings: list[Finding] = []
    path_text = rel_path.as_posix()

    for label, pattern in PATH_RULES:
        if pattern.search(path_text):
            findings.append(Finding(label, rel_path, path_text))

    full_path = repo_root / rel_path
    if (
        rel_path in CONTENT_SCAN_EXEMPT_PATHS
        or not full_path.exists()
        or not full_path.is_file()
        or is_probably_binary(full_path)
    ):
        return findings

    try:
        text = full_path.read_text(encoding="utf-8")
    except OSError:
        return findings

    for label, pattern in CONTENT_RULES:
        for match in pattern.finditer(text):
            findings.append(Finding(label, rel_path, content_excerpt(text, match)))

    return findings


def collect_paths(repo_root: Path, staged: bool) -> list[Path]:
    try:
        return git_paths(repo_root, staged)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return filesystem_paths(repo_root)


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan the repo for privacy-sensitive artifacts.")
    parser.add_argument(
        "--staged",
        action="store_true",
        help="Scan staged files only. Intended for the pre-commit hook.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    findings: list[Finding] = []

    for rel_path in collect_paths(repo_root, args.staged):
        findings.extend(scan_path(repo_root, rel_path))

    if findings:
        print("Privacy scan failed. Findings:")
        for finding in findings:
            print(f"- {finding.kind}: {finding.path} -> {finding.detail}")
        return 1

    mode = "staged files" if args.staged else "repository files"
    print(f"Privacy scan passed for {mode}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
