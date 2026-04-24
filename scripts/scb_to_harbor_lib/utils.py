from __future__ import annotations

import re
import subprocess
from pathlib import Path

from .log import LOGGER


def run_command(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    LOGGER.debug("$ %s", format_command(cmd))
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def format_command(cmd: list[str]) -> str:
    return " ".join(shlex_quote(part) for part in cmd)


def shlex_quote(value: str) -> str:
    if not value:
        return "''"
    if re.fullmatch(r"[A-Za-z0-9_./:-]+", value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def dedupe_casefold(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def collapse_whitespace(text: str) -> str:
    return " ".join(text.split())


def git_revision(repo_root: Path) -> str:
    proc = run_command(["git", "rev-parse", "--short", "HEAD"], cwd=repo_root)
    if proc.returncode != 0:
        return "unknown"
    sha = proc.stdout.strip() or "unknown"

    dirty_proc = run_command(["git", "status", "--porcelain"], cwd=repo_root)
    if dirty_proc.returncode == 0 and dirty_proc.stdout.strip():
        sha += "-dirty"
    return sha
