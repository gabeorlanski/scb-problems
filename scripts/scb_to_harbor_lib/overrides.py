from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from .constants import ALLOWED_OVERRIDE_ENV_KEYS, ALLOWED_OVERRIDE_KEYS
from .errors import CliError, ConversionError


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def validate_test_sh_snippet_override(problem_name: str, key: str, value: Any) -> None:
    if key == "test_sh_snippet":
        if not isinstance(value, str):
            raise ConversionError(
                f"{problem_name}: test_sh_snippet override must be a string"
            )
        return

    if isinstance(value, str) or _is_string_list(value):
        return

    if isinstance(value, dict):
        for snippet_key, snippet_value in value.items():
            if not isinstance(snippet_key, str):
                raise ConversionError(
                    f"{problem_name}: test_sh_snippets keys must be strings"
                )
            if not isinstance(snippet_value, str) and not _is_string_list(snippet_value):
                raise ConversionError(
                    f"{problem_name}: test_sh_snippets[{snippet_key!r}] must be "
                    "a string or list of strings"
                )
        return

    raise ConversionError(
        f"{problem_name}: test_sh_snippets override must be a string, "
        "list of strings, or table of checkpoint names to snippets"
    )

def resolve_default_overrides_path(repo_root: Path, cli_value: Path | None) -> Path | None:
    if cli_value is not None:
        return cli_value
    default_path = repo_root / "conversion.toml"
    if default_path.exists():
        return default_path
    return None

def load_overrides(
    path: Path | None,
    *,
    known_problem_names: set[str],
) -> tuple[dict[str, dict[str, Any]], Path | None]:
    if path is None:
        return {}, None

    if not path.exists():
        raise CliError(f"Overrides file does not exist: {path}")

    try:
        parsed = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as exc:
        raise ConversionError(f"Invalid TOML in overrides file {path}: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ConversionError(f"Overrides file {path} must parse to a TOML table")

    overrides: dict[str, dict[str, Any]] = {}

    for problem_name, value in parsed.items():
        if not isinstance(problem_name, str):
            raise ConversionError(
                f"Overrides file {path}: top-level keys must be problem names"
            )
        if problem_name not in known_problem_names:
            raise ConversionError(
                f"conversion.toml references unknown problem name: {problem_name}"
            )

        if not isinstance(value, dict):
            raise ConversionError(
                f"Overrides file {path}: section [{problem_name}] must be a table"
            )

        unknown = set(value.keys()) - ALLOWED_OVERRIDE_KEYS
        if unknown:
            raise ConversionError(
                f"{problem_name}: unknown override key(s): {sorted(unknown)}"
            )

        if "environment" in value:
            environment_value = value["environment"]
            if not isinstance(environment_value, dict):
                raise ConversionError(
                    f"{problem_name}: override environment must be a table"
                )
            env_unknown = set(environment_value.keys()) - ALLOWED_OVERRIDE_ENV_KEYS
            if env_unknown:
                raise ConversionError(
                    f"{problem_name}: unknown environment override key(s): "
                    f"{sorted(env_unknown)}"
                )

        if "environment_env" in value and not isinstance(
            value["environment_env"], dict
        ):
            raise ConversionError(
                f"{problem_name}: environment_env override must be a table"
            )

        if "dockerfile" in value:
            dockerfile_value = value["dockerfile"]
            if not isinstance(dockerfile_value, str) or not dockerfile_value.strip():
                raise ConversionError(
                    f"{problem_name}: dockerfile override must be a non-empty string"
                )
            resolved = (path.parent / dockerfile_value).resolve()
            if not resolved.exists():
                raise ConversionError(
                    f"{problem_name}: override dockerfile path does not exist: {resolved}"
                )
            value = dict(value)
            value["dockerfile"] = resolved

        if "entrypoint_command" in value and (
            not isinstance(value["entrypoint_command"], str)
            or not value["entrypoint_command"].strip()
        ):
            raise ConversionError(
                f"{problem_name}: entrypoint_command override must be a non-empty string"
            )

        if "dockerfile_preset" in value:
            preset = value["dockerfile_preset"]
            if preset not in {"python-light", "python-heavy"}:
                raise ConversionError(
                    f"{problem_name}: dockerfile_preset must be python-light or python-heavy"
                )

        if "min_reward" in value:
            min_reward = value["min_reward"]
            if not isinstance(min_reward, dict) or not min_reward:
                raise ConversionError(
                    f"{problem_name}: min_reward override must be a non-empty table"
                )
            if len(min_reward) != 1:
                raise ConversionError(
                    f"{problem_name}: min_reward override must contain exactly one key "
                    "because Harbor job metrics require scalar rewards"
                )

        for snippet_key in ("test_sh_snippet", "test_sh_snippets"):
            if snippet_key in value:
                validate_test_sh_snippet_override(
                    problem_name,
                    snippet_key,
                    value[snippet_key],
                )

        if "artifacts" in value and "task_artifacts" in value:
            raise ConversionError(
                f"{problem_name}: use either artifacts or task_artifacts, not both"
            )

        overrides[problem_name] = dict(value)

    return overrides, path

