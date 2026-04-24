from __future__ import annotations

import json
from typing import Any


def render_inline_table(values: dict[str, Any]) -> str:
    parts: list[str] = []
    for key, value in values.items():
        parts.append(f"{key} = {toml_value(value)}")
    return "{ " + ", ".join(parts) + " }"


def toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def toml_float(value: float) -> str:
    formatted = f"{value:.10f}".rstrip("0").rstrip(".")
    if "." not in formatted:
        formatted += ".0"
    return formatted


def toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return toml_float(value)
    if isinstance(value, str):
        return toml_string(value)
    raise TypeError(f"Unsupported TOML value type: {type(value).__name__}")


def render_string_array(values: list[str], *, indent: int = 0) -> list[str]:
    prefix = " " * indent
    if not values:
        return [f"{prefix}[]"]
    lines = [f"{prefix}["]
    for value in values:
        lines.append(f"{prefix}  {toml_string(value)},")
    lines.append(f"{prefix}]")
    return lines
