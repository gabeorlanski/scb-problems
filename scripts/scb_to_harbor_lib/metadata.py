from __future__ import annotations

import re

from .constants import MANDATORY_KEYWORDS, MAX_KEYWORDS
from .errors import ConversionError

def parse_author(raw_author: str, *, problem_name: str) -> list[dict[str, str]]:
    text = raw_author.strip()
    if not text:
        raise ConversionError(f"{problem_name}: author is empty")

    match = re.match(r"^\s*(?P<name>[^<]+?)(?:\s*<(?P<email>[^>]+)>)?\s*$", text)
    if not match:
        raise ConversionError(
            f"{problem_name}: could not parse author field {raw_author!r}"
        )
    name = match.group("name").strip()
    email = (match.group("email") or "").strip()
    if not name:
        raise ConversionError(f"{problem_name}: author name is empty")

    author: dict[str, str] = {"name": name}
    if email:
        author["email"] = email
    return [author]

def build_keywords(tags: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()

    for tag in tags:
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(lowered)

    for suffix in MANDATORY_KEYWORDS:
        lowered = suffix.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(lowered)

    if len(deduped) <= MAX_KEYWORDS:
        return deduped

    mandatory_set = {value.lower() for value in MANDATORY_KEYWORDS}
    mandatory = [value for value in deduped if value in mandatory_set]
    non_mandatory = [value for value in deduped if value not in mandatory_set]

    available = max(0, MAX_KEYWORDS - len(mandatory))
    trimmed = non_mandatory[:available] + mandatory
    return trimmed[:MAX_KEYWORDS]

