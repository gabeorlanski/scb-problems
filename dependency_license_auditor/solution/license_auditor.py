#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict, deque
from datetime import date
from pathlib import Path


def fail(message: str) -> None:
    raise ValueError(message)


def parse_day(value: object, field: str) -> date:
    if not isinstance(value, str):
        fail(f"{field} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        fail(f"{field} must be YYYY-MM-DD")


def load(path: Path) -> dict:
    data = json.loads(path.read_text())
    if set(data) != {"as_of", "components", "dependencies", "policy", "exceptions"}:
        fail("exact top-level fields required")
    parse_day(data["as_of"], "as_of")
    components = data["components"]
    if not isinstance(components, list) or not components:
        fail("components must be non-empty")
    ids = [row.get("id") for row in components]
    if any(not isinstance(x, str) or not x for x in ids) or len(ids) != len(set(ids)):
        fail("component IDs must be unique strings")
    component_ids = set(ids)
    for row in components:
        if set(row) != {"id", "license", "ships"} or not isinstance(row["license"], str) or not isinstance(row["ships"], bool):
            fail("invalid component row")
    dependencies = data["dependencies"]
    if not isinstance(dependencies, list):
        fail("dependencies must be a list")
    edge_keys = []
    for edge in dependencies:
        if set(edge) != {"from", "to", "scope"} or edge["from"] not in component_ids or edge["to"] not in component_ids or edge["scope"] not in {"runtime", "build", "test"}:
            fail("invalid dependency edge")
        if edge["from"] == edge["to"]:
            fail("self dependency")
        edge_keys.append((edge["from"], edge["to"], edge["scope"]))
    if len(edge_keys) != len(set(edge_keys)):
        fail("duplicate dependency edge")
    policy = data["policy"]
    if set(policy) != {"allow", "review", "deny", "restrictive"}:
        fail("exact policy fields required")
    buckets = []
    for key in ("allow", "review", "deny", "restrictive"):
        if not isinstance(policy[key], list) or len(policy[key]) != len(set(policy[key])) or any(not isinstance(x, str) for x in policy[key]):
            fail(f"invalid policy {key}")
        if key != "restrictive":
            buckets.extend(policy[key])
    if len(buckets) != len(set(buckets)):
        fail("license appears in multiple decision buckets")
    exceptions = data["exceptions"]
    if not isinstance(exceptions, list):
        fail("exceptions must be a list")
    exception_ids = []
    for row in exceptions:
        if set(row) != {"id", "component", "license", "start", "end", "disposition"}:
            fail("invalid exception fields")
        if row["component"] not in component_ids or row["disposition"] not in {"allow", "review"}:
            fail("invalid exception scope")
        start, end = parse_day(row["start"], "start"), parse_day(row["end"], "end")
        if start > end:
            fail("exception start after end")
        exception_ids.append(row["id"])
    if any(not isinstance(x, str) or not x for x in exception_ids) or len(exception_ids) != len(set(exception_ids)):
        fail("exception IDs must be unique strings")
    return data


def audit(data: dict) -> dict:
    by_id = {row["id"]: row for row in data["components"]}
    policy = data["policy"]
    as_of = date.fromisoformat(data["as_of"])
    runtime: dict[str, list[str]] = defaultdict(list)
    for edge in data["dependencies"]:
        if edge["scope"] == "runtime":
            runtime[edge["from"]].append(edge["to"])
    for key in runtime:
        runtime[key].sort()

    active_exceptions = {}
    for row in sorted(data["exceptions"], key=lambda x: x["id"]):
        if date.fromisoformat(row["start"]) <= as_of <= date.fromisoformat(row["end"]):
            key = (row["component"], row["license"])
            if key in active_exceptions:
                fail("overlapping active exceptions")
            active_exceptions[key] = row

    def base_disposition(license_id: str) -> str:
        for decision in ("deny", "review", "allow"):
            if license_id in policy[decision]:
                return decision
        return "review"

    findings = []
    for component in sorted(by_id):
        row = by_id[component]
        decision = base_disposition(row["license"])
        exception = active_exceptions.get((component, row["license"]))
        if exception:
            decision = exception["disposition"]
        findings.append({
            "component": component,
            "license": row["license"],
            "source": "direct",
            "path": [component],
            "disposition": decision,
            "exception_id": exception["id"] if exception else None,
            "action": "remove_or_replace" if decision == "deny" else ("legal_review" if decision == "review" else "none"),
        })

    for root in sorted(row["id"] for row in data["components"] if row["ships"]):
        queue = deque([(root, [root])])
        seen = {root}
        while queue:
            current, path = queue.popleft()
            for dependency in runtime[current]:
                if dependency in seen:
                    continue
                seen.add(dependency)
                next_path = path + [dependency]
                dep = by_id[dependency]
                if dep["license"] in policy["restrictive"]:
                    exception = active_exceptions.get((dependency, dep["license"]))
                    decision = exception["disposition"] if exception else base_disposition(dep["license"])
                    findings.append({
                        "component": root,
                        "license": dep["license"],
                        "source": "transitive_runtime",
                        "path": next_path,
                        "disposition": decision,
                        "exception_id": exception["id"] if exception else None,
                        "action": "isolate_or_replace_dependency" if decision == "deny" else ("legal_review" if decision == "review" else "none"),
                    })
                queue.append((dependency, next_path))
    findings.sort(key=lambda x: (x["component"], x["source"], x["license"], x["path"]))
    summary = {
        "components": len(data["components"]),
        "shipping_roots": sum(row["ships"] for row in data["components"]),
        "findings": len(findings),
        "deny": sum(row["disposition"] == "deny" for row in findings),
        "review": sum(row["disposition"] == "review" for row in findings),
        "allow": sum(row["disposition"] == "allow" for row in findings),
        "active_exceptions": len(active_exceptions),
    }
    payload = json.dumps({"findings": findings, "summary": summary}, sort_keys=True, separators=(",", ":"))
    return {"status": "complete", "findings": findings, "summary": summary, "report_digest": hashlib.sha256(payload.encode()).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = audit(load(args.input))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Validation Error: {exc}")
        return 2
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
