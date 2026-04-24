# Scripts Agent Guide

Quick notes for agents working in `scripts/`.

## SCB → Harbor converter

- CLI entrypoint: `scripts/scb_to_harbor.py`
  - Keep this file thin and backwards-compatible.
  - Existing calls like `uv run scripts/scb_to_harbor.py ...` must continue to work.
  - It re-exports helpers from `scb_to_harbor_lib` for compatibility with direct imports.

- Implementation package: `scripts/scb_to_harbor_lib/`
  - `cli.py` — argument parsing and top-level orchestration
  - `converter.py` — generation workflow
  - `specs.py` — SCB problem/config loading
  - `overrides.py` — `conversion.toml` validation/loading
  - `artifacts.py` — artifacts, rewards, environment, timeout resolution
  - `renderers.py` — `task.toml`, README, instructions, `test.sh`, `solve.sh`
  - `validation.py` — static/build/oracle validation
  - `dockerfiles.py` / `templates.py` — embedded Dockerfiles and generated helper scripts
  - `canary.py`, `fileops.py`, `metadata.py`, `toml_utils.py`, `utils.py`, `models.py`, `errors.py`, `log.py` — focused helpers

## Adding converter behavior

- Prefer adding logic to the focused module instead of expanding `scb_to_harbor.py`.
- If behavior is configurable from `conversion.toml`:
  1. Add the override key to `constants.py`.
  2. Validate shape/types in `overrides.py`.
  3. Resolve/apply the behavior near its use site.
  4. Include resolved values in `plan_for_problem()` when useful for `--dry-run`.

## Problem-specific `test.sh` snippets

Supported in `conversion.toml`:

```toml
[problem_name]
test_sh_snippet = "echo applies to every checkpoint"
```

```toml
[problem_name]
test_sh_snippets = [
  "echo first global snippet",
  "echo second global snippet",
]
```

```toml
[problem_name.test_sh_snippets]
all = "echo applies to all checkpoints"
checkpoint_2 = "echo only checkpoint_2"
checkpoint_3 = [
  "echo checkpoint_3 snippet A",
  "echo checkpoint_3 snippet B",
]
```

- Snippets are inserted in generated `steps/<checkpoint>/tests/test.sh` after setup / `PRIOR_TESTS` initialization and before the `uv run ... pytest` command.
- Unknown checkpoint keys should fail conversion.

## Validation commands

Run these after converter changes:

```bash
python3 -m py_compile scripts/scb_to_harbor.py scripts/scb_to_harbor_lib/*.py
python3 scripts/scb_to_harbor.py --org test --dry-run --all >/tmp/scb-all-dry.txt
rm -rf /tmp/scb-harbor-test
python3 scripts/scb_to_harbor.py --org test --out /tmp/scb-harbor-test --no-build file_backup
```

For snippet-specific changes, create a small temporary override file and verify generated `test.sh` contents with `rg`.

## Hygiene

- Remove generated `__pycache__` directories before finishing.
- Do not write generated Harbor tasks into the repo unless explicitly requested; use `/tmp/...` for test output.
- Keep generated shell scripts deterministic and non-interactive.
