from __future__ import annotations


class CliError(RuntimeError):
    """CLI misuse (exit 1)."""


class ConversionError(RuntimeError):
    """Conversion failure (exit 2)."""


class BuildSmokeError(RuntimeError):
    """Build smoke failure (exit 3)."""


class OracleValidationError(RuntimeError):
    """Oracle validation failure (exit 4)."""
