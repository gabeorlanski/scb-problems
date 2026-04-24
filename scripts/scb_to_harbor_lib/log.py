from __future__ import annotations

import logging

LOGGER = logging.getLogger("scb_to_harbor")


def setup_logging(*, verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
