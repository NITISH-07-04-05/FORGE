from __future__ import annotations

import logging
from typing import Any

from app.execution.registry import TaskHandler

logger = logging.getLogger(__name__)


class EchoTaskHandler(TaskHandler):
    """Minimal deterministic handler used to prove the execution pipeline works."""

    def execute(self, payload: dict[str, Any]) -> None:
        logger.info("Echo task executed with payload=%s", payload)
