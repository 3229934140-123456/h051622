import asyncio
import time
import logging
from typing import Callable, Optional, Dict

logger = logging.getLogger(__name__)


class TimeoutDetector:
    def __init__(self, check_interval: float = 1.0):
        self._check_interval = check_interval
        self._registrations: Dict[str, dict] = {}
        self._task: Optional[asyncio.Task] = None
        self._on_timeout: Optional[Callable] = None

    def set_callback(self, callback: Callable) -> None:
        self._on_timeout = callback

    def register(self, tx_id: str, timeout_seconds: float) -> None:
        self._registrations[tx_id] = {
            "registered_at": time.time(),
            "timeout_seconds": timeout_seconds,
        }
        logger.info("Timeout registered: tx=%s timeout=%.1fs", tx_id, timeout_seconds)

    def unregister(self, tx_id: str) -> None:
        self._registrations.pop(tx_id, None)

    def is_expired(self, tx_id: str) -> bool:
        reg = self._registrations.get(tx_id)
        if not reg:
            return False
        elapsed = time.time() - reg["registered_at"]
        return elapsed >= reg["timeout_seconds"]

    def remaining(self, tx_id: str) -> Optional[float]:
        reg = self._registrations.get(tx_id)
        if not reg:
            return None
        elapsed = time.time() - reg["registered_at"]
        return max(0.0, reg["timeout_seconds"] - elapsed)

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._check_interval)
                self._check()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Timeout check error: %s", exc)

    def _check(self) -> None:
        now = time.time()
        expired = []
        for tx_id, reg in list(self._registrations.items()):
            if now - reg["registered_at"] >= reg["timeout_seconds"]:
                expired.append(tx_id)

        for tx_id in expired:
            self._registrations.pop(tx_id, None)
            logger.warning("Transaction timed out: tx=%s", tx_id)
            if self._on_timeout:
                try:
                    self._on_timeout(tx_id)
                except Exception as exc:
                    logger.error("Timeout callback error for tx=%s: %s", tx_id, exc)
