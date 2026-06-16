import asyncio
import logging
import uuid
from typing import Dict, List, Optional, Set

from .logger import (
    ParticipantRecord,
    TransactionLog,
    TransactionLogger,
    TxMode,
    TxStatus,
    Vote,
)
from .participant import Participant, TCCParticipant
from .timeout import TimeoutDetector

logger = logging.getLogger(__name__)


class TransactionError(Exception):
    pass


class TransactionManager:
    def __init__(
        self,
        log_dir: str = "tx_logs",
        default_timeout: float = 30.0,
        timeout_check_interval: float = 1.0,
    ):
        self._tx_logger = TransactionLogger(log_dir)
        self._default_timeout = default_timeout
        self._participants: Dict[str, Participant] = {}
        self._active_tx: Dict[str, TransactionLog] = {}
        self._timeout = TimeoutDetector(check_interval=timeout_check_interval)
        self._timeout.set_callback(self._on_timeout)
        self._default_retry_max = 3
        self._default_retry_delay = 0.5
        self._retry_config: Dict[str, dict] = {}
        self._aborted_tx: Set[str] = set()
        self._inflight_tx: Dict[str, asyncio.Event] = {}
        self._last_attempts: Dict[str, int] = {}
        self._pending_failures: Dict[str, list] = {}

    def set_participant_retry(
        self,
        participant_id: str,
        retry_max: int = 3,
        retry_delay: float = 0.5,
    ) -> None:
        self._retry_config[participant_id] = {
            "retry_max": retry_max,
            "retry_delay": retry_delay,
        }
        logger.info(
            "Participant retry configured: %s retry_max=%d retry_delay=%.2fs",
            participant_id, retry_max, retry_delay,
        )

    def _get_retry_config(self, participant_id: str) -> dict:
        cfg = self._retry_config.get(participant_id)
        if cfg:
            return cfg
        return {
            "retry_max": self._default_retry_max,
            "retry_delay": self._default_retry_delay,
        }

    def register_participant(
        self,
        participant: Participant,
        *,
        retry_max: Optional[int] = None,
        retry_delay: Optional[float] = None,
    ) -> None:
        self._participants[participant.participant_id] = participant
        if retry_max is not None or retry_delay is not None:
            self.set_participant_retry(
                participant.participant_id,
                retry_max=retry_max or self._default_retry_max,
                retry_delay=retry_delay or self._default_retry_delay,
            )
        logger.info("Participant registered: %s", participant.participant_id)

    def unregister_participant(self, participant_id: str) -> None:
        self._participants.pop(participant_id, None)

    async def start(self) -> None:
        await self._timeout.start()
        logger.info("TransactionManager started")

    async def stop(self) -> None:
        await self._timeout.stop()
        logger.info("TransactionManager stopped")

    def begin(
        self,
        mode: TxMode,
        participant_ids: List[str],
        timeout: Optional[float] = None,
        context: Optional[dict] = None,
    ) -> str:
        tx_id = uuid.uuid4().hex[:16]
        participants = []
        for pid in participant_ids:
            if pid not in self._participants:
                raise TransactionError(f"Unknown participant: {pid}")
            participants.append(ParticipantRecord(participant_id=pid))

        effective_timeout = timeout or self._default_timeout
        status = TxStatus.TRYING if mode == TxMode.TCC else TxStatus.PREPARING
        log = TransactionLog(
            tx_id=tx_id,
            mode=mode,
            status=status,
            participants=participants,
            timeout_seconds=effective_timeout,
        )

        self._tx_logger.append(log)
        self._active_tx[tx_id] = log
        self._timeout.register(tx_id, effective_timeout)
        self._inflight_tx[tx_id] = asyncio.Event()

        logger.info(
            "Transaction begun: tx=%s mode=%s participants=%s",
            tx_id,
            mode.value,
            participant_ids,
        )
        return tx_id

    async def execute(self, tx_id: str, context: Optional[dict] = None) -> None:
        log = self._active_tx.get(tx_id)
        if not log:
            log = self._tx_logger.read(tx_id)
            if not log:
                raise TransactionError(f"Unknown transaction: {tx_id}")
            self._active_tx[tx_id] = log

        ctx = context or {}

        if log.mode == TxMode.TWO_PHASE:
            await self._execute_2pc(log, ctx)
        elif log.mode == TxMode.TCC:
            await self._execute_tcc(log, ctx)
        else:
            raise TransactionError(f"Unknown mode: {log.mode}")

    def _is_aborted(self, tx_id: str) -> bool:
        return tx_id in self._aborted_tx

    def _ack_abort(self, tx_id: str) -> None:
        self._aborted_tx.discard(tx_id)

    async def _execute_2pc(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id

        log.status = TxStatus.PREPARING
        self._tx_logger.append(log)

        all_yes = True
        for prec in log.participants:
            if self._is_aborted(tx_id):
                logger.warning(
                    "2PC prepare aborted by timeout: tx=%s", tx_id
                )
                self._ack_abort(tx_id)
                return

            participant = self._participants.get(prec.participant_id)
            if not participant:
                prec.vote = Vote.NO
                self._tx_logger.append(log)
                all_yes = False
                logger.error(
                    "Participant not found during prepare: %s (logged NO)", prec.participant_id
                )
                continue

            try:
                vote = await participant.prepare(tx_id, context)
                prec.vote = vote
                self._tx_logger.append(log)
                logger.info(
                    "Prepare vote: tx=%s participant=%s vote=%s (logged)",
                    tx_id,
                    prec.participant_id,
                    vote.value,
                )
            except Exception as exc:
                prec.vote = Vote.NO
                self._tx_logger.append(log)
                all_yes = False
                logger.error(
                    "Prepare error: tx=%s participant=%s error=%s (logged NO)",
                    tx_id,
                    prec.participant_id,
                    exc,
                )
                continue

            if vote != Vote.YES:
                all_yes = False

        if self._is_aborted(tx_id):
            logger.warning(
                "2PC aborted after prepare, timeout handler will take over: tx=%s",
                tx_id,
            )
            self._ack_abort(tx_id)
            return

        if all_yes:
            log.status = TxStatus.PREPARED
            self._tx_logger.append(log)
            await self._commit_phase(log, context)
        else:
            log.status = TxStatus.PREPARED
            self._tx_logger.append(log)
            await self._rollback_phase(log, context)

    async def _commit_phase(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id
        target_action = "commit"

        if log.status != TxStatus.COMMITTING:
            log.status = TxStatus.COMMITTING
            self._tx_logger.append(log)

        all_ok = True
        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            if prec.phase_completed == target_action:
                logger.info(
                    "Skip already-completed participant: tx=%s participant=%s phase=%s",
                    tx_id,
                    prec.participant_id,
                    target_action,
                )
                continue

            success = await self._invoke_with_retry(
                tx_id, prec.participant_id, target_action, context
            )
            if success:
                prec.phase_completed = target_action
                self._tx_logger.append(log)
            else:
                all_ok = False
                logger.error(
                    "Participant commit failed, tx stays COMMITTING: tx=%s participant=%s",
                    tx_id,
                    prec.participant_id,
                )

        if all_ok:
            log.status = TxStatus.COMMITTED
            self._tx_logger.append(log)
            self._finalize(tx_id)
            logger.info("Transaction committed: tx=%s", tx_id)
        else:
            logger.warning(
                "Transaction partially committed, stays COMMITTING for recovery: tx=%s",
                tx_id,
            )
            self._finalize(tx_id)

    async def _rollback_phase(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id
        target_action = "rollback"

        if log.status != TxStatus.ROLLING_BACK:
            log.status = TxStatus.ROLLING_BACK
            self._tx_logger.append(log)

        all_ok = True
        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            if prec.phase_completed == target_action:
                logger.info(
                    "Skip already-completed participant: tx=%s participant=%s phase=%s",
                    tx_id,
                    prec.participant_id,
                    target_action,
                )
                continue

            success = await self._invoke_with_retry(
                tx_id, prec.participant_id, target_action, context
            )
            if success:
                prec.phase_completed = target_action
                self._tx_logger.append(log)
            else:
                all_ok = False
                logger.error(
                    "Participant rollback failed, tx stays ROLLING_BACK: tx=%s participant=%s",
                    tx_id,
                    prec.participant_id,
                )

        if all_ok:
            log.status = TxStatus.ROLLED_BACK
            self._tx_logger.append(log)
            self._finalize(tx_id)
            logger.info("Transaction rolled back: tx=%s", tx_id)
        else:
            logger.warning(
                "Transaction partially rolled back, stays ROLLING_BACK for recovery: tx=%s",
                tx_id,
            )
            self._finalize(tx_id)

    async def _execute_tcc(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id

        log.status = TxStatus.TRYING
        self._tx_logger.append(log)

        all_yes = True
        for prec in log.participants:
            if self._is_aborted(tx_id):
                logger.warning(
                    "TCC try aborted by timeout: tx=%s", tx_id
                )
                self._ack_abort(tx_id)
                return

            participant = self._participants.get(prec.participant_id)
            if not participant:
                prec.vote = Vote.NO
                self._tx_logger.append(log)
                all_yes = False
                logger.error(
                    "Participant not found during try: %s (logged NO)", prec.participant_id
                )
                continue

            if not isinstance(participant, TCCParticipant):
                prec.vote = Vote.NO
                self._tx_logger.append(log)
                all_yes = False
                logger.error(
                    "Participant is not TCC-capable: %s (logged NO)", prec.participant_id
                )
                continue

            try:
                vote = await participant.try_phase(tx_id, context)
                prec.vote = vote
                self._tx_logger.append(log)
                logger.info(
                    "Try vote: tx=%s participant=%s vote=%s (logged)",
                    tx_id,
                    prec.participant_id,
                    vote.value,
                )
            except Exception as exc:
                prec.vote = Vote.NO
                self._tx_logger.append(log)
                all_yes = False
                logger.error(
                    "Try error: tx=%s participant=%s error=%s (logged NO)",
                    tx_id,
                    prec.participant_id,
                    exc,
                )
                continue

            if vote != Vote.YES:
                all_yes = False

        if self._is_aborted(tx_id):
            logger.warning(
                "TCC aborted after try, timeout handler will take over: tx=%s",
                tx_id,
            )
            self._ack_abort(tx_id)
            return

        if all_yes:
            log.status = TxStatus.TRIED
            self._tx_logger.append(log)
            await self._confirm_phase(log, context)
        else:
            log.status = TxStatus.TRIED
            self._tx_logger.append(log)
            await self._cancel_phase(log, context)

    async def _confirm_phase(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id
        target_action = "confirm"

        if log.status != TxStatus.CONFIRMING:
            log.status = TxStatus.CONFIRMING
            self._tx_logger.append(log)

        all_ok = True
        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            if prec.phase_completed == target_action:
                logger.info(
                    "Skip already-completed participant: tx=%s participant=%s phase=%s",
                    tx_id,
                    prec.participant_id,
                    target_action,
                )
                continue

            success = await self._invoke_with_retry(
                tx_id, prec.participant_id, target_action, context
            )
            if success:
                prec.phase_completed = target_action
                self._tx_logger.append(log)
            else:
                all_ok = False
                logger.error(
                    "Participant confirm failed, tx stays CONFIRMING: tx=%s participant=%s",
                    tx_id,
                    prec.participant_id,
                )

        if all_ok:
            log.status = TxStatus.CONFIRMED
            self._tx_logger.append(log)
            self._finalize(tx_id)
            logger.info("TCC transaction confirmed: tx=%s", tx_id)
        else:
            logger.warning(
                "Transaction partially confirmed, stays CONFIRMING for recovery: tx=%s",
                tx_id,
            )
            self._finalize(tx_id)

    async def _cancel_phase(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id
        target_action = "cancel"

        if log.status != TxStatus.CANCELLING:
            log.status = TxStatus.CANCELLING
            self._tx_logger.append(log)

        all_ok = True
        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            if prec.phase_completed == target_action:
                logger.info(
                    "Skip already-completed participant: tx=%s participant=%s phase=%s",
                    tx_id,
                    prec.participant_id,
                    target_action,
                )
                continue

            success = await self._invoke_with_retry(
                tx_id, prec.participant_id, target_action, context
            )
            if success:
                prec.phase_completed = target_action
                self._tx_logger.append(log)
            else:
                all_ok = False
                logger.error(
                    "Participant cancel failed, tx stays CANCELLING: tx=%s participant=%s",
                    tx_id,
                    prec.participant_id,
                )

        if all_ok:
            log.status = TxStatus.CANCELLED
            self._tx_logger.append(log)
            self._finalize(tx_id)
            logger.info("TCC transaction cancelled: tx=%s", tx_id)
        else:
            logger.warning(
                "Transaction partially cancelled, stays CANCELLING for recovery: tx=%s",
                tx_id,
            )
            self._finalize(tx_id)

    async def _invoke_with_retry(
        self,
        tx_id: str,
        participant_id: str,
        action: str,
        context: dict,
    ) -> bool:
        participant = self._participants.get(participant_id)
        if not participant:
            logger.error(
                "Participant not found for %s: %s", action, participant_id
            )
            self._record_failure(tx_id, participant_id, action, 0, "Participant not found")
            return False

        method = getattr(participant, action, None)
        if not method or not callable(method):
            logger.error(
                "Action %s not supported by participant %s", action, participant_id
            )
            self._record_failure(tx_id, participant_id, action, 0, f"Action {action} not supported")
            return False

        cfg = self._get_retry_config(participant_id)
        retry_max = cfg["retry_max"]
        retry_delay = cfg["retry_delay"]
        last_error = None

        for attempt in range(1, retry_max + 1):
            try:
                await method(tx_id, context)
                self._last_attempts[participant_id] = attempt
                logger.info(
                    "Action succeeded: tx=%s participant=%s action=%s attempt=%d/%d",
                    tx_id,
                    participant_id,
                    action,
                    attempt,
                    retry_max,
                )
                return True
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Action failed (retry %d/%d): tx=%s participant=%s action=%s error=%s",
                    attempt,
                    retry_max,
                    tx_id,
                    participant_id,
                    action,
                    exc,
                )
                if attempt < retry_max:
                    await asyncio.sleep(retry_delay * attempt)

        self._last_attempts[participant_id] = retry_max
        self._record_failure(tx_id, participant_id, action, retry_max, last_error)
        logger.error(
            "Action exhausted retries (%d): tx=%s participant=%s action=%s",
            retry_max,
            tx_id,
            participant_id,
            action,
        )
        return False

    def _record_failure(
        self, tx_id: str, participant_id: str, action: str, attempts: int, error: str
    ) -> None:
        self._pending_failures.setdefault(tx_id, []).append(
            (participant_id, action, attempts, error)
        )

    def consume_pending_failures(self, tx_id: str) -> list:
        return self._pending_failures.pop(tx_id, [])

    def get_last_attempt_count(self, participant_id: str) -> Optional[int]:
        return self._last_attempts.get(participant_id)

    def _finalize(self, tx_id: str) -> None:
        self._timeout.unregister(tx_id)
        self._active_tx.pop(tx_id, None)
        evt = self._inflight_tx.pop(tx_id, None)
        if evt:
            evt.set()

    def _on_timeout(self, tx_id: str) -> None:
        log = self._active_tx.get(tx_id) or self._tx_logger.read(tx_id)
        if not log:
            logger.warning("Timeout for unknown transaction: tx=%s", tx_id)
            return
        if self._tx_logger.is_terminal(log.status):
            self._finalize(tx_id)
            return

        self._aborted_tx.add(tx_id)
        logger.warning(
            "Handling timeout: tx=%s status=%s (original flow will be aborted)",
            tx_id,
            log.status.value,
        )
        asyncio.ensure_future(self._handle_timeout(log))

    async def _handle_timeout(self, log: TransactionLog) -> None:
        tx_id = log.tx_id
        context = {"timed_out": True}

        if log.mode == TxMode.TWO_PHASE:
            if log.status == TxStatus.PREPARING:
                for prec in log.participants:
                    if prec.vote is None:
                        prec.vote = Vote.TIMEOUT
                log.status = TxStatus.PREPARED
                self._tx_logger.append(log)
                await self._rollback_phase(log, context)
            elif log.status == TxStatus.PREPARED:
                await self._rollback_phase(log, context)
            elif log.status == TxStatus.COMMITTING:
                await self._commit_phase(log, context)
            elif log.status == TxStatus.ROLLING_BACK:
                await self._rollback_phase(log, context)
        elif log.mode == TxMode.TCC:
            if log.status == TxStatus.TRYING:
                for prec in log.participants:
                    if prec.vote is None:
                        prec.vote = Vote.TIMEOUT
                log.status = TxStatus.TRIED
                self._tx_logger.append(log)
                await self._cancel_phase(log, context)
            elif log.status == TxStatus.TRIED:
                await self._cancel_phase(log, context)
            elif log.status == TxStatus.CONFIRMING:
                await self._confirm_phase(log, context)
            elif log.status == TxStatus.CANCELLING:
                await self._cancel_phase(log, context)

    def get_status(self, tx_id: str) -> Optional[TxStatus]:
        log = self._active_tx.get(tx_id) or self._tx_logger.read(tx_id)
        return log.status if log else None

    def get_log(self, tx_id: str) -> Optional[TransactionLog]:
        return self._active_tx.get(tx_id) or self._tx_logger.read(tx_id)

    def list_unconverged(self) -> list:
        result = []
        for log in self._tx_logger.list_all():
            if not self._tx_logger.is_terminal(log.status):
                result.append(log)
        return result

    @property
    def logger(self) -> TransactionLogger:
        return self._tx_logger
