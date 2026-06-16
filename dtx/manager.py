import asyncio
import logging
import uuid
from typing import Dict, List, Optional

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
        self._retry_max = 3
        self._retry_delay = 0.5

    def register_participant(self, participant: Participant) -> None:
        self._participants[participant.participant_id] = participant
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

    async def _execute_2pc(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id

        log.status = TxStatus.PREPARING
        self._tx_logger.append(log)

        all_yes = True
        for prec in log.participants:
            participant = self._participants.get(prec.participant_id)
            if not participant:
                prec.vote = Vote.NO
                all_yes = False
                logger.error(
                    "Participant not found during prepare: %s", prec.participant_id
                )
                continue

            try:
                vote = await participant.prepare(tx_id, context)
                prec.vote = vote
                logger.info(
                    "Prepare vote: tx=%s participant=%s vote=%s",
                    tx_id,
                    prec.participant_id,
                    vote.value,
                )
            except Exception as exc:
                prec.vote = Vote.NO
                all_yes = False
                logger.error(
                    "Prepare error: tx=%s participant=%s error=%s",
                    tx_id,
                    prec.participant_id,
                    exc,
                )
                continue

            if vote != Vote.YES:
                all_yes = False

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
        log.status = TxStatus.COMMITTING
        self._tx_logger.append(log)

        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            await self._invoke_with_retry(
                tx_id, prec.participant_id, "commit", context
            )
            prec.phase_completed = "commit"

        log.status = TxStatus.COMMITTED
        self._tx_logger.append(log)
        self._finalize(tx_id)
        logger.info("Transaction committed: tx=%s", tx_id)

    async def _rollback_phase(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id
        log.status = TxStatus.ROLLING_BACK
        self._tx_logger.append(log)

        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            await self._invoke_with_retry(
                tx_id, prec.participant_id, "rollback", context
            )
            prec.phase_completed = "rollback"

        log.status = TxStatus.ROLLED_BACK
        self._tx_logger.append(log)
        self._finalize(tx_id)
        logger.info("Transaction rolled back: tx=%s", tx_id)

    async def _execute_tcc(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id

        log.status = TxStatus.TRYING
        self._tx_logger.append(log)

        all_yes = True
        tried_participants = []
        for prec in log.participants:
            participant = self._participants.get(prec.participant_id)
            if not participant:
                prec.vote = Vote.NO
                all_yes = False
                logger.error(
                    "Participant not found during try: %s", prec.participant_id
                )
                continue

            if not isinstance(participant, TCCParticipant):
                prec.vote = Vote.NO
                all_yes = False
                logger.error(
                    "Participant is not TCC-capable: %s", prec.participant_id
                )
                continue

            try:
                vote = await participant.try_phase(tx_id, context)
                prec.vote = vote
                if vote == Vote.YES:
                    tried_participants.append(prec)
                logger.info(
                    "Try vote: tx=%s participant=%s vote=%s",
                    tx_id,
                    prec.participant_id,
                    vote.value,
                )
            except Exception as exc:
                prec.vote = Vote.NO
                all_yes = False
                logger.error(
                    "Try error: tx=%s participant=%s error=%s",
                    tx_id,
                    prec.participant_id,
                    exc,
                )
                continue

            if vote != Vote.YES:
                all_yes = False

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
        log.status = TxStatus.CONFIRMING
        self._tx_logger.append(log)

        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            await self._invoke_with_retry(
                tx_id, prec.participant_id, "confirm", context
            )
            prec.phase_completed = "confirm"

        log.status = TxStatus.CONFIRMED
        self._tx_logger.append(log)
        self._finalize(tx_id)
        logger.info("TCC transaction confirmed: tx=%s", tx_id)

    async def _cancel_phase(self, log: TransactionLog, context: dict) -> None:
        tx_id = log.tx_id
        log.status = TxStatus.CANCELLING
        self._tx_logger.append(log)

        for prec in log.participants:
            if prec.vote != Vote.YES:
                continue
            await self._invoke_with_retry(
                tx_id, prec.participant_id, "cancel", context
            )
            prec.phase_completed = "cancel"

        log.status = TxStatus.CANCELLED
        self._tx_logger.append(log)
        self._finalize(tx_id)
        logger.info("TCC transaction cancelled: tx=%s", tx_id)

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
            return False

        method = getattr(participant, action, None)
        if not method or not callable(method):
            logger.error(
                "Action %s not supported by participant %s", action, participant_id
            )
            return False

        for attempt in range(1, self._retry_max + 1):
            try:
                await method(tx_id, context)
                logger.info(
                    "Action succeeded: tx=%s participant=%s action=%s attempt=%d",
                    tx_id,
                    participant_id,
                    action,
                    attempt,
                )
                return True
            except Exception as exc:
                logger.warning(
                    "Action failed (retry %d/%d): tx=%s participant=%s action=%s error=%s",
                    attempt,
                    self._retry_max,
                    tx_id,
                    participant_id,
                    action,
                    exc,
                )
                if attempt < self._retry_max:
                    await asyncio.sleep(self._retry_delay * attempt)
        logger.error(
            "Action exhausted retries: tx=%s participant=%s action=%s",
            tx_id,
            participant_id,
            action,
        )
        return False

    def _finalize(self, tx_id: str) -> None:
        self._timeout.unregister(tx_id)
        self._active_tx.pop(tx_id, None)

    def _on_timeout(self, tx_id: str) -> None:
        log = self._active_tx.get(tx_id) or self._tx_logger.read(tx_id)
        if not log:
            logger.warning("Timeout for unknown transaction: tx=%s", tx_id)
            return
        if self._tx_logger.is_terminal(log.status):
            self._finalize(tx_id)
            return

        logger.warning(
            "Handling timeout: tx=%s status=%s", tx_id, log.status.value
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

    @property
    def logger(self) -> TransactionLogger:
        return self._tx_logger
