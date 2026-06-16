import logging
from typing import List, Optional

from .logger import TransactionLog, TransactionLogger, TxMode, TxStatus, Vote
from .manager import TransactionManager

logger = logging.getLogger(__name__)


class RecoveryResult:
    def __init__(self):
        self.committed: List[str] = []
        self.rolled_back: List[str] = []
        self.confirmed: List[str] = []
        self.cancelled: List[str] = []
        self.skipped: List[str] = []
        self.errors: List[str] = []
        self.partial: List[str] = []

    def __repr__(self) -> str:
        return (
            f"RecoveryResult(committed={self.committed}, "
            f"rolled_back={self.rolled_back}, "
            f"confirmed={self.confirmed}, "
            f"cancelled={self.cancelled}, "
            f"partial={self.partial}, "
            f"skipped={self.skipped}, "
            f"errors={self.errors})"
        )


class RecoveryManager:
    def __init__(self, tm: TransactionManager):
        self._tm = tm
        self._logger = tm.logger

    async def recover_all(self) -> RecoveryResult:
        result = RecoveryResult()
        logs = self._logger.list_all()

        logger.info("Starting recovery: found %d transaction logs", len(logs))

        for log in logs:
            try:
                await self._recover_one(log, result)
            except Exception as exc:
                result.errors.append(log.tx_id)
                logger.error(
                    "Recovery error: tx=%s error=%s", log.tx_id, exc
                )

        logger.info(
            "Recovery complete: committed=%d rolled_back=%d confirmed=%d "
            "cancelled=%d partial=%d skipped=%d errors=%d",
            len(result.committed),
            len(result.rolled_back),
            len(result.confirmed),
            len(result.cancelled),
            len(result.partial),
            len(result.skipped),
            len(result.errors),
        )
        return result

    async def _recover_one(
        self, log: TransactionLog, result: RecoveryResult
    ) -> None:
        tx_id = log.tx_id
        status = log.status

        if self._logger.is_terminal(status):
            result.skipped.append(tx_id)
            logger.info(
                "Skip terminal transaction: tx=%s status=%s", tx_id, status.value
            )
            return

        logger.info(
            "Recovering transaction: tx=%s mode=%s status=%s",
            tx_id,
            log.mode.value,
            status.value,
        )

        self._log_participant_status(log)

        if log.mode == TxMode.TWO_PHASE:
            await self._recover_2pc(log, result)
        elif log.mode == TxMode.TCC:
            await self._recover_tcc(log, result)

    def _log_participant_status(self, log: TransactionLog) -> None:
        for p in log.participants:
            logger.info(
                "  Participant: %s vote=%s phase_completed=%s",
                p.participant_id,
                p.vote.value if p.vote else None,
                p.phase_completed,
            )

    async def _recover_2pc(
        self, log: TransactionLog, result: RecoveryResult
    ) -> None:
        tx_id = log.tx_id
        status = log.status

        if status in (TxStatus.PREPARING,):
            logger.info(
                "2PC recovery: crashed during prepare, will rollback: tx=%s", tx_id
            )
            any_voted_yes = any(p.vote == Vote.YES for p in log.participants)
            if any_voted_yes:
                await self._tm._rollback_phase(log, {})
                final = self._logger.read(tx_id)
                if final and self._logger.is_terminal(final.status):
                    result.rolled_back.append(tx_id)
                else:
                    result.partial.append(tx_id)
            else:
                log.status = TxStatus.ROLLED_BACK
                self._logger.append(log)
                self._tm._finalize(tx_id)
                result.rolled_back.append(tx_id)

        elif status == TxStatus.PREPARED:
            all_yes = all(p.vote == Vote.YES for p in log.participants)
            if all_yes:
                logger.info(
                    "2PC recovery: all voted YES, committing: tx=%s", tx_id
                )
                await self._tm._commit_phase(log, {})
                final = self._logger.read(tx_id)
                if final and self._logger.is_terminal(final.status):
                    result.committed.append(tx_id)
                else:
                    result.partial.append(tx_id)
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "2PC recovery: mixed votes, rolling back: tx=%s", tx_id
                    )
                    await self._tm._rollback_phase(log, {})
                    final = self._logger.read(tx_id)
                    if final and self._logger.is_terminal(final.status):
                        result.rolled_back.append(tx_id)
                    else:
                        result.partial.append(tx_id)
                else:
                    log.status = TxStatus.ROLLED_BACK
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.rolled_back.append(tx_id)

        elif status == TxStatus.COMMITTING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "commit"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "commit"]
            logger.info(
                "2PC recovery: COMMITTING tx=%s, already committed=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._tm._commit_phase(log, {})
            final = self._logger.read(tx_id)
            if final and self._logger.is_terminal(final.status):
                result.committed.append(tx_id)
            else:
                result.partial.append(tx_id)

        elif status == TxStatus.ROLLING_BACK:
            already = [p.participant_id for p in log.participants if p.phase_completed == "rollback"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "rollback"]
            logger.info(
                "2PC recovery: ROLLING_BACK tx=%s, already rolled back=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._tm._rollback_phase(log, {})
            final = self._logger.read(tx_id)
            if final and self._logger.is_terminal(final.status):
                result.rolled_back.append(tx_id)
            else:
                result.partial.append(tx_id)

    async def _recover_tcc(
        self, log: TransactionLog, result: RecoveryResult
    ) -> None:
        tx_id = log.tx_id
        status = log.status

        if status in (TxStatus.TRYING,):
            logger.info(
                "TCC recovery: crashed during try, will cancel: tx=%s", tx_id
            )
            any_tried = any(p.vote == Vote.YES for p in log.participants)
            if any_tried:
                await self._tm._cancel_phase(log, {})
                final = self._logger.read(tx_id)
                if final and self._logger.is_terminal(final.status):
                    result.cancelled.append(tx_id)
                else:
                    result.partial.append(tx_id)
            else:
                log.status = TxStatus.CANCELLED
                self._logger.append(log)
                self._tm._finalize(tx_id)
                result.cancelled.append(tx_id)

        elif status == TxStatus.TRIED:
            all_yes = all(p.vote == Vote.YES for p in log.participants)
            if all_yes:
                logger.info(
                    "TCC recovery: all tried OK, confirming: tx=%s", tx_id
                )
                await self._tm._confirm_phase(log, {})
                final = self._logger.read(tx_id)
                if final and self._logger.is_terminal(final.status):
                    result.confirmed.append(tx_id)
                else:
                    result.partial.append(tx_id)
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "TCC recovery: mixed try results, cancelling: tx=%s", tx_id
                    )
                    await self._tm._cancel_phase(log, {})
                    final = self._logger.read(tx_id)
                    if final and self._logger.is_terminal(final.status):
                        result.cancelled.append(tx_id)
                    else:
                        result.partial.append(tx_id)
                else:
                    log.status = TxStatus.CANCELLED
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.cancelled.append(tx_id)

        elif status == TxStatus.CONFIRMING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "confirm"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "confirm"]
            logger.info(
                "TCC recovery: CONFIRMING tx=%s, already confirmed=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._tm._confirm_phase(log, {})
            final = self._logger.read(tx_id)
            if final and self._logger.is_terminal(final.status):
                result.confirmed.append(tx_id)
            else:
                result.partial.append(tx_id)

        elif status == TxStatus.CANCELLING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "cancel"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "cancel"]
            logger.info(
                "TCC recovery: CANCELLING tx=%s, already cancelled=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._tm._cancel_phase(log, {})
            final = self._logger.read(tx_id)
            if final and self._logger.is_terminal(final.status):
                result.cancelled.append(tx_id)
            else:
                result.partial.append(tx_id)

    def analyze_log(self, tx_id: str) -> Optional[dict]:
        log = self._logger.read(tx_id)
        if not log:
            return None

        analysis = {
            "tx_id": log.tx_id,
            "mode": log.mode.value,
            "status": log.status.value,
            "is_terminal": self._logger.is_terminal(log.status),
            "participants": [],
            "recovery_hint": None,
        }

        for p in log.participants:
            analysis["participants"].append(
                {
                    "participant_id": p.participant_id,
                    "vote": p.vote.value if p.vote else None,
                    "phase_completed": p.phase_completed,
                }
            )

        if not self._logger.is_terminal(log.status):
            if log.mode == TxMode.TWO_PHASE:
                if log.status == TxStatus.PREPARED:
                    all_yes = all(
                        p.vote == Vote.YES for p in log.participants
                    )
                    analysis["recovery_hint"] = (
                        "commit" if all_yes else "rollback"
                    )
                elif log.status == TxStatus.COMMITTING:
                    analysis["recovery_hint"] = "commit"
                elif log.status == TxStatus.ROLLING_BACK:
                    analysis["recovery_hint"] = "rollback"
                else:
                    analysis["recovery_hint"] = "rollback"
            elif log.mode == TxMode.TCC:
                if log.status == TxStatus.TRIED:
                    all_yes = all(
                        p.vote == Vote.YES for p in log.participants
                    )
                    analysis["recovery_hint"] = (
                        "confirm" if all_yes else "cancel"
                    )
                elif log.status == TxStatus.CONFIRMING:
                    analysis["recovery_hint"] = "confirm"
                elif log.status == TxStatus.CANCELLING:
                    analysis["recovery_hint"] = "cancel"
                else:
                    analysis["recovery_hint"] = "cancel"

        return analysis
