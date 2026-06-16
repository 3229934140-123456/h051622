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

    def __repr__(self) -> str:
        return (
            f"RecoveryResult(committed={self.committed}, "
            f"rolled_back={self.rolled_back}, "
            f"confirmed={self.confirmed}, "
            f"cancelled={self.cancelled}, "
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
            "cancelled=%d skipped=%d errors=%d",
            len(result.committed),
            len(result.rolled_back),
            len(result.confirmed),
            len(result.cancelled),
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

        if log.mode == TxMode.TWO_PHASE:
            await self._recover_2pc(log, result)
        elif log.mode == TxMode.TCC:
            await self._recover_tcc(log, result)

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
                result.rolled_back.append(tx_id)
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
                result.committed.append(tx_id)
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "2PC recovery: mixed votes, rolling back: tx=%s", tx_id
                    )
                    await self._tm._rollback_phase(log, {})
                    result.rolled_back.append(tx_id)
                else:
                    log.status = TxStatus.ROLLED_BACK
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.rolled_back.append(tx_id)

        elif status == TxStatus.COMMITTING:
            logger.info(
                "2PC recovery: committing in progress, re-committing: tx=%s", tx_id
            )
            await self._tm._commit_phase(log, {})
            result.committed.append(tx_id)

        elif status == TxStatus.ROLLING_BACK:
            logger.info(
                "2PC recovery: rollback in progress, re-rolling back: tx=%s", tx_id
            )
            await self._tm._rollback_phase(log, {})
            result.rolled_back.append(tx_id)

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
                result.cancelled.append(tx_id)
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
                result.confirmed.append(tx_id)
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "TCC recovery: mixed try results, cancelling: tx=%s", tx_id
                    )
                    await self._tm._cancel_phase(log, {})
                    result.cancelled.append(tx_id)
                else:
                    log.status = TxStatus.CANCELLED
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.cancelled.append(tx_id)

        elif status == TxStatus.CONFIRMING:
            logger.info(
                "TCC recovery: confirming in progress, re-confirming: tx=%s", tx_id
            )
            await self._tm._confirm_phase(log, {})
            result.confirmed.append(tx_id)

        elif status == TxStatus.CANCELLING:
            logger.info(
                "TCC recovery: cancelling in progress, re-cancelling: tx=%s", tx_id
            )
            await self._tm._cancel_phase(log, {})
            result.cancelled.append(tx_id)

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
