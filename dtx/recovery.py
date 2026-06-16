import logging
from typing import List, Optional, Dict

from .logger import TransactionLog, TransactionLogger, TxMode, TxStatus, Vote
from .manager import TransactionManager

logger = logging.getLogger(__name__)


class RecoveryOutcome:
    PREPARE_COMPLETED = "prepare_completed"
    COMMIT_COMPLETED = "commit_completed"
    CANCEL_COMPLETED = "cancel_completed"
    STILL_FAILED = "still_failed"
    TERMINAL = "terminal"
    ERROR = "error"


class RecoveryResult:
    def __init__(self):
        self.committed: List[str] = []
        self.rolled_back: List[str] = []
        self.confirmed: List[str] = []
        self.cancelled: List[str] = []
        self.skipped: List[str] = []
        self.errors: List[str] = []
        self.partial: List[str] = []
        self.prepare_completed: List[str] = []
        self.outcomes: Dict[str, str] = {}

    def set_outcome(self, tx_id: str, outcome: str) -> None:
        self.outcomes[tx_id] = outcome

    def summary(self) -> str:
        lines = [
            "=" * 60,
            "RECOVERY SUMMARY",
            "=" * 60,
        ]

        prepare_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.PREPARE_COMPLETED]
        commit_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.COMMIT_COMPLETED]
        cancel_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.CANCEL_COMPLETED]
        still_failed = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.STILL_FAILED]
        terminal = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.TERMINAL]
        errors = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.ERROR]

        if terminal:
            lines.append(f"[ALREADY DONE]  Already in terminal state: {terminal}")
        if prepare_done:
            lines.append(f"[COMPENSATED]  Prepare phase compensation completed: {prepare_done}")
            lines.append("                  (Reserved resources released, no orphaned locks)")
        if commit_done:
            lines.append(f"[COMMITTED]    Commit phase completed: {commit_done}")
        if cancel_done:
            lines.append(f"[CANCELLED]    Cancel phase completed: {cancel_done}")
        if still_failed:
            lines.append(f"[STUCK]        Still has failing participants (retry again later): {still_failed}")
            lines.append("                  (Check participant health, re-run recovery after fix)")
        if errors:
            lines.append(f"[ERROR]        Recovery errors (need manual investigation): {errors}")

        if not (prepare_done or commit_done or cancel_done or still_failed or terminal or errors):
            lines.append("  No transactions needed recovery.")

        lines.append("=" * 60)
        return "\n".join(lines)

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
                result.set_outcome(log.tx_id, RecoveryOutcome.ERROR)
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

        print()
        print(result.summary())
        return result

    async def _recover_one(
        self, log: TransactionLog, result: RecoveryResult
    ) -> None:
        tx_id = log.tx_id
        status = log.status

        if self._logger.is_terminal(status):
            result.skipped.append(tx_id)
            result.set_outcome(tx_id, RecoveryOutcome.TERMINAL)
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
            vote_info = p.vote.value if p.vote else "NOT_VOTED"
            phase_info = p.phase_completed or "NOT_STARTED"
            logger.info(
                "  Participant: %s vote=%s phase=%s",
                p.participant_id,
                vote_info,
                phase_info,
            )

    def _check_terminal(self, log: TransactionLog) -> bool:
        return self._logger.is_terminal(log.status)

    async def _run_phase_and_update_result(
        self,
        log: TransactionLog,
        result: RecoveryResult,
        phase_fn,
        phase_name: str,
        success_list: List[str],
        success_outcome: str,
        partial_outcome: str,
    ) -> None:
        tx_id = log.tx_id
        await phase_fn(log, {})
        final = self._logger.read(tx_id)

        if final and self._check_terminal(final):
            success_list.append(tx_id)
            result.set_outcome(tx_id, success_outcome)
        elif final and final.status in (TxStatus.PREPARING, TxStatus.TRYING):
            result.prepare_completed.append(tx_id)
            result.set_outcome(tx_id, RecoveryOutcome.PREPARE_COMPLETED)
        else:
            result.partial.append(tx_id)
            result.set_outcome(tx_id, partial_outcome)

    async def _recover_2pc(
        self, log: TransactionLog, result: RecoveryResult
    ) -> None:
        tx_id = log.tx_id
        status = log.status

        if status == TxStatus.PREPARING:
            voted_yes = [p.participant_id for p in log.participants if p.vote == Vote.YES]
            not_voted = [p.participant_id for p in log.participants if p.vote is None]
            voted_no = [p.participant_id for p in log.participants if p.vote in (Vote.NO, Vote.TIMEOUT)]

            logger.info(
                "2PC recovery: crashed during PREPARE. voted_yes=%s not_voted=%s voted_no=%s",
                voted_yes, not_voted, voted_no,
            )

            for p in log.participants:
                if p.vote is None:
                    p.vote = Vote.TIMEOUT

            log.status = TxStatus.PREPARED
            self._logger.append(log)

            if voted_yes:
                logger.info(
                    "2PC recovery: compensating %d participants that already voted YES: %s",
                    len(voted_yes), voted_yes,
                )
                await self._tm._rollback_phase(log, {})
                final = self._logger.read(tx_id)
                if final and self._check_terminal(final):
                    result.rolled_back.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.CANCEL_COMPLETED)
                else:
                    result.partial.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.STILL_FAILED)
            else:
                log.status = TxStatus.ROLLED_BACK
                self._logger.append(log)
                self._tm._finalize(tx_id)
                result.rolled_back.append(tx_id)
                result.set_outcome(tx_id, RecoveryOutcome.CANCEL_COMPLETED)
                logger.info("2PC recovery: no YES voters, marked ROLLED_BACK")

        elif status == TxStatus.PREPARED:
            all_yes = all(p.vote == Vote.YES for p in log.participants)
            if all_yes:
                logger.info(
                    "2PC recovery: all voted YES, committing: tx=%s", tx_id
                )
                await self._run_phase_and_update_result(
                    log, result,
                    self._tm._commit_phase,
                    "commit",
                    result.committed,
                    RecoveryOutcome.COMMIT_COMPLETED,
                    RecoveryOutcome.STILL_FAILED,
                )
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "2PC recovery: mixed votes, rolling back: tx=%s", tx_id
                    )
                    await self._run_phase_and_update_result(
                        log, result,
                        self._tm._rollback_phase,
                        "rollback",
                        result.rolled_back,
                        RecoveryOutcome.CANCEL_COMPLETED,
                        RecoveryOutcome.STILL_FAILED,
                    )
                else:
                    log.status = TxStatus.ROLLED_BACK
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.rolled_back.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.CANCEL_COMPLETED)

        elif status == TxStatus.COMMITTING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "commit"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "commit"]
            logger.info(
                "2PC recovery: COMMITTING tx=%s, already committed=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_update_result(
                log, result,
                self._tm._commit_phase,
                "commit",
                result.committed,
                RecoveryOutcome.COMMIT_COMPLETED,
                RecoveryOutcome.STILL_FAILED,
            )

        elif status == TxStatus.ROLLING_BACK:
            already = [p.participant_id for p in log.participants if p.phase_completed == "rollback"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "rollback"]
            logger.info(
                "2PC recovery: ROLLING_BACK tx=%s, already rolled back=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_update_result(
                log, result,
                self._tm._rollback_phase,
                "rollback",
                result.rolled_back,
                RecoveryOutcome.CANCEL_COMPLETED,
                RecoveryOutcome.STILL_FAILED,
            )

    async def _recover_tcc(
        self, log: TransactionLog, result: RecoveryResult
    ) -> None:
        tx_id = log.tx_id
        status = log.status

        if status == TxStatus.TRYING:
            tried_yes = [p.participant_id for p in log.participants if p.vote == Vote.YES]
            not_tried = [p.participant_id for p in log.participants if p.vote is None]
            tried_no = [p.participant_id for p in log.participants if p.vote in (Vote.NO, Vote.TIMEOUT)]

            logger.info(
                "TCC recovery: crashed during TRY. tried_yes=%s not_tried=%s tried_no=%s",
                tried_yes, not_tried, tried_no,
            )

            for p in log.participants:
                if p.vote is None:
                    p.vote = Vote.TIMEOUT

            log.status = TxStatus.TRIED
            self._logger.append(log)

            if tried_yes:
                logger.info(
                    "TCC recovery: compensating %d participants that already tried YES: %s",
                    len(tried_yes), tried_yes,
                )
                await self._tm._cancel_phase(log, {})
                final = self._logger.read(tx_id)
                if final and self._check_terminal(final):
                    result.cancelled.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.CANCEL_COMPLETED)
                else:
                    result.partial.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.STILL_FAILED)
            else:
                log.status = TxStatus.CANCELLED
                self._logger.append(log)
                self._tm._finalize(tx_id)
                result.cancelled.append(tx_id)
                result.set_outcome(tx_id, RecoveryOutcome.CANCEL_COMPLETED)
                logger.info("TCC recovery: no YES tryers, marked CANCELLED")

        elif status == TxStatus.TRIED:
            all_yes = all(p.vote == Vote.YES for p in log.participants)
            if all_yes:
                logger.info(
                    "TCC recovery: all tried OK, confirming: tx=%s", tx_id
                )
                await self._run_phase_and_update_result(
                    log, result,
                    self._tm._confirm_phase,
                    "confirm",
                    result.confirmed,
                    RecoveryOutcome.COMMIT_COMPLETED,
                    RecoveryOutcome.STILL_FAILED,
                )
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "TCC recovery: mixed try results, cancelling: tx=%s", tx_id
                    )
                    await self._run_phase_and_update_result(
                        log, result,
                        self._tm._cancel_phase,
                        "cancel",
                        result.cancelled,
                        RecoveryOutcome.CANCEL_COMPLETED,
                        RecoveryOutcome.STILL_FAILED,
                    )
                else:
                    log.status = TxStatus.CANCELLED
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.cancelled.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.CANCEL_COMPLETED)

        elif status == TxStatus.CONFIRMING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "confirm"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "confirm"]
            logger.info(
                "TCC recovery: CONFIRMING tx=%s, already confirmed=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_update_result(
                log, result,
                self._tm._confirm_phase,
                "confirm",
                result.confirmed,
                RecoveryOutcome.COMMIT_COMPLETED,
                RecoveryOutcome.STILL_FAILED,
            )

        elif status == TxStatus.CANCELLING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "cancel"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "cancel"]
            logger.info(
                "TCC recovery: CANCELLING tx=%s, already cancelled=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_update_result(
                log, result,
                self._tm._cancel_phase,
                "cancel",
                result.cancelled,
                RecoveryOutcome.CANCEL_COMPLETED,
                RecoveryOutcome.STILL_FAILED,
            )

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
                if log.status == TxStatus.PREPARING:
                    any_yes = any(p.vote == Vote.YES for p in log.participants)
                    if any_yes:
                        analysis["recovery_hint"] = "rollback YES voters only"
                    else:
                        analysis["recovery_hint"] = "mark rolled_back (no compensation needed)"
                elif log.status == TxStatus.PREPARED:
                    all_yes = all(p.vote == Vote.YES for p in log.participants)
                    analysis["recovery_hint"] = "commit" if all_yes else "rollback"
                elif log.status == TxStatus.COMMITTING:
                    analysis["recovery_hint"] = "commit (skip already done)"
                elif log.status == TxStatus.ROLLING_BACK:
                    analysis["recovery_hint"] = "rollback (skip already done)"
            elif log.mode == TxMode.TCC:
                if log.status == TxStatus.TRYING:
                    any_yes = any(p.vote == Vote.YES for p in log.participants)
                    if any_yes:
                        analysis["recovery_hint"] = "cancel YES tryers only"
                    else:
                        analysis["recovery_hint"] = "mark cancelled (no compensation needed)"
                elif log.status == TxStatus.TRIED:
                    all_yes = all(p.vote == Vote.YES for p in log.participants)
                    analysis["recovery_hint"] = "confirm" if all_yes else "cancel"
                elif log.status == TxStatus.CONFIRMING:
                    analysis["recovery_hint"] = "confirm (skip already done)"
                elif log.status == TxStatus.CANCELLING:
                    analysis["recovery_hint"] = "cancel (skip already done)"

        return analysis
