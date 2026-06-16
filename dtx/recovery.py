import logging
from typing import List, Optional, Dict, Any

from .logger import TransactionLog, TransactionLogger, TxMode, TxStatus, Vote
from .manager import TransactionManager

logger = logging.getLogger(__name__)


class RecoveryOutcome:
    PREPARE_COMPENSATED = "prepare_compensated"
    TRY_COMPENSATED = "try_compensated"
    COMMIT_COMPLETED = "commit_completed"
    CONFIRM_COMPLETED = "confirm_completed"
    ROLLBACK_COMPLETED = "rollback_completed"
    CANCEL_COMPLETED = "cancel_completed"
    STILL_FAILED = "still_failed"
    TERMINAL = "terminal"
    ERROR = "error"


class ParticipantRecoveryAttempt:
    def __init__(self, participant_id: str, action: str):
        self.participant_id = participant_id
        self.action = action
        self.attempts: int = 0
        self.succeeded: bool = False
        self.final_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "participant_id": self.participant_id,
            "action": self.action,
            "attempts": self.attempts,
            "succeeded": self.succeeded,
            "final_error": self.final_error,
        }


class RecoveryResult:
    def __init__(self):
        self.committed: List[str] = []
        self.rolled_back: List[str] = []
        self.confirmed: List[str] = []
        self.cancelled: List[str] = []
        self.skipped: List[str] = []
        self.errors: List[str] = []
        self.partial: List[str] = []
        self.prepare_compensated: List[str] = []
        self.try_compensated: List[str] = []
        self.outcomes: Dict[str, str] = {}
        self.participant_attempts: Dict[str, List[ParticipantRecoveryAttempt]] = {}

    def set_outcome(self, tx_id: str, outcome: str) -> None:
        self.outcomes[tx_id] = outcome

    def add_attempt(self, tx_id: str, attempt: ParticipantRecoveryAttempt) -> None:
        self.participant_attempts.setdefault(tx_id, []).append(attempt)

    def summary(self) -> str:
        lines = [
            "=" * 72,
            "  RECOVERY SUMMARY",
            "=" * 72,
        ]

        prepare_comp = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.PREPARE_COMPENSATED]
        try_comp = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.TRY_COMPENSATED]
        commit_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.COMMIT_COMPLETED]
        confirm_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.CONFIRM_COMPLETED]
        rollback_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.ROLLBACK_COMPLETED]
        cancel_done = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.CANCEL_COMPLETED]
        still_failed = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.STILL_FAILED]
        terminal = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.TERMINAL]
        errors = [tx for tx, o in self.outcomes.items() if o == RecoveryOutcome.ERROR]

        if terminal:
            lines.append(f"[ALREADY DONE ]  Already in terminal state: {terminal}")

        if prepare_comp:
            lines.append("")
            lines.append(f"[PREPARE FIXED ]  2PC crashed in PREPARE, compensated {len(prepare_comp)} tx(s):")
            for tx in prepare_comp:
                lines.append(f"                    - {tx} (reserved resources released)")
                lines.extend(self._format_attempts(tx, indent="                      "))

        if try_comp:
            lines.append("")
            lines.append(f"[TRY FIXED     ]  TCC crashed in TRY, compensated {len(try_comp)} tx(s):")
            for tx in try_comp:
                lines.append(f"                    - {tx} (frozen resources released)")
                lines.extend(self._format_attempts(tx, indent="                      "))

        if commit_done:
            lines.append("")
            lines.append(f"[COMMITTED     ]  2PC commit phase completed: {commit_done}")
            for tx in commit_done:
                lines.extend(self._format_attempts(tx, indent="                    "))

        if confirm_done:
            lines.append("")
            lines.append(f"[CONFIRMED     ]  TCC confirm phase completed: {confirm_done}")
            for tx in confirm_done:
                lines.extend(self._format_attempts(tx, indent="                    "))

        if rollback_done:
            lines.append("")
            lines.append(f"[ROLLED BACK   ]  2PC rollback phase completed: {rollback_done}")
            for tx in rollback_done:
                lines.extend(self._format_attempts(tx, indent="                    "))

        if cancel_done:
            lines.append("")
            lines.append(f"[CANCELLED     ]  TCC cancel phase completed: {cancel_done}")
            for tx in cancel_done:
                lines.extend(self._format_attempts(tx, indent="                    "))

        if still_failed:
            lines.append("")
            lines.append(f"[STUCK         ]  {len(still_failed)} tx(s) still have failing participants:")
            for tx in still_failed:
                lines.append(f"                    - {tx}")
                lines.extend(self._format_attempts(tx, indent="                      "))
            lines.append("                  -> Fix participant health and re-run recovery")

        if errors:
            lines.append("")
            lines.append(f"[ERROR         ]  Recovery engine errors (investigate code): {errors}")

        all_categories = bool(
            prepare_comp or try_comp or commit_done or confirm_done
            or rollback_done or cancel_done or still_failed or terminal or errors
        )
        if not all_categories:
            lines.append("  (no transactions needed recovery)")

        lines.append("")
        lines.append("=" * 72)
        return "\n".join(lines)

    def _format_attempts(self, tx_id: str, indent: str = "  ") -> List[str]:
        attempts = self.participant_attempts.get(tx_id, [])
        if not attempts:
            return []
        lines = []
        for a in attempts:
            status = "OK" if a.succeeded else "FAIL"
            err = f"  error={a.final_error}" if a.final_error and not a.succeeded else ""
            lines.append(
                f"{indent}{a.participant_id:20s} action={a.action:8s} attempts={a.attempts:<2d} [{status}]{err}"
            )
        return lines

    def __repr__(self) -> str:
        return (
            f"RecoveryResult(committed={self.committed}, "
            f"rolled_back={self.rolled_back}, "
            f"confirmed={self.confirmed}, "
            f"cancelled={self.cancelled}, "
            f"prepare_compensated={self.prepare_compensated}, "
            f"try_compensated={self.try_compensated}, "
            f"partial={self.partial}, "
            f"skipped={self.skipped}, "
            f"errors={self.errors})"
        )


class RecoveryManager:
    def __init__(self, tm: TransactionManager):
        self._tm = tm
        self._logger = tm.logger

    def analyze_log(self, tx_id: str) -> Optional[dict]:
        log = self._tm.get_log(tx_id)
        if not log:
            return None
        status = log.status
        mode = log.mode
        is_terminal = self._logger.is_terminal(status)

        voted_yes = [p for p in log.participants if p.vote and p.vote.value == "YES"]
        voted_no = [p for p in log.participants if p.vote and p.vote.value in ("NO", "TIMEOUT")]
        not_voted = [p for p in log.participants if p.vote is None]
        phase_done = [p for p in log.participants if p.phase_completed]

        if is_terminal:
            hint = "Already in terminal state; no recovery needed."
        elif mode == TxMode.TWO_PHASE and status == TxStatus.PREPARING:
            if voted_yes:
                hint = (
                    f"Crash during prepare phase ({len(voted_yes)} voted YES, "
                    f"{len(not_voted)} never started.  "
                    f"Recovery will rollback only YES-voted participants."
                )
            else:
                hint = "No YES votes recorded; transaction can be abandoned."
        elif mode == TxMode.TCC and status == TxStatus.TRYING:
            if voted_yes:
                hint = (
                    f"Crash during TCC try phase ({len(voted_yes)} reserved, "
                    f"{len(not_voted)} never started.  "
                    f"Recovery will cancel reserved participants."
                )
            else:
                hint = "No reservations recorded; transaction can be abandoned."
        elif status in (TxStatus.PREPARED,):
            if not phase_done:
                hint = "All participants prepared; commit phase will be retried."
            else:
                hint = (
                    f"Commit phase incomplete — {len(phase_done)} committed already, "
                    f"{len(log.participants) - len(phase_done)} left to commit.  "
                    f"Retry commit on remaining."
                )
        elif status == TxStatus.COMMITTING:
            hint = (
                f"Commit phase in progress — {len(phase_done)} done, "
                f"{len(log.participants) - len(phase_done)} left.  Retry."
            )
        elif status in (TxStatus.ROLLING_BACK,):
            hint = "Rollback phase incomplete. Retry rollback on all participants."
        elif status == TxStatus.TRIED:
            if not phase_done:
                hint = "All participants reserved. Confirm phase will be retried."
            else:
                hint = (
                    f"Confirm phase incomplete — {len(phase_done)} confirmed, "
                    f"{len(log.participants) - len(phase_done)} left.  Retry."
                )
        elif status == TxStatus.CONFIRMING:
            hint = (
                f"Confirm phase in progress — {len(phase_done)} done, "
                f"{len(log.participants) - len(phase_done)} left.  Retry."
            )
        elif status == TxStatus.CANCELLING:
            hint = "Cancel phase in progress — retry cancel on remaining."
        else:
            hint = "Run recovery to re-determine outcome."

        return {
            "mode": mode.value,
            "status": status.value,
            "is_terminal": is_terminal,
            "voted_yes": [p.participant_id for p in voted_yes],
            "voted_no": [p.participant_id for p in voted_no],
            "not_voted": [p.participant_id for p in not_voted],
            "phase_done": [p.participant_id for p in phase_done],
            "recovery_hint": hint,
        }

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
            "cancelled=%d prepare_comp=%d try_comp=%d partial=%d skipped=%d errors=%d",
            len(result.committed),
            len(result.rolled_back),
            len(result.confirmed),
            len(result.cancelled),
            len(result.prepare_compensated),
            len(result.try_compensated),
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

    async def _run_phase_and_track(
        self,
        log: TransactionLog,
        result: RecoveryResult,
        phase_fn,
        success_outcome: str,
        success_list: List[str],
    ) -> None:
        tx_id = log.tx_id
        snapshot_before = {
            p.participant_id: p.phase_completed for p in log.participants
        }

        await phase_fn(log, {})

        for p in log.participants:
            before = snapshot_before.get(p.participant_id)
            after = p.phase_completed
            if before != after and after is not None:
                attempt = ParticipantRecoveryAttempt(p.participant_id, after)
                attempt.attempts = self._tm.get_last_attempt_count(p.participant_id) or 1
                attempt.succeeded = True
                result.add_attempt(tx_id, attempt)

        pending_failures = self._tm.consume_pending_failures(tx_id)
        for pid, action, attempts, err in pending_failures:
            attempt = ParticipantRecoveryAttempt(pid, action)
            attempt.attempts = attempts
            attempt.succeeded = False
            attempt.final_error = err
            result.add_attempt(tx_id, attempt)

        final = self._logger.read(tx_id)
        if final and self._check_terminal(final):
            success_list.append(tx_id)
            result.set_outcome(tx_id, success_outcome)
        else:
            result.partial.append(tx_id)
            result.set_outcome(tx_id, RecoveryOutcome.STILL_FAILED)

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
                await self._run_phase_and_track(
                    log, result,
                    self._tm._rollback_phase,
                    RecoveryOutcome.PREPARE_COMPENSATED,
                    result.prepare_compensated,
                )
            else:
                log.status = TxStatus.ROLLED_BACK
                self._logger.append(log)
                self._tm._finalize(tx_id)
                result.rolled_back.append(tx_id)
                result.set_outcome(tx_id, RecoveryOutcome.ROLLBACK_COMPLETED)
                logger.info("2PC recovery: no YES voters, marked ROLLED_BACK")

        elif status == TxStatus.PREPARED:
            all_yes = all(p.vote == Vote.YES for p in log.participants)
            if all_yes:
                logger.info(
                    "2PC recovery: all voted YES, committing: tx=%s", tx_id
                )
                await self._run_phase_and_track(
                    log, result,
                    self._tm._commit_phase,
                    RecoveryOutcome.COMMIT_COMPLETED,
                    result.committed,
                )
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "2PC recovery: mixed votes, rolling back: tx=%s", tx_id
                    )
                    await self._run_phase_and_track(
                        log, result,
                        self._tm._rollback_phase,
                        RecoveryOutcome.ROLLBACK_COMPLETED,
                        result.rolled_back,
                    )
                else:
                    log.status = TxStatus.ROLLED_BACK
                    self._logger.append(log)
                    self._tm._finalize(tx_id)
                    result.rolled_back.append(tx_id)
                    result.set_outcome(tx_id, RecoveryOutcome.ROLLBACK_COMPLETED)

        elif status == TxStatus.COMMITTING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "commit"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "commit"]
            logger.info(
                "2PC recovery: COMMITTING tx=%s, already committed=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_track(
                log, result,
                self._tm._commit_phase,
                RecoveryOutcome.COMMIT_COMPLETED,
                result.committed,
            )

        elif status == TxStatus.ROLLING_BACK:
            already = [p.participant_id for p in log.participants if p.phase_completed == "rollback"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "rollback"]
            logger.info(
                "2PC recovery: ROLLING_BACK tx=%s, already rolled back=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_track(
                log, result,
                self._tm._rollback_phase,
                RecoveryOutcome.ROLLBACK_COMPLETED,
                result.rolled_back,
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
                await self._run_phase_and_track(
                    log, result,
                    self._tm._cancel_phase,
                    RecoveryOutcome.TRY_COMPENSATED,
                    result.try_compensated,
                )
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
                await self._run_phase_and_track(
                    log, result,
                    self._tm._confirm_phase,
                    RecoveryOutcome.CONFIRM_COMPLETED,
                    result.confirmed,
                )
            else:
                any_yes = any(p.vote == Vote.YES for p in log.participants)
                if any_yes:
                    logger.info(
                        "TCC recovery: mixed try results, cancelling: tx=%s", tx_id
                    )
                    await self._run_phase_and_track(
                        log, result,
                        self._tm._cancel_phase,
                        RecoveryOutcome.CANCEL_COMPLETED,
                        result.cancelled,
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
            await self._run_phase_and_track(
                log, result,
                self._tm._confirm_phase,
                RecoveryOutcome.CONFIRM_COMPLETED,
                result.confirmed,
            )

        elif status == TxStatus.CANCELLING:
            already = [p.participant_id for p in log.participants if p.phase_completed == "cancel"]
            pending = [p.participant_id for p in log.participants if p.vote == Vote.YES and p.phase_completed != "cancel"]
            logger.info(
                "TCC recovery: CANCELLING tx=%s, already cancelled=%s, pending=%s",
                tx_id, already, pending,
            )
            await self._run_phase_and_track(
                log, result,
                self._tm._cancel_phase,
                RecoveryOutcome.CANCEL_COMPLETED,
                result.cancelled,
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
                        analysis["recovery_hint"] = "rollback YES voters only (prepare compensation)"
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
                        analysis["recovery_hint"] = "cancel YES tryers only (try compensation)"
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
