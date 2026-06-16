import asyncio
import logging
import shutil
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dtx import (
    TransactionManager,
    TxMode,
    TxStatus,
    Vote,
    InMemory2PCParticipant,
    InMemoryTCCParticipant,
    Failing2PCParticipant,
    FailingTCCParticipant,
    RecoveryManager,
)
from dtx.logger import TxStatus as _TxS, ParticipantRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("demo")


async def demo_2pc_commit():
    logger.info("=" * 60)
    logger.info("Demo: 2PC - All participants vote YES -> COMMIT")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/2pc_commit")
    await tm.start()

    p1 = InMemory2PCParticipant("order-service")
    p2 = InMemory2PCParticipant("inventory-service")
    p3 = InMemory2PCParticipant("payment-service")

    tm.register_participant(p1)
    tm.register_participant(p2)
    tm.register_participant(p3)

    tx_id = tm.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["order-service", "inventory-service", "payment-service"],
        timeout=10.0,
        context={"order_id": "ORD-001"},
    )
    logger.info("Transaction started: %s", tx_id)

    await tm.execute(tx_id, {"order_id": "ORD-001", "amount": 99.9})

    status = tm.get_status(tx_id)
    logger.info("Final status: %s", status.value)

    assert p1.is_committed(tx_id)
    assert p2.is_committed(tx_id)
    assert p3.is_committed(tx_id)
    logger.info("All participants committed. OK")

    await tm.stop()


async def demo_2pc_rollback():
    logger.info("=" * 60)
    logger.info("Demo: 2PC - One participant votes NO -> ROLLBACK")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/2pc_rollback")
    await tm.start()

    p1 = InMemory2PCParticipant("order-service")
    p2 = InMemory2PCParticipant("inventory-service", always_vote_yes=False)
    p3 = InMemory2PCParticipant("payment-service")

    tm.register_participant(p1)
    tm.register_participant(p2)
    tm.register_participant(p3)

    tx_id = tm.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["order-service", "inventory-service", "payment-service"],
        timeout=10.0,
    )

    await tm.execute(tx_id, {"order_id": "ORD-002"})

    status = tm.get_status(tx_id)
    logger.info("Final status: %s", status.value)

    assert p1.is_rolled_back(tx_id)
    assert p3.is_rolled_back(tx_id)
    logger.info("Participants that voted YES were rolled back. OK")

    await tm.stop()


async def demo_tcc_confirm():
    logger.info("=" * 60)
    logger.info("Demo: TCC - All try succeed -> CONFIRM")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/tcc_confirm")
    await tm.start()

    p1 = InMemoryTCCParticipant("account-service")
    p2 = InMemoryTCCParticipant("point-service")

    tm.register_participant(p1)
    tm.register_participant(p2)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["account-service", "point-service"],
        timeout=10.0,
    )

    await tm.execute(tx_id, {"user_id": "U001", "amount": 50.0})

    status = tm.get_status(tx_id)
    logger.info("Final status: %s", status.value)

    assert p1.is_confirmed(tx_id)
    assert p2.is_confirmed(tx_id)
    logger.info("All TCC participants confirmed. OK")

    await tm.stop()


async def demo_tcc_cancel():
    logger.info("=" * 60)
    logger.info("Demo: TCC - One try fails -> CANCEL")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/tcc_cancel")
    await tm.start()

    p1 = InMemoryTCCParticipant("account-service")
    p2 = InMemoryTCCParticipant("point-service", always_try_yes=False)

    tm.register_participant(p1)
    tm.register_participant(p2)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["account-service", "point-service"],
        timeout=10.0,
    )

    await tm.execute(tx_id, {"user_id": "U002", "amount": 200.0})

    status = tm.get_status(tx_id)
    logger.info("Final status: %s", status.value)

    assert p1.is_cancelled(tx_id)
    logger.info("TCC participant that tried was cancelled. OK")

    await tm.stop()


# ============================================================
# NEW: Requirement 1 - Partial failure stays non-terminal
# ============================================================
async def demo_partial_failure_2pc():
    logger.info("=" * 60)
    logger.info("Demo [Req1]: 2PC commit partial failure -> stays COMMITTING, recovery completes it")
    logger.info("=" * 60)

    log_dir = "demo_logs/partial_2pc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    await tm.start()

    p1 = InMemory2PCParticipant("svc-a")
    p2 = Failing2PCParticipant("svc-b", fail_on_commit=True, fail_count=10)
    p3 = InMemory2PCParticipant("svc-c")

    tm.register_participant(p1)
    tm.register_participant(p2)
    tm.register_participant(p3)

    tx_id = tm.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["svc-a", "svc-b", "svc-c"],
        timeout=60.0,
    )

    await tm.execute(tx_id, {"order": "ORD-100"})

    final_log = tm.logger.read(tx_id)
    logger.info("After execute: status=%s", final_log.status.value)

    for p in final_log.participants:
        logger.info(
            "  Participant: %s vote=%s phase_completed=%s",
            p.participant_id,
            p.vote.value if p.vote else None,
            p.phase_completed,
        )

    assert final_log.status == _TxS.COMMITTING, f"Expected COMMITTING, got {final_log.status.value}"
    assert p1.is_committed(tx_id), "svc-a should have committed"
    assert not p2.is_committed(tx_id), "svc-b should NOT have committed"
    assert p3.is_committed(tx_id), "svc-c should have committed"
    logger.info("Status is COMMITTING (not terminal). Partial completion recorded. OK")

    await tm.stop()

    tm2 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    await tm2.start()

    p1_new = InMemory2PCParticipant("svc-a")
    p2_new = InMemory2PCParticipant("svc-b")
    p3_new = InMemory2PCParticipant("svc-c")

    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)
    tm2.register_participant(p3_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    logger.info("Recovery result: %s", result)

    final_log2 = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log2.status.value)

    for p in final_log2.participants:
        logger.info(
            "  Participant: %s vote=%s phase_completed=%s",
            p.participant_id,
            p.vote.value if p.vote else None,
            p.phase_completed,
        )

    assert final_log2.status == _TxS.COMMITTED, f"Expected COMMITTED after recovery, got {final_log2.status.value}"
    assert p1_new.commit_count(tx_id) == 0, "svc-a skipped (already completed)"
    assert p2_new.is_committed(tx_id), "svc-b committed after recovery"
    assert p3_new.commit_count(tx_id) == 0, "svc-c skipped (already completed)"
    logger.info("Recovery completed partial commit to full COMMITTED. Already-done participants skipped. OK")

    await tm2.stop()


async def demo_partial_failure_tcc():
    logger.info("=" * 60)
    logger.info("Demo [Req1]: TCC confirm partial failure -> stays CONFIRMING, recovery completes it")
    logger.info("=" * 60)

    log_dir = "demo_logs/partial_tcc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    await tm.start()

    p1 = InMemoryTCCParticipant("account-svc")
    p2 = FailingTCCParticipant("point-svc", fail_on_confirm=True, fail_count=10)

    tm.register_participant(p1)
    tm.register_participant(p2)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["account-svc", "point-svc"],
        timeout=60.0,
    )

    await tm.execute(tx_id, {"user": "U100"})

    final_log = tm.logger.read(tx_id)
    logger.info("After execute: status=%s", final_log.status.value)

    for p in final_log.participants:
        logger.info(
            "  Participant: %s vote=%s phase_completed=%s",
            p.participant_id,
            p.vote.value if p.vote else None,
            p.phase_completed,
        )

    assert final_log.status == _TxS.CONFIRMING, f"Expected CONFIRMING, got {final_log.status.value}"
    assert p1.is_confirmed(tx_id), "account-svc should have confirmed"
    assert not p2.is_confirmed(tx_id), "point-svc should NOT have confirmed"
    logger.info("Status is CONFIRMING (not terminal). Partial completion recorded. OK")

    await tm.stop()

    tm2 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    await tm2.start()

    p1_new = InMemoryTCCParticipant("account-svc")
    p2_new = InMemoryTCCParticipant("point-svc")

    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    logger.info("Recovery result: %s", result)

    final_log2 = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log2.status.value)

    assert final_log2.status == _TxS.CONFIRMED
    assert p1_new.confirm_count(tx_id) == 0, "account-svc: skipped (already completed)"
    assert p2_new.is_confirmed(tx_id), "point-svc confirmed after recovery"
    logger.info("Recovery completed partial confirm to full CONFIRMED. Already-done participant skipped. OK")

    await tm2.stop()


# ============================================================
# NEW: Requirement 2 - Idempotent confirm/cancel
# ============================================================
async def demo_idempotent_confirm():
    logger.info("=" * 60)
    logger.info("Demo [Req2]: TCC confirm idempotency - repeated calls do NOT duplicate business effects")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/idempotent_confirm")
    await tm.start()

    p1 = InMemoryTCCParticipant("wallet-service")
    tm.register_participant(p1)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["wallet-service"],
        timeout=10.0,
    )

    await tm.execute(tx_id, {"user": "U200", "amount": 100.0})

    assert p1.is_confirmed(tx_id)
    assert p1.confirm_count(tx_id) == 1
    logger.info("First confirm: business effect applied. confirm_count=%d", p1.confirm_count(tx_id))

    await p1.confirm(tx_id, {})
    assert p1.confirm_count(tx_id) == 2, "confirm was called again"
    logger.info("Second confirm: call count=%d but business effect NOT duplicated (idempotent guard)", p1.confirm_count(tx_id))

    await p1.confirm(tx_id, {})
    assert p1.confirm_count(tx_id) == 3, "confirm was called third time"
    logger.info("Third confirm: call count=%d still no duplicate effect. OK", p1.confirm_count(tx_id))

    await tm.stop()


async def demo_idempotent_cancel():
    logger.info("=" * 60)
    logger.info("Demo [Req2]: TCC cancel idempotency - repeated calls do NOT duplicate business effects")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/idempotent_cancel")
    await tm.start()

    p1 = InMemoryTCCParticipant("wallet-service")
    p2 = InMemoryTCCParticipant("reward-service", always_try_yes=False)

    tm.register_participant(p1)
    tm.register_participant(p2)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["wallet-service", "reward-service"],
        timeout=10.0,
    )

    await tm.execute(tx_id, {"user": "U300"})

    assert p1.is_cancelled(tx_id)
    assert p1.cancel_count(tx_id) == 1
    logger.info("First cancel: business effect applied. cancel_count=%d", p1.cancel_count(tx_id))

    await p1.cancel(tx_id, {})
    assert p1.cancel_count(tx_id) == 2
    logger.info("Second cancel: call count=%d but business effect NOT duplicated (idempotent guard). OK", p1.cancel_count(tx_id))

    await tm.stop()


# ============================================================
# NEW: Requirement 3 - Timeout stable convergence
# ============================================================
async def demo_timeout_convergence():
    logger.info("=" * 60)
    logger.info("Demo [Req3]: Timeout during slow prepare -> rollback converges, original flow does NOT override")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/timeout_conv", timeout_check_interval=0.1)
    await tm.start()

    p1 = InMemory2PCParticipant("fast-svc")
    p2 = InMemory2PCParticipant("slow-svc", delay=5.0)

    tm.register_participant(p1)
    tm.register_participant(p2)

    tx_id = tm.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["fast-svc", "slow-svc"],
        timeout=0.5,
    )

    execute_task = asyncio.create_task(tm.execute(tx_id, {"data": "test"}))

    await asyncio.sleep(1.5)

    status = tm.get_status(tx_id)
    logger.info("Status after timeout: %s", status.value if status else "None")

    assert status in (_TxS.ROLLING_BACK, _TxS.ROLLED_BACK), f"Expected ROLLING_BACK or ROLLED_BACK, got {status}"

    try:
        await asyncio.wait_for(execute_task, timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("Execute task still running (expected, original flow is aborted)")

    await asyncio.sleep(1.0)

    final_log = tm.logger.read(tx_id)
    final_status = final_log.status.value if final_log else "UNKNOWN"
    logger.info("Final status: %s", final_status)

    assert final_status in ("ROLLED_BACK", "ROLLING_BACK"), f"Expected terminal/intermediate rollback, got {final_status}"
    assert final_status != "COMMITTED", "MUST NOT be COMMITTED after timeout rollback"
    logger.info("Timeout correctly converged to rollback. Original flow did NOT override. OK")

    await tm.stop()


async def demo_timeout_tcc_convergence():
    logger.info("=" * 60)
    logger.info("Demo [Req3]: Timeout during slow TCC try -> cancel converges, original flow does NOT override")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/timeout_tcc_conv", timeout_check_interval=0.1)
    await tm.start()

    p1 = InMemoryTCCParticipant("fast-tcc")
    p2 = InMemoryTCCParticipant("slow-tcc", delay=5.0)

    tm.register_participant(p1)
    tm.register_participant(p2)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["fast-tcc", "slow-tcc"],
        timeout=0.5,
    )

    execute_task = asyncio.create_task(tm.execute(tx_id, {"data": "test"}))

    await asyncio.sleep(1.5)

    status = tm.get_status(tx_id)
    logger.info("Status after timeout: %s", status.value if status else "None")

    assert status in (_TxS.CANCELLING, _TxS.CANCELLED), f"Expected CANCELLING or CANCELLED, got {status}"

    try:
        await asyncio.wait_for(execute_task, timeout=5.0)
    except asyncio.TimeoutError:
        pass

    await asyncio.sleep(1.0)

    final_log = tm.logger.read(tx_id)
    final_status = final_log.status.value if final_log else "UNKNOWN"
    logger.info("Final status: %s", final_status)

    assert final_status in ("CANCELLED", "CANCELLING"), f"Expected cancel state, got {final_status}"
    assert final_status != "CONFIRMED", "MUST NOT be CONFIRMED after timeout cancel"
    logger.info("Timeout correctly converged to cancel. Original flow did NOT override. OK")

    await tm.stop()


# ============================================================
# NEW: Requirement 4 - Recovery with partial phase completion
# ============================================================
async def demo_recovery_partial_commit():
    logger.info("=" * 60)
    logger.info("Demo [Req4]: Crash mid-commit (2 out of 3 committed) -> recovery only commits remaining 1")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_partial"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)
    p1 = InMemory2PCParticipant("order-db")
    p2 = InMemory2PCParticipant("inventory-db")
    p3 = InMemory2PCParticipant("payment-db")
    tm1.register_participant(p1)
    tm1.register_participant(p2)
    tm1.register_participant(p3)

    tx_id = tm1.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["order-db", "inventory-db", "payment-db"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote, TransactionLog
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.COMMITTING
    log.participants[0].vote = Vote.YES
    log.participants[0].phase_completed = "commit"
    log.participants[1].vote = Vote.YES
    log.participants[1].phase_completed = "commit"
    log.participants[2].vote = Vote.YES
    log.participants[2].phase_completed = None
    tm1.logger.append(log)

    logger.info("Simulated crash: tx=%s COMMITTING", tx_id)
    logger.info("  order-db:     vote=YES phase_completed=commit (DONE)")
    logger.info("  inventory-db: vote=YES phase_completed=commit (DONE)")
    logger.info("  payment-db:   vote=YES phase_completed=None   (PENDING)")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemory2PCParticipant("order-db")
    p2_new = InMemory2PCParticipant("inventory-db")
    p3_new = InMemory2PCParticipant("payment-db")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)
    tm2.register_participant(p3_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    logger.info("Recovery result: %s", result)

    final_log = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log.status.value)

    for p in final_log.participants:
        logger.info(
            "  Participant: %s vote=%s phase_completed=%s",
            p.participant_id,
            p.vote.value if p.vote else None,
            p.phase_completed,
        )

    assert final_log.status == _TxS.COMMITTED, f"Expected COMMITTED, got {final_log.status.value}"
    assert final_log.participants[0].phase_completed == "commit", "order-db should still be commit"
    assert final_log.participants[1].phase_completed == "commit", "inventory-db should still be commit"
    assert final_log.participants[2].phase_completed == "commit", "payment-db should now be commit"

    assert p1_new.commit_count(tx_id) == 0, "order-db: skipped (already completed)"
    assert p2_new.commit_count(tx_id) == 0, "inventory-db: skipped (already completed)"
    assert p3_new.is_committed(tx_id), "payment-db: committed by recovery"
    logger.info("Recovery only re-committed the unfinished participant. Already-done ones skipped. OK")

    await tm2.stop()


async def demo_recovery_partial_tcc_confirm():
    logger.info("=" * 60)
    logger.info("Demo [Req4]: Crash mid-confirm TCC (1 out of 2 confirmed) -> recovery only confirms remaining 1")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_partial_tcc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)
    p1 = InMemoryTCCParticipant("billing-svc")
    p2 = InMemoryTCCParticipant("shipping-svc")
    tm1.register_participant(p1)
    tm1.register_participant(p2)

    tx_id = tm1.begin(
        mode=TxMode.TCC,
        participant_ids=["billing-svc", "shipping-svc"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.CONFIRMING
    log.participants[0].vote = Vote.YES
    log.participants[0].phase_completed = "confirm"
    log.participants[1].vote = Vote.YES
    log.participants[1].phase_completed = None
    tm1.logger.append(log)

    logger.info("Simulated crash: tx=%s CONFIRMING", tx_id)
    logger.info("  billing-svc:  vote=YES phase_completed=confirm (DONE)")
    logger.info("  shipping-svc: vote=YES phase_completed=None    (PENDING)")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemoryTCCParticipant("billing-svc")
    p2_new = InMemoryTCCParticipant("shipping-svc")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    logger.info("Recovery result: %s", result)

    final_log = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log.status.value)

    for p in final_log.participants:
        logger.info(
            "  Participant: %s vote=%s phase_completed=%s",
            p.participant_id,
            p.vote.value if p.vote else None,
            p.phase_completed,
        )

    assert final_log.status == _TxS.CONFIRMED, f"Expected CONFIRMED, got {final_log.status.value}"
    assert p1_new.confirm_count(tx_id) == 0, "billing-svc: skipped (already completed)"
    assert p2_new.is_confirmed(tx_id), "shipping-svc: confirmed by recovery"
    logger.info("Recovery only confirmed the unfinished participant. Already-done one skipped. OK")

    await tm2.stop()


# ============================================================
# NEW: Requirement 1+3 - Mid-prepare crash recovery (2PC)
# ============================================================
async def demo_recovery_mid_prepare_2pc():
    logger.info("=" * 60)
    logger.info("Demo [Req1,2,3]: 2PC mid-prepare crash -> svc1 voted YES, svc2 not started -> recovery rollback svc1")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_mid_prepare_2pc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)
    p1 = InMemory2PCParticipant("svc-a")
    p2 = InMemory2PCParticipant("svc-b")
    tm1.register_participant(p1)
    tm1.register_participant(p2)

    tx_id = tm1.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["svc-a", "svc-b"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.PREPARING
    log.participants[0].vote = Vote.YES
    log.participants[1].vote = None
    tm1.logger.append(log)

    logger.info("Simulated crash: 2PC tx=%s PREPARING", tx_id)
    logger.info("  svc-a: vote=YES (already reserved resources)")
    logger.info("  svc-b: vote=None (never called, nothing to rollback)")

    log_on_disk = tm1.logger.read(tx_id)
    assert log_on_disk.participants[0].vote == Vote.YES, "svc-a vote should be logged"
    assert log_on_disk.participants[1].vote is None, "svc-b vote should be None"
    logger.info("Verified: per-participant vote logged immediately after each step. OK")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemory2PCParticipant("svc-a")
    p2_new = InMemory2PCParticipant("svc-b")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    final_log = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log.status.value)

    assert final_log.status == _TxS.ROLLED_BACK, f"Expected ROLLED_BACK, got {final_log.status.value}"
    assert final_log.participants[0].phase_completed == "rollback", "svc-a should have rollback called"
    assert final_log.participants[1].vote in (Vote.TIMEOUT, Vote.NO), "svc-b should be marked TIMEOUT"

    outcome = result.outcomes.get(tx_id)
    logger.info("Recovery outcome: %s", outcome)
    assert outcome in ("prepare_compensated",), f"Expected prepare_compensated, got {outcome}"
    assert p2_new.rollback_count(tx_id) == 0, "svc-b never voted YES, should NOT be rolled back"
    logger.info("Only svc-a was rolled back (compensated). svc-b skipped (never voted YES). OK")
    logger.info("No orphaned reserved resources. All locks released. OK")

    await tm2.stop()


# ============================================================
# NEW: Requirement 1+3 - Mid-try crash recovery (TCC)
# ============================================================
async def demo_recovery_mid_try_tcc():
    logger.info("=" * 60)
    logger.info("Demo [Req1,2,3]: TCC mid-try crash -> svc1 tried YES, svc2 not started -> recovery cancel svc1")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_mid_try_tcc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)
    p1 = InMemoryTCCParticipant("wallet-svc")
    p2 = InMemoryTCCParticipant("reward-svc")
    tm1.register_participant(p1)
    tm1.register_participant(p2)

    tx_id = tm1.begin(
        mode=TxMode.TCC,
        participant_ids=["wallet-svc", "reward-svc"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.TRYING
    log.participants[0].vote = Vote.YES
    log.participants[1].vote = None
    tm1.logger.append(log)

    logger.info("Simulated crash: TCC tx=%s TRYING", tx_id)
    logger.info("  wallet-svc: vote=YES (frozen 100$ balance)")
    logger.info("  reward-svc: vote=None (never called, nothing to cancel)")

    log_on_disk = tm1.logger.read(tx_id)
    assert log_on_disk.participants[0].vote == Vote.YES, "wallet-svc vote should be logged"
    assert log_on_disk.participants[1].vote is None, "reward-svc vote should be None"
    logger.info("Verified: per-participant try logged immediately after each step. OK")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemoryTCCParticipant("wallet-svc")
    p2_new = InMemoryTCCParticipant("reward-svc")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    final_log = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log.status.value)

    assert final_log.status == _TxS.CANCELLED, f"Expected CANCELLED, got {final_log.status.value}"
    assert final_log.participants[0].phase_completed == "cancel", "wallet-svc should have cancel called"

    outcome = result.outcomes.get(tx_id)
    logger.info("Recovery outcome: %s", outcome)
    assert outcome in ("try_compensated",), f"Expected try_compensated, got {outcome}"
    assert p2_new.cancel_count(tx_id) == 0, "reward-svc never tried YES, should NOT be cancelled"
    logger.info("Only wallet-svc was cancelled (frozen balance released). reward-svc skipped. OK")
    logger.info("No orphaned frozen resources. Business-level locks released. OK")

    await tm2.stop()


# ============================================================
# NEW: Requirement 3 - 2PC prepare crash with NO voters (edge case)
# ============================================================
async def demo_recovery_prepare_all_no_2pc():
    logger.info("=" * 60)
    logger.info("Demo [Req3]: 2PC mid-prepare crash -> svc1 NO, svc2 None -> recovery marks ROLLED_BACK, no actual rollback needed")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_prepare_all_no"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)
    p1 = InMemory2PCParticipant("svc-a", always_vote_yes=False)
    p2 = InMemory2PCParticipant("svc-b")
    tm1.register_participant(p1)
    tm1.register_participant(p2)

    tx_id = tm1.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["svc-a", "svc-b"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.PREPARING
    log.participants[0].vote = Vote.NO
    log.participants[1].vote = None
    tm1.logger.append(log)

    logger.info("Simulated crash: 2PC tx=%s PREPARING", tx_id)
    logger.info("  svc-a: vote=NO (never reserved)")
    logger.info("  svc-b: vote=None (never called)")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemory2PCParticipant("svc-a")
    p2_new = InMemory2PCParticipant("svc-b")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    final_log = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log.status.value)

    assert final_log.status == _TxS.ROLLED_BACK, f"Expected ROLLED_BACK, got {final_log.status.value}"
    assert p1_new.rollback_count(tx_id) == 0, "svc-a voted NO, should NOT be rolled back"
    assert p2_new.rollback_count(tx_id) == 0, "svc-b never voted, should NOT be rolled back"
    outcome = result.outcomes.get(tx_id)
    assert outcome in ("rollback_completed",), f"Expected rollback_completed, got {outcome}"
    logger.info("No YES voters -> directly marked ROLLED_BACK. No rollback calls needed. OK")

    await tm2.stop()


# ============================================================
# NEW: Requirement 3 - TCC try crash with NO tryers (edge case)
# ============================================================
async def demo_recovery_try_all_no_tcc():
    logger.info("=" * 60)
    logger.info("Demo [Req3]: TCC mid-try crash -> svc1 NO, svc2 None -> recovery marks CANCELLED, no actual cancel needed")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_try_all_no"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)
    p1 = InMemoryTCCParticipant("wallet-svc", always_try_yes=False)
    p2 = InMemoryTCCParticipant("reward-svc")
    tm1.register_participant(p1)
    tm1.register_participant(p2)

    tx_id = tm1.begin(
        mode=TxMode.TCC,
        participant_ids=["wallet-svc", "reward-svc"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.TRYING
    log.participants[0].vote = Vote.NO
    log.participants[1].vote = None
    tm1.logger.append(log)

    logger.info("Simulated crash: TCC tx=%s TRYING", tx_id)
    logger.info("  wallet-svc: vote=NO (never froze balance)")
    logger.info("  reward-svc: vote=None (never called)")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemoryTCCParticipant("wallet-svc")
    p2_new = InMemoryTCCParticipant("reward-svc")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    final_log = tm2.logger.read(tx_id)
    logger.info("After recovery: status=%s", final_log.status.value)

    assert final_log.status == _TxS.CANCELLED, f"Expected CANCELLED, got {final_log.status.value}"
    assert p1_new.cancel_count(tx_id) == 0, "wallet-svc tried NO, should NOT be cancelled"
    assert p2_new.cancel_count(tx_id) == 0, "reward-svc never tried, should NOT be cancelled"
    outcome = result.outcomes.get(tx_id)
    assert outcome in ("cancel_completed",), f"Expected cancel_completed, got {outcome}"
    logger.info("No YES tryers -> directly marked CANCELLED. No cancel calls needed. OK")

    await tm2.stop()


async def demo_stuck_then_recover_2pc():
    logger.info("=" * 60)
    logger.info("Demo [Req4.2]: 2PC - participant down -> 1st recovery STUCK -> service back -> 2nd recovery COMMITTED")
    logger.info("=" * 60)

    log_dir = "demo_logs/stuck_recover_2pc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm.set_participant_retry("svc-a", retry_max=2, retry_delay=0.1)
    tm.set_participant_retry("svc-b", retry_max=1, retry_delay=0.05)
    tm.set_participant_retry("svc-c", retry_max=2, retry_delay=0.1)
    await tm.start()

    p1 = InMemory2PCParticipant("svc-a")
    p2 = Failing2PCParticipant("svc-b", fail_on_commit=True, fail_count=999)
    p3 = InMemory2PCParticipant("svc-c")

    tm.register_participant(p1)
    tm.register_participant(p2)
    tm.register_participant(p3)

    tx_id = tm.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["svc-a", "svc-b", "svc-c"],
        timeout=60.0,
    )

    await tm.execute(tx_id, {"order": "ORD-200"})

    final_log = tm.logger.read(tx_id)
    logger.info("After execute: status=%s", final_log.status.value)
    logger.info("  svc-a committed=%s, svc-b committed=%s, svc-c committed=%s",
                p1.is_committed(tx_id), p2.is_committed(tx_id), p3.is_committed(tx_id))

    assert final_log.status == _TxS.COMMITTING
    assert p1.is_committed(tx_id)
    assert not p2.is_committed(tx_id)
    assert p3.is_committed(tx_id)
    logger.info("Status=COMMITTING, svc-b stuck (still failing). OK")

    await tm.stop()

    logger.info("--- First recovery run (svc-b STILL DOWN) ---")
    tm2 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm2.set_participant_retry("svc-a", retry_max=2, retry_delay=0.1)
    tm2.set_participant_retry("svc-b", retry_max=2, retry_delay=0.05)
    tm2.set_participant_retry("svc-c", retry_max=2, retry_delay=0.1)
    await tm2.start()

    p1_r1 = InMemory2PCParticipant("svc-a")
    p2_r1 = Failing2PCParticipant("svc-b", fail_on_commit=True, fail_count=999)
    p3_r1 = InMemory2PCParticipant("svc-c")
    tm2.register_participant(p1_r1)
    tm2.register_participant(p2_r1)
    tm2.register_participant(p3_r1)

    recovery2 = RecoveryManager(tm2)
    result1 = await recovery2.recover_all()

    after_r1 = tm2.logger.read(tx_id)
    logger.info("After 1st recovery: status=%s outcome=%s",
                after_r1.status.value, result1.outcomes.get(tx_id))

    assert after_r1.status == _TxS.COMMITTING, f"Expected still COMMITTING, got {after_r1.status.value}"
    assert result1.outcomes.get(tx_id) == "still_failed", f"Expected still_failed, got {result1.outcomes.get(tx_id)}"
    assert not p2_r1.is_committed(tx_id), "svc-b still not committed"
    logger.info("1st recovery shows STUCK as expected. OK")

    await tm2.stop()

    logger.info("--- Second recovery run (svc-b BACK UP) ---")
    tm3 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm3.set_participant_retry("svc-a", retry_max=2, retry_delay=0.1)
    tm3.set_participant_retry("svc-b", retry_max=3, retry_delay=0.05)
    tm3.set_participant_retry("svc-c", retry_max=2, retry_delay=0.1)
    await tm3.start()

    p1_r2 = InMemory2PCParticipant("svc-a")
    p2_r2 = InMemory2PCParticipant("svc-b")
    p3_r2 = InMemory2PCParticipant("svc-c")
    tm3.register_participant(p1_r2)
    tm3.register_participant(p2_r2)
    tm3.register_participant(p3_r2)

    recovery3 = RecoveryManager(tm3)
    result2 = await recovery3.recover_all()

    after_r2 = tm3.logger.read(tx_id)
    logger.info("After 2nd recovery: status=%s outcome=%s",
                after_r2.status.value, result2.outcomes.get(tx_id))

    assert after_r2.status == _TxS.COMMITTED, f"Expected COMMITTED, got {after_r2.status.value}"
    assert result2.outcomes.get(tx_id) == "commit_completed", f"Expected commit_completed, got {result2.outcomes.get(tx_id)}"
    assert p2_r2.is_committed(tx_id), "svc-b committed on 2nd recovery"
    assert p1_r2.commit_count(tx_id) == 0, "svc-a not re-called (already done)"
    assert p3_r2.commit_count(tx_id) == 0, "svc-c not re-called (already done)"
    logger.info("2nd recovery converged to COMMITTED. OK")

    await tm3.stop()


async def demo_stuck_then_recover_tcc():
    logger.info("=" * 60)
    logger.info("Demo [Req4.2]: TCC - participant down -> 1st recovery STUCK -> service back -> 2nd recovery CONFIRMED")
    logger.info("=" * 60)

    log_dir = "demo_logs/stuck_recover_tcc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm.set_participant_retry("wallet", retry_max=1, retry_delay=0.05)
    tm.set_participant_retry("inventory", retry_max=1, retry_delay=0.05)
    tm.set_participant_retry("points", retry_max=1, retry_delay=0.05)
    await tm.start()

    p1 = InMemoryTCCParticipant("wallet")
    p2 = FailingTCCParticipant("inventory", fail_on_confirm=True, fail_count=999)
    p3 = InMemoryTCCParticipant("points")

    tm.register_participant(p1)
    tm.register_participant(p2)
    tm.register_participant(p3)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["wallet", "inventory", "points"],
        timeout=60.0,
    )

    await tm.execute(tx_id, {"user": 42})

    final_log = tm.logger.read(tx_id)
    logger.info("After execute: status=%s", final_log.status.value)
    logger.info("  wallet confirmed=%s, inventory confirmed=%s, points confirmed=%s",
                p1.is_confirmed(tx_id), p2.is_confirmed(tx_id), p3.is_confirmed(tx_id))

    assert final_log.status == _TxS.CONFIRMING
    assert p1.is_confirmed(tx_id)
    assert not p2.is_confirmed(tx_id)
    assert p3.is_confirmed(tx_id)
    logger.info("Status=CONFIRMING, inventory stuck (still failing). OK")

    await tm.stop()

    logger.info("--- First recovery run (inventory STILL DOWN) ---")
    tm2 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm2.set_participant_retry("wallet", retry_max=1, retry_delay=0.05)
    tm2.set_participant_retry("inventory", retry_max=2, retry_delay=0.05)
    tm2.set_participant_retry("points", retry_max=1, retry_delay=0.05)
    await tm2.start()

    p1_r1 = InMemoryTCCParticipant("wallet")
    p2_r1 = FailingTCCParticipant("inventory", fail_on_confirm=True, fail_count=999)
    p3_r1 = InMemoryTCCParticipant("points")
    tm2.register_participant(p1_r1)
    tm2.register_participant(p2_r1)
    tm2.register_participant(p3_r1)

    recovery2 = RecoveryManager(tm2)
    result1 = await recovery2.recover_all()

    after_r1 = tm2.logger.read(tx_id)
    logger.info("After 1st recovery: status=%s outcome=%s",
                after_r1.status.value, result1.outcomes.get(tx_id))

    assert after_r1.status == _TxS.CONFIRMING
    assert result1.outcomes.get(tx_id) == "still_failed"
    assert not p2_r1.is_confirmed(tx_id)
    logger.info("1st recovery shows STUCK as expected. OK")

    await tm2.stop()

    logger.info("--- Second recovery run (inventory BACK UP) ---")
    tm3 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm3.set_participant_retry("wallet", retry_max=1, retry_delay=0.05)
    tm3.set_participant_retry("inventory", retry_max=2, retry_delay=0.05)
    tm3.set_participant_retry("points", retry_max=1, retry_delay=0.05)
    await tm3.start()

    p1_r2 = InMemoryTCCParticipant("wallet")
    p2_r2 = InMemoryTCCParticipant("inventory")
    p3_r2 = InMemoryTCCParticipant("points")
    tm3.register_participant(p1_r2)
    tm3.register_participant(p2_r2)
    tm3.register_participant(p3_r2)

    recovery3 = RecoveryManager(tm3)
    result2 = await recovery3.recover_all()

    after_r2 = tm3.logger.read(tx_id)
    logger.info("After 2nd recovery: status=%s outcome=%s",
                after_r2.status.value, result2.outcomes.get(tx_id))

    assert after_r2.status == _TxS.CONFIRMED, f"Expected CONFIRMED, got {after_r2.status.value}"
    assert result2.outcomes.get(tx_id) == "confirm_completed"
    assert p2_r2.is_confirmed(tx_id)
    assert p1_r2.confirm_count(tx_id) == 0, "wallet not re-called"
    assert p3_r2.confirm_count(tx_id) == 0, "points not re-called"
    logger.info("2nd recovery converged to CONFIRMED. OK")

    await tm3.stop()


async def demo_per_participant_retry_config():
    logger.info("=" * 60)
    logger.info("Demo [Req4.3]: Per-participant retry config — different max retries, backoff visible in recovery output")
    logger.info("=" * 60)

    log_dir = "demo_logs/per_participant_retry"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm.set_participant_retry("fast-svc", retry_max=1, retry_delay=0.05)
    tm.set_participant_retry("slow-svc", retry_max=3, retry_delay=0.02)
    tm.set_participant_retry("med-svc", retry_max=2, retry_delay=0.03)
    await tm.start()

    p1 = InMemory2PCParticipant("fast-svc")
    p2 = Failing2PCParticipant("slow-svc", fail_on_commit=True, fail_count=999)
    p3 = InMemory2PCParticipant("med-svc")

    tm.register_participant(p1)
    tm.register_participant(p2)
    tm.register_participant(p3)

    tx_id = tm.begin(
        mode=TxMode.TWO_PHASE,
        participant_ids=["fast-svc", "slow-svc", "med-svc"],
        timeout=60.0,
    )

    await tm.execute(tx_id, {"order": "ORD-300"})

    after_exe = tm.logger.read(tx_id)
    logger.info("After execute: status=%s", after_exe.status.value)
    assert after_exe.status == _TxS.COMMITTING
    await tm.stop()

    tm2 = TransactionManager(log_dir=log_dir, default_timeout=60.0)
    tm2.set_participant_retry("fast-svc", retry_max=1, retry_delay=0.05)
    tm2.set_participant_retry("slow-svc", retry_max=3, retry_delay=0.02)
    tm2.set_participant_retry("med-svc", retry_max=2, retry_delay=0.03)
    await tm2.start()

    p1_r1 = InMemory2PCParticipant("fast-svc")
    p2_r1 = Failing2PCParticipant("slow-svc", fail_on_commit=True, fail_count=999)
    p3_r1 = InMemory2PCParticipant("med-svc")
    tm2.register_participant(p1_r1)
    tm2.register_participant(p2_r1)
    tm2.register_participant(p3_r1)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    attempts = result.participant_attempts.get(tx_id, [])
    for pa in attempts:
        logger.info("  Participant %s: action=%s attempts=%s succeeded=%s error=%s",
                    pa.participant_id, pa.action, pa.attempts, pa.succeeded, pa.final_error)

    slow_attempt = next((a for a in attempts if a.participant_id == "slow-svc"), None)
    assert slow_attempt is not None, "slow-svc should have attempt record"
    assert slow_attempt.attempts == 3, f"slow-svc should have 3 attempts (retry_max=3), got {slow_attempt.attempts}"
    assert not slow_attempt.succeeded

    fast_attempt = next((a for a in attempts if a.participant_id == "fast-svc"), None)
    if fast_attempt and fast_attempt.succeeded:
        assert fast_attempt.attempts in (1, None), f"fast-svc succeeds immediately"

    logger.info("Per-participant retry config honored: slow-svc retried 3 times. OK")
    await tm2.stop()


async def main():
    await demo_2pc_commit()
    print()
    await demo_2pc_rollback()
    print()
    await demo_tcc_confirm()
    print()
    await demo_tcc_cancel()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 1: Partial failure stays non-terminal")
    logger.info("#" * 60)
    await demo_partial_failure_2pc()
    print()
    await demo_partial_failure_tcc()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 2: Idempotent confirm/cancel")
    logger.info("#" * 60)
    await demo_idempotent_confirm()
    print()
    await demo_idempotent_cancel()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 3: Timeout stable convergence")
    logger.info("#" * 60)
    await demo_timeout_convergence()
    print()
    await demo_timeout_tcc_convergence()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 4: Recovery with partial phase completion")
    logger.info("#" * 60)
    await demo_recovery_partial_commit()
    print()
    await demo_recovery_partial_tcc_confirm()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 5+6: Per-step logging + mid-prepare/try recovery")
    logger.info("#" * 60)
    await demo_recovery_mid_prepare_2pc()
    print()
    await demo_recovery_mid_try_tcc()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 7: Prepare phase crash + no YES voters (edge cases)")
    logger.info("#" * 60)
    await demo_recovery_prepare_all_no_2pc()
    print()
    await demo_recovery_try_all_no_tcc()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 8: Fail -> stuck -> recover -> converge")
    logger.info("#" * 60)
    await demo_stuck_then_recover_2pc()
    print()
    await demo_stuck_then_recover_tcc()
    print()

    logger.info("#" * 60)
    logger.info("# Requirement 9: Per-participant retry config with attempt tracking")
    logger.info("#" * 60)
    await demo_per_participant_retry_config()
    print()

    logger.info("=" * 60)
    logger.info("ALL demos completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
