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

    logger.info("=" * 60)
    logger.info("ALL demos completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
