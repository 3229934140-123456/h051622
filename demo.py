import asyncio
import logging
import shutil
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dtx import (
    TransactionManager,
    TxMode,
    InMemory2PCParticipant,
    InMemoryTCCParticipant,
    RecoveryManager,
)

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


async def demo_tcc_idempotent_retry():
    logger.info("=" * 60)
    logger.info("Demo: TCC - Confirm/Cancel idempotency (retries do not duplicate)")
    logger.info("=" * 60)

    tm = TransactionManager(log_dir="demo_logs/tcc_idempotent")
    await tm.start()

    p1 = InMemoryTCCParticipant("idempotent-service")
    tm.register_participant(p1)

    tx_id = tm.begin(
        mode=TxMode.TCC,
        participant_ids=["idempotent-service"],
        timeout=10.0,
    )

    await tm.execute(tx_id, {"key": "val"})

    assert p1.confirm_count(tx_id) == 1
    logger.info("Confirm called exactly once (first execution). Count=%d", p1.confirm_count(tx_id))

    log = tm.logger.read(tx_id)
    await tm._confirm_phase(log, {})

    assert p1.confirm_count(tx_id) == 2
    logger.info("Confirm called again (recovery replay). Count=%d - participant must be idempotent!", p1.confirm_count(tx_id))
    logger.info("Idempotency is a PARTICIPANT responsibility: confirm/cancel must check if already done. OK")

    await tm.stop()


async def demo_recovery():
    logger.info("=" * 60)
    logger.info("Demo: Recovery - Coordinator crashes, then recovers")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery"
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

    log = tm1.logger.read(tx_id)
    log.status = TxMode.TWO_PHASE and __import__("dtx.logger", fromlist=["TxStatus"]).TxStatus.PREPARED
    for p in log.participants:
        p.vote = __import__("dtx.logger", fromlist=["Vote"]).Vote.YES
    tm1.logger.append(log)

    logger.info("Simulated crash: transaction %s is PREPARED with all YES votes", tx_id)
    logger.info("Coordinator process dies. Transaction log is on disk.")

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemory2PCParticipant("svc-a")
    p2_new = InMemory2PCParticipant("svc-b")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    logger.info("Recovery result: %s", result)
    logger.info("Transaction status after recovery: %s", tm2.get_status(tx_id).value)

    assert tm2.get_status(tx_id).value in ("COMMITTED", "ROLLED_BACK")
    logger.info("Recovery successfully resolved the dangling transaction. OK")

    await tm2.stop()


async def demo_recovery_tcc():
    logger.info("=" * 60)
    logger.info("Demo: Recovery - TCC transaction crashes after Try")
    logger.info("=" * 60)

    log_dir = "demo_logs/recovery_tcc"
    if os.path.exists(log_dir):
        shutil.rmtree(log_dir)

    tm1 = TransactionManager(log_dir=log_dir)

    p1 = InMemoryTCCParticipant("tcc-svc-a")
    p2 = InMemoryTCCParticipant("tcc-svc-b")
    tm1.register_participant(p1)
    tm1.register_participant(p2)

    tx_id = tm1.begin(
        mode=TxMode.TCC,
        participant_ids=["tcc-svc-a", "tcc-svc-b"],
        timeout=60.0,
    )

    from dtx.logger import TxStatus, Vote
    log = tm1.logger.read(tx_id)
    log.status = TxStatus.TRIED
    for p in log.participants:
        p.vote = Vote.YES
    tm1.logger.append(log)

    logger.info("Simulated crash: TCC transaction %s is TRIED with all YES votes", tx_id)

    tm2 = TransactionManager(log_dir=log_dir)
    await tm2.start()

    p1_new = InMemoryTCCParticipant("tcc-svc-a")
    p2_new = InMemoryTCCParticipant("tcc-svc-b")
    tm2.register_participant(p1_new)
    tm2.register_participant(p2_new)

    recovery = RecoveryManager(tm2)
    result = await recovery.recover_all()

    logger.info("Recovery result: %s", result)
    logger.info("Transaction status after recovery: %s", tm2.get_status(tx_id).value)

    assert tm2.get_status(tx_id).value in ("CONFIRMED", "CANCELLED")
    logger.info("TCC Recovery successfully resolved. OK")

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
    await demo_tcc_idempotent_retry()
    print()
    await demo_recovery()
    print()
    await demo_recovery_tcc()

    logger.info("=" * 60)
    logger.info("All demos completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
