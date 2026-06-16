#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dtx import (
    TransactionManager,
    RecoveryManager,
)
from dtx.logger import TxStatus


def format_time(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def print_tx_detail(tm: TransactionManager, tx_id: str) -> int:
    log = tm.get_log(tx_id)
    if not log:
        print(f"ERROR: Transaction '{tx_id}' not found.")
        return 1

    width = 78
    print("=" * width)
    print(f"  TRANSACTION DETAIL: {tx_id}")
    print("=" * width)

    is_terminal = tm.logger.is_terminal(log.status)
    status_color = "[OK] " if is_terminal else "[ ! ]"
    print(f"  Mode:       {log.mode.value}")
    print(f"  Status:     {log.status.value:15s}  {status_color}  {'TERMINAL' if is_terminal else 'NOT CONVERGED'}")
    print(f"  Created:    {format_time(log.created_at)}")
    print(f"  Updated:    {format_time(log.updated_at)}")
    print(f"  Timeout:    {log.timeout_seconds:.1f}s")
    print()

    print("  Participants:")
    print("  " + "-" * (width - 2))
    header = f"  {'ID':<25s} {'VOTE':<10s} {'PHASE_DONE':<15s} STATUS"
    print(header)
    print("  " + "-" * (width - 2))

    for p in log.participants:
        vote = p.vote.value if p.vote else "NOT_VOTED"
        phase = p.phase_completed or "-"
        if is_terminal or p.phase_completed:
            status_mark = "[OK]"
        elif p.vote and not p.phase_completed:
            status_mark = "[ ! ]  Awaiting phase 2"
        else:
            status_mark = "[ ? ]  Not started"
        print(f"  {p.participant_id:<25s} {vote:<10s} {phase:<15s} {status_mark}")

    print()

    recovery = RecoveryManager(tm)
    analysis = recovery.analyze_log(tx_id)
    if analysis and analysis.get("recovery_hint"):
        print("  Recovery hint:")
        print(f"    -> {analysis['recovery_hint']}")
        print()

    print("  Raw JSON:")
    raw = {
        "tx_id": log.tx_id,
        "mode": log.mode.value,
        "status": log.status.value,
        "is_terminal": is_terminal,
        "created_at": format_time(log.created_at),
        "updated_at": format_time(log.updated_at),
        "timeout_seconds": log.timeout_seconds,
        "participants": [
            {
                "participant_id": p.participant_id,
                "vote": p.vote.value if p.vote else None,
                "phase_completed": p.phase_completed,
            }
            for p in log.participants
        ],
        "recovery_hint": analysis.get("recovery_hint") if analysis else None,
    }
    print(json.dumps(raw, indent=2, ensure_ascii=False))

    print("=" * width)
    return 0


def print_unconverged(tm: TransactionManager, verbose: bool) -> int:
    tx_list = tm.list_unconverged()
    width = 95
    print("=" * width)
    print(f"  UNCONVERGED TRANSACTIONS  ({len(tx_list)} found)")
    print("=" * width)

    if not tx_list:
        print("  (none)  All transactions are in terminal state.")
        print("=" * width)
        return 0

    header = (
        f"  {'TX_ID':<18s} {'MODE':<10s} {'STATUS':<14s} "
        f"{'Y':>3s} {'N':>3s} {'?':>3s} {'UPDATED':<20s} HINT"
    )
    print(header)
    print("  " + "-" * (width - 2))

    recovery = RecoveryManager(tm)
    for log in sorted(tx_list, key=lambda t: t.updated_at, reverse=True):
        voted_yes = sum(1 for p in log.participants if p.vote and p.vote.value == "YES")
        voted_no = sum(1 for p in log.participants if p.vote and p.vote.value in ("NO", "TIMEOUT"))
        not_voted = sum(1 for p in log.participants if p.vote is None)
        analysis = recovery.analyze_log(log.tx_id)
        hint = (analysis or {}).get("recovery_hint", "") or ""
        print(
            f"  {log.tx_id:<18s} {log.mode.value:<10s} {log.status.value:<14s} "
            f"{voted_yes:>3d} {voted_no:>3d} {not_voted:>3d} "
            f"{format_time(log.updated_at):<20s} {hint}"
        )

    if verbose:
        print()
        for log in tx_list:
            print_tx_detail(tm, log.tx_id)
            print()

    print("=" * width)
    print(f"  Total: {len(tx_list)} un-converged transaction(s).  Re-run with --recover to fix.")
    print("=" * width)
    return 0


def print_all(tm: TransactionManager) -> int:
    all_logs = tm.logger.list_all()
    width = 90
    print("=" * width)
    print(f"  ALL TRANSACTIONS  ({len(all_logs)} total)")
    print("=" * width)

    if not all_logs:
        print("  (no transaction logs found)")
        print("=" * width)
        return 0

    header = (
        f"  {'TX_ID':<18s} {'MODE':<10s} {'STATUS':<14s} {'TERM':<6s} "
        f"{'Y':>3s} {'N':>3s} {'?':>3s} {'UPDATED':<20s}"
    )
    print(header)
    print("  " + "-" * (width - 2))

    for log in sorted(all_logs, key=lambda t: t.updated_at, reverse=True):
        voted_yes = sum(1 for p in log.participants if p.vote and p.vote.value == "YES")
        voted_no = sum(1 for p in log.participants if p.vote and p.vote.value in ("NO", "TIMEOUT"))
        not_voted = sum(1 for p in log.participants if p.vote is None)
        is_terminal = tm.logger.is_terminal(log.status)
        terminal_mark = "YES" if is_terminal else "NO"
        print(
            f"  {log.tx_id:<18s} {log.mode.value:<10s} {log.status.value:<14s} {terminal_mark:<6s} "
            f"{voted_yes:>3d} {voted_no:>3d} {not_voted:>3d} "
            f"{format_time(log.updated_at):<20s}"
        )

    print("=" * width)
    return 0


async def do_recover(tm: TransactionManager) -> int:
    await tm.start()
    recovery = RecoveryManager(tm)
    result = await recovery.recover_all()
    await tm.stop()

    stuck = [tx for tx, o in result.outcomes.items() if o == "still_failed"]
    return 1 if stuck else 0


def main():
    parser = argparse.ArgumentParser(
        description="Distributed Transaction Coordinator — Inspection & Recovery Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python inspect.py ls                           # list all transactions
  python inspect.py ps                           # list un-converged (stuck) transactions
  python inspect.py ps -v                        # list stuck + show each in detail
  python inspect.py show <tx_id>                 # show detail for one transaction
  python inspect.py recover --log-dir tx_logs    # run recovery for all un-converged tx
  python inspect.py -d my_logs show abc123       # use custom log directory
""",
    )
    parser.add_argument(
        "-d", "--log-dir", default="tx_logs",
        help="directory containing transaction JSON logs (default: tx_logs)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ls", help="list all transactions")

    ps_p = sub.add_parser("ps", help="list un-converged (stuck) transactions")
    ps_p.add_argument("-v", "--verbose", action="store_true", help="also show full detail for each")

    show_p = sub.add_parser("show", help="show detail for a specific transaction")
    show_p.add_argument("tx_id", help="transaction ID to inspect")

    sub.add_parser("recover", help="run recovery on all un-converged transactions")

    args = parser.parse_args()

    tm = TransactionManager(log_dir=args.log_dir)

    if args.command == "ls":
        sys.exit(print_all(tm))
    elif args.command == "ps":
        sys.exit(print_unconverged(tm, args.verbose))
    elif args.command == "show":
        sys.exit(print_tx_detail(tm, args.tx_id))
    elif args.command == "recover":
        rc = asyncio.run(do_recover(tm))
        sys.exit(rc)


if __name__ == "__main__":
    main()
