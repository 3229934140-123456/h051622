import enum
import json
import os
import time
import threading
from dataclasses import dataclass, field
from typing import Optional


class TxStatus(enum.Enum):
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMITTING = "COMMITTING"
    COMMITTED = "COMMITTED"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"
    TRYING = "TRYING"
    TRIED = "TRIED"
    CONFIRMING = "CONFIRMING"
    CONFIRMED = "CONFIRMED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class TxMode(enum.Enum):
    TWO_PHASE = "TWO_PHASE"
    TCC = "TCC"


class Vote(enum.Enum):
    YES = "YES"
    NO = "NO"
    TIMEOUT = "TIMEOUT"


@dataclass
class ParticipantRecord:
    participant_id: str
    vote: Optional[Vote] = None
    phase_completed: Optional[str] = None


@dataclass
class TransactionLog:
    tx_id: str
    mode: TxMode
    status: TxStatus
    participants: list = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    timeout_seconds: float = 30.0


class TransactionLogger:
    def __init__(self, log_dir: str = "tx_logs"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self._lock = threading.Lock()

    def _log_path(self, tx_id: str) -> str:
        return os.path.join(self.log_dir, f"{tx_id}.json")

    def _serialize(self, log: TransactionLog) -> dict:
        return {
            "tx_id": log.tx_id,
            "mode": log.mode.value,
            "status": log.status.value,
            "participants": [
                {
                    "participant_id": p.participant_id,
                    "vote": p.vote.value if p.vote else None,
                    "phase_completed": p.phase_completed,
                }
                for p in log.participants
            ],
            "created_at": log.created_at,
            "updated_at": log.updated_at,
            "timeout_seconds": log.timeout_seconds,
        }

    def _deserialize(self, data: dict) -> TransactionLog:
        participants = []
        for p in data["participants"]:
            participants.append(
                ParticipantRecord(
                    participant_id=p["participant_id"],
                    vote=Vote(p["vote"]) if p["vote"] else None,
                    phase_completed=p.get("phase_completed"),
                )
            )
        return TransactionLog(
            tx_id=data["tx_id"],
            mode=TxMode(data["mode"]),
            status=TxStatus(data["status"]),
            participants=participants,
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            timeout_seconds=data.get("timeout_seconds", 30.0),
        )

    def append(self, log: TransactionLog) -> None:
        with self._lock:
            log.updated_at = time.time()
            path = self._log_path(log.tx_id)
            tmp_path = path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(self._serialize(log), f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)

    def read(self, tx_id: str) -> Optional[TransactionLog]:
        path = self._log_path(tx_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return self._deserialize(data)

    def delete(self, tx_id: str) -> None:
        with self._lock:
            path = self._log_path(tx_id)
            if os.path.exists(path):
                os.remove(path)

    def list_all(self) -> list:
        result = []
        if not os.path.exists(self.log_dir):
            return result
        for fname in os.listdir(self.log_dir):
            if fname.endswith(".json"):
                tx_id = fname[:-5]
                log = self.read(tx_id)
                if log:
                    result.append(log)
        return result

    def is_terminal(self, status: TxStatus) -> bool:
        return status in (
            TxStatus.COMMITTED,
            TxStatus.ROLLED_BACK,
            TxStatus.CONFIRMED,
            TxStatus.CANCELLED,
        )
