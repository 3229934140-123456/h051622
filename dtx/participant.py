from abc import ABC, abstractmethod
from typing import Optional
from .logger import Vote


class Participant(ABC):
    @property
    @abstractmethod
    def participant_id(self) -> str:
        ...

    @abstractmethod
    async def prepare(self, tx_id: str, context: dict) -> Vote:
        ...

    @abstractmethod
    async def commit(self, tx_id: str, context: dict) -> None:
        ...

    @abstractmethod
    async def rollback(self, tx_id: str, context: dict) -> None:
        ...


class TCCParticipant(Participant):
    @abstractmethod
    async def try_phase(self, tx_id: str, context: dict) -> Vote:
        ...

    @abstractmethod
    async def confirm(self, tx_id: str, context: dict) -> None:
        ...

    @abstractmethod
    async def cancel(self, tx_id: str, context: dict) -> None:
        ...

    async def prepare(self, tx_id: str, context: dict) -> Vote:
        return await self.try_phase(tx_id, context)

    async def commit(self, tx_id: str, context: dict) -> None:
        await self.confirm(tx_id, context)

    async def rollback(self, tx_id: str, context: dict) -> None:
        await self.cancel(tx_id, context)


class InMemory2PCParticipant(Participant):
    def __init__(self, pid: str, *, always_vote_yes: bool = True, delay: float = 0.0):
        self._id = pid
        self._always_vote_yes = always_vote_yes
        self._delay = delay
        self._prepared: dict = {}
        self._committed: dict = {}
        self._rolled_back: dict = {}
        self._commit_call_count: dict = {}
        self._rollback_call_count: dict = {}

    @property
    def participant_id(self) -> str:
        return self._id

    async def prepare(self, tx_id: str, context: dict) -> Vote:
        import asyncio
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._always_vote_yes:
            self._prepared[tx_id] = context
            return Vote.YES
        self._rolled_back[tx_id] = context
        return Vote.NO

    async def commit(self, tx_id: str, context: dict) -> None:
        self._commit_call_count[tx_id] = self._commit_call_count.get(tx_id, 0) + 1
        if tx_id in self._committed:
            return
        self._committed[tx_id] = context
        self._prepared.pop(tx_id, None)

    async def rollback(self, tx_id: str, context: dict) -> None:
        self._rollback_call_count[tx_id] = self._rollback_call_count.get(tx_id, 0) + 1
        if tx_id in self._rolled_back and tx_id not in self._prepared:
            return
        self._rolled_back[tx_id] = context
        self._prepared.pop(tx_id, None)

    def is_prepared(self, tx_id: str) -> bool:
        return tx_id in self._prepared

    def is_committed(self, tx_id: str) -> bool:
        return tx_id in self._committed

    def is_rolled_back(self, tx_id: str) -> bool:
        return tx_id in self._rolled_back

    def commit_count(self, tx_id: str) -> int:
        return self._commit_call_count.get(tx_id, 0)

    def rollback_count(self, tx_id: str) -> int:
        return self._rollback_call_count.get(tx_id, 0)


class Failing2PCParticipant(InMemory2PCParticipant):
    def __init__(self, pid: str, *, fail_on_commit: bool = False, fail_on_rollback: bool = False, fail_count: int = 1):
        super().__init__(pid)
        self._fail_on_commit = fail_on_commit
        self._fail_on_rollback = fail_on_rollback
        self._fail_count = fail_count
        self._attempt_count: dict = {}

    async def commit(self, tx_id: str, context: dict) -> None:
        self._attempt_count.setdefault(tx_id, 0)
        self._attempt_count[tx_id] += 1
        if self._fail_on_commit and self._attempt_count[tx_id] <= self._fail_count:
            raise RuntimeError(f"Simulated commit failure for tx={tx_id}")
        await super().commit(tx_id, context)

    async def rollback(self, tx_id: str, context: dict) -> None:
        self._attempt_count.setdefault(tx_id, 0)
        self._attempt_count[tx_id] += 1
        if self._fail_on_rollback and self._attempt_count[tx_id] <= self._fail_count:
            raise RuntimeError(f"Simulated rollback failure for tx={tx_id}")
        await super().rollback(tx_id, context)


class InMemoryTCCParticipant(TCCParticipant):
    def __init__(self, pid: str, *, always_try_yes: bool = True, delay: float = 0.0):
        self._id = pid
        self._always_try_yes = always_try_yes
        self._delay = delay
        self._tried: dict = {}
        self._confirmed: dict = {}
        self._cancelled: dict = {}
        self._confirm_call_count: dict = {}
        self._cancel_call_count: dict = {}

    @property
    def participant_id(self) -> str:
        return self._id

    async def try_phase(self, tx_id: str, context: dict) -> Vote:
        import asyncio
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._always_try_yes:
            self._tried[tx_id] = context
            return Vote.YES
        self._cancelled[tx_id] = context
        return Vote.NO

    async def confirm(self, tx_id: str, context: dict) -> None:
        self._confirm_call_count[tx_id] = self._confirm_call_count.get(tx_id, 0) + 1
        if tx_id in self._confirmed:
            return
        self._confirmed[tx_id] = context
        self._tried.pop(tx_id, None)

    async def cancel(self, tx_id: str, context: dict) -> None:
        self._cancel_call_count[tx_id] = self._cancel_call_count.get(tx_id, 0) + 1
        if tx_id in self._cancelled and tx_id not in self._tried:
            return
        self._cancelled[tx_id] = context
        self._tried.pop(tx_id, None)

    def is_tried(self, tx_id: str) -> bool:
        return tx_id in self._tried

    def is_confirmed(self, tx_id: str) -> bool:
        return tx_id in self._confirmed

    def is_cancelled(self, tx_id: str) -> bool:
        return tx_id in self._cancelled

    def confirm_count(self, tx_id: str) -> int:
        return self._confirm_call_count.get(tx_id, 0)

    def cancel_count(self, tx_id: str) -> int:
        return self._cancel_call_count.get(tx_id, 0)


class FailingTCCParticipant(InMemoryTCCParticipant):
    def __init__(self, pid: str, *, fail_on_confirm: bool = False, fail_on_cancel: bool = False, fail_count: int = 1):
        super().__init__(pid)
        self._fail_on_confirm = fail_on_confirm
        self._fail_on_cancel = fail_on_cancel
        self._fail_count = fail_count
        self._attempt_count: dict = {}

    async def confirm(self, tx_id: str, context: dict) -> None:
        self._attempt_count.setdefault(tx_id, 0)
        self._attempt_count[tx_id] += 1
        if self._fail_on_confirm and self._attempt_count[tx_id] <= self._fail_count:
            raise RuntimeError(f"Simulated confirm failure for tx={tx_id}")
        await super().confirm(tx_id, context)

    async def cancel(self, tx_id: str, context: dict) -> None:
        self._attempt_count.setdefault(tx_id, 0)
        self._attempt_count[tx_id] += 1
        if self._fail_on_cancel and self._attempt_count[tx_id] <= self._fail_count:
            raise RuntimeError(f"Simulated cancel failure for tx={tx_id}")
        await super().cancel(tx_id, context)
