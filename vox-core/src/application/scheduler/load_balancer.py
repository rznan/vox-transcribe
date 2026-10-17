from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from uuid import UUID

from src.domain.entities import Worker


class WorkerIdNotRegisteredError(LookupError):
    """Raised when an operation requires a worker absent from the registry."""


class LoadBalancer(ABC):
    @abstractmethod
    async def start(self) -> None:
        """Start periodic heartbeat monitoring."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop monitoring and await its task shutdown."""

    @abstractmethod
    async def register(self, worker: Worker) -> None:
        """Register or refresh a worker in the active registry."""

    @abstractmethod
    async def unregister(self, worker_id: UUID) -> Worker:
        """Remove an active worker and return its current entity."""

    @abstractmethod
    async def get_worker(self, language: str) -> Worker:
        """Wait for and return an available worker supporting a language."""

    @abstractmethod
    async def get_registered_worker(self, worker_id: UUID) -> Worker | None:
        """Return the active worker by ID, including workers currently at capacity."""

    @abstractmethod
    async def mark_worker_as_available(self, worker_id: UUID) -> None:
        """Make a worker available after its workload decreases."""

    @abstractmethod
    async def verify_worker(self, worker_id: UUID) -> bool:
        """Return whether a worker is present in the active registry."""


class RoundRobinLoadBalancer(LoadBalancer):
    def __init__(
        self,
        heartbeat_interval: float = 10,
        heartbeat_timeout: float = 10,
    ) -> None:
        if heartbeat_interval <= 0 or heartbeat_timeout <= 0:
            raise ValueError("Heartbeat interval and timeout must be positive.")

        self.workers: dict[UUID, Worker] = {}
        self.language_deques: dict[str, deque[UUID]] = defaultdict(deque)
        self.to_be_removed_worker_id_sets: dict[str, set[UUID]] = defaultdict(set)
        self.full_worker_id_set: set[UUID] = set()
        self._worker_available_events: dict[str, asyncio.Event] = defaultdict(
            asyncio.Event
        )

        self._lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = timedelta(seconds=heartbeat_timeout)
        self._monitor_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start one monitor task; repeated calls are idempotent."""
        async with self._lifecycle_lock:
            if self._monitor_task is not None and not self._monitor_task.done():
                return
            self._monitor_task = asyncio.create_task(
                self._heartbeat_monitor(),
                name="worker-heartbeat-monitor",
            )

    async def stop(self) -> None:
        """Cancel and await the monitor so no background task is left behind."""
        async with self._lifecycle_lock:
            monitor_task = self._monitor_task
            self._monitor_task = None
            if monitor_task is None:
                return
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_monitor(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            now = datetime.now(timezone.utc)
            async with self._lock:
                expired_ids = [
                    worker_id
                    for worker_id, worker in self.workers.items()
                    if self._check_timeout(now, worker, self._heartbeat_timeout)
                ]
                for worker_id in expired_ids:
                    self._unregister(worker_id)

    async def register(self, worker: Worker) -> None:
        async with self._lock:
            previous_worker = self.workers.get(worker.id)
            if previous_worker is not None:
                for old_language in previous_worker.languages_supported:
                    if old_language in worker.languages_supported:
                        continue
                    old_queue = self.language_deques[old_language]
                    try:
                        old_queue.remove(worker.id)
                    except ValueError:
                        pass
                    self.to_be_removed_worker_id_sets[old_language].discard(worker.id)

            self.workers[worker.id] = worker
            self.full_worker_id_set.discard(worker.id)

            for language in worker.languages_supported:
                queue = self.language_deques[language]
                # A lazy-removal tombstone can still have this ID in the deque.
                # Reuse that slot instead of appending a duplicate.
                if worker.id not in queue:
                    queue.append(worker.id)
                self.to_be_removed_worker_id_sets[language].discard(worker.id)

                if worker.is_available():
                    self._worker_available_events[language].set()

    async def unregister(self, worker_id: UUID) -> Worker:
        async with self._lock:
            return self._unregister(worker_id)

    async def get_registered_worker(self, worker_id: UUID) -> Worker | None:
        async with self._lock:
            return self.workers.get(worker_id)

    async def get_worker(self, language: str) -> Worker:
        while True:
            async with self._lock:
                language_queue = self.language_deques[language]

                while language_queue:
                    worker_id = language_queue[0]
                    if worker_id in self.to_be_removed_worker_id_sets[language]:
                        language_queue.popleft()
                        self.to_be_removed_worker_id_sets[language].discard(worker_id)
                        continue

                    worker = self.workers.get(worker_id)
                    if worker is None:
                        language_queue.popleft()
                        continue
                    if not worker.is_available():
                        language_queue.popleft()
                        self.full_worker_id_set.add(worker_id)
                        continue

                    language_queue.rotate(-1)
                    return worker

                event = self._worker_available_events[language]
                event.clear()

            await event.wait()

    async def mark_worker_as_available(self, worker_id: UUID) -> None:
        async with self._lock:
            worker = self.workers.get(worker_id)
            if worker is None:
                raise WorkerIdNotRegisteredError(
                    f"The worker {worker_id} is not registered."
                )

            if not worker.is_available():
                return

            self.full_worker_id_set.discard(worker_id)
            for language in worker.languages_supported:
                queue = self.language_deques[language]
                if worker_id not in queue:
                    queue.append(worker_id)
                self.to_be_removed_worker_id_sets[language].discard(worker_id)
                self._worker_available_events[language].set()

    async def verify_worker(self, worker_id: UUID) -> bool:
        async with self._lock:
            return worker_id in self.workers

    def _unregister(self, worker_id: UUID) -> Worker:
        worker = self.workers.pop(worker_id, None)
        if worker is None:
            raise WorkerIdNotRegisteredError(
                f"The worker {worker_id} is not registered."
            )

        self.full_worker_id_set.discard(worker_id)
        for language in worker.languages_supported:
            queue = self.language_deques[language]
            if queue and queue[0] == worker_id:
                queue.popleft()
            elif worker_id in queue:
                self.to_be_removed_worker_id_sets[language].add(worker_id)
        return worker

    @staticmethod
    def _check_timeout(
        now: datetime,
        worker: Worker,
        timeout: timedelta = timedelta(seconds=10),
    ) -> bool:
        heartbeat = worker.heartbeat
        if heartbeat is None:
            return False
        if heartbeat.tzinfo is None:
            heartbeat = heartbeat.replace(tzinfo=timezone.utc)
        return now - heartbeat >= timeout
