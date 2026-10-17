from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from src.application.repository_factory import RepositoryFactory
from src.application.scheduler.load_balancer import (
    LoadBalancer,
    WorkerIdNotRegisteredError,
)
from src.domain.entities import Worker
from src.domain.interfaces.services import WorkerService
from src.domain.value_objects.enums import WorkerStatus


class WorkerServiceImpl(WorkerService):
    """Manages worker registration, availability, and lifecycle."""

    def __init__(
        self,
        repository_factory: RepositoryFactory,
        load_balancer: LoadBalancer,
    ) -> None:
        self.repository_factory = repository_factory
        self.load_balancer = load_balancer

    async def restore_known_workers(self) -> None:
        """Restore persisted workers with a fresh runtime state."""
        offset = 0
        page_size = 100

        while True:
            async with self.repository_factory() as repositories:
                workers = await repositories.workers.list_all(
                    limit=page_size,
                    offset=offset,
                )

            for worker in workers:
                self._reset_runtime_state(worker)
                await self.load_balancer.register(worker)

            if len(workers) < page_size:
                break

            offset += len(workers)

    async def register_worker(
        self,
        worker: Worker,
    ) -> WorkerService.ConnectionStatusEnum:
        """Register a new worker or refresh an existing registration."""
        existing_worker = await self._find_worker(worker.id)
        was_registered = await self.load_balancer.verify_worker(worker.id)

        self._prepare_worker_for_connection(worker)

        async with self.repository_factory() as repositories:
            if existing_worker is None:
                persisted_worker = await repositories.workers.add(worker)
            else:
                persisted_worker = await repositories.workers.update(worker)

        self._prepare_worker_for_connection(persisted_worker)

        if was_registered:
            await self._unregister_from_load_balancer(worker.id)
        await self.load_balancer.register(persisted_worker)

        return self.ConnectionStatusEnum.CONNECTION_STATUS_OK

    async def compute_heartbeat(
        self,
        worker_id: UUID,
    ) -> WorkerService.HeartbeatResponseEnum:
        """Update the heartbeat of a registered worker."""
        worker = await self.load_balancer.get_registered_worker(worker_id)

        if worker is None:
            return self.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_NOT_REGISTERED

        worker.heartbeat = datetime.now(timezone.utc)

        return self.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_OK

    async def shutdown_worker(self, worker_id: UUID) -> bool:
        """Mark a worker as offline and remove it from active tracking."""
        worker = await self._find_worker(worker_id)

        if worker is None:
            return False

        worker.status = WorkerStatus.OFFLINE
        worker.heartbeat = None

        async with self.repository_factory() as repositories:
            await repositories.workers.update(worker)

        if await self.load_balancer.verify_worker(worker_id):
            await self._unregister_from_load_balancer(worker_id)

        return True

    async def _find_worker(self, worker_id: UUID) -> Worker | None:
        """Find a worker in memory or retrieve it from persistence."""
        worker = await self.load_balancer.get_registered_worker(worker_id)

        if worker is not None:
            return worker

        async with self.repository_factory() as repositories:
            return await repositories.workers.get_by_id(worker_id)

    @staticmethod
    def _reset_runtime_state(worker: Worker) -> None:
        """Initialize transient worker state after application startup."""
        worker.status = WorkerStatus.IDLE
        worker.heartbeat = datetime.now(timezone.utc)
        worker.current_workload = 0

    @staticmethod
    def _prepare_worker_for_connection(worker: Worker) -> None:
        """Prepare worker state for a new or refreshed connection."""
        worker.status = WorkerStatus.IDLE
        worker.heartbeat = datetime.now(timezone.utc)

    async def _unregister_from_load_balancer(
        self,
        worker_id: UUID,
    ) -> None:
        """Remove a worker from the load balancer when registered."""
        try:
            await self.load_balancer.unregister(worker_id)
        except WorkerIdNotRegisteredError:
            pass
