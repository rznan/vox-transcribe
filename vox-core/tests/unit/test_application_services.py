from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from src.application.scheduler.data_strutures import BatchNotFoundError
from src.application.services import TaskServiceImpl, WorkerServiceImpl
from src.domain.entities import Batch, Task, Worker
from src.domain.interfaces.services import TaskService, WorkerService
from src.domain.value_objects.enums import TaskStatus, WorkerRuntime, WorkerStatus


class FakeRepositoryFactory:
    def __init__(self) -> None:
        self.batches = SimpleNamespace(
            add=AsyncMock(),
            delete=AsyncMock(return_value=True),
            get_with_tasks=AsyncMock(return_value=None),
            list_all=AsyncMock(return_value=[]),
        )
        self.tasks = SimpleNamespace(
            get_by_id=AsyncMock(return_value=None),
            update=AsyncMock(),
        )
        self.workers = SimpleNamespace(
            add=AsyncMock(),
            get_by_id=AsyncMock(return_value=None),
            list_all=AsyncMock(return_value=[]),
            update=AsyncMock(),
        )
        self.repositories = SimpleNamespace(
            batches=self.batches,
            tasks=self.tasks,
            workers=self.workers,
        )

    @asynccontextmanager
    async def __call__(self):
        yield self.repositories


def make_task(
    task_id: int | None = 1,
    status: TaskStatus = TaskStatus.SUBMITTED,
) -> Task:
    return Task(
        _id=task_id,
        _batch_id=7 if task_id is not None else None,
        status=status,
        artifact={},
        filename="audio.wav",
        size=42,
    )


def make_batch(
    batch_id: int | None = 7,
    tasks: list[Task] | None = None,
) -> Batch:
    return Batch(
        _id=batch_id,
        created_at=datetime(2025, 1, 1),
        tasks=tasks if tasks is not None else [make_task()],
    )


def make_worker(worker_id=None) -> Worker:
    return Worker(
        id=worker_id or uuid4(),
        name="worker",
        ip="127.0.0.1",
        port=5000,
        runtime=WorkerRuntime.CPU,
        languages_supported=["pt-br"],
    )


@pytest.mark.asyncio
async def test_create_batch_persists_caches_and_schedules_batch():
    factory = FakeRepositoryFactory()
    scheduler = Mock()
    batch = make_batch(batch_id=None, tasks=[make_task(task_id=None)])
    created = make_batch(batch_id=7, tasks=batch.tasks)
    factory.batches.add.return_value = created
    factory.batches.get_with_tasks.return_value = created
    service = TaskServiceImpl(factory, scheduler)

    result = await service.create_batch(batch)

    assert result is created
    assert result.tasks[0].status is TaskStatus.SUBMITTED
    factory.batches.add.assert_awaited_once_with(batch)
    scheduler.append.assert_called_once_with(created)
    assert await service.get_batch(7) is created


@pytest.mark.asyncio
async def test_create_batch_rejects_empty_or_terminal_tasks():
    factory = FakeRepositoryFactory()
    service = TaskServiceImpl(factory)

    with pytest.raises(ValueError, match="ao menos uma tarefa"):
        await service.create_batch(make_batch(tasks=[]))

    with pytest.raises(ValueError, match="estado terminal"):
        await service.create_batch(
            make_batch(tasks=[make_task(status=TaskStatus.SUCCEEDED)])
        )

    factory.batches.add.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_task_uses_repository_then_cache():
    factory = FakeRepositoryFactory()
    task = make_task()
    factory.tasks.get_by_id.return_value = task
    service = TaskServiceImpl(factory)

    assert await service.get_task(task.id) is task
    assert await service.get_task(task.id) is task
    factory.tasks.get_by_id.assert_awaited_once_with(task.id)


@pytest.mark.asyncio
async def test_get_batch_returns_none_when_missing():
    factory = FakeRepositoryFactory()
    service = TaskServiceImpl(factory)

    assert await service.get_batch(7) is None
    factory.batches.get_with_tasks.assert_awaited_once_with(7)


@pytest.mark.asyncio
async def test_cancel_batch_cancels_and_persists_only_unfinished_tasks():
    factory = FakeRepositoryFactory()
    scheduler = Mock()
    scheduler.remove.side_effect = BatchNotFoundError
    finished = make_task(2, TaskStatus.SUCCEEDED)
    unfinished = make_task(3, TaskStatus.RUNNING)
    batch = make_batch(tasks=[finished, unfinished])
    factory.batches.get_with_tasks.return_value = batch
    service = TaskServiceImpl(factory, scheduler)

    result = await service.cancel_batch(7)

    assert result is batch
    assert finished.status is TaskStatus.SUCCEEDED
    assert unfinished.status is TaskStatus.CANCELLED
    factory.tasks.update.assert_awaited_once_with(unfinished)
    scheduler.remove.assert_called_once_with(7)


@pytest.mark.asyncio
async def test_delete_batch_cancels_then_deletes_and_forgets_cache():
    factory = FakeRepositoryFactory()
    scheduler = Mock()
    batch = make_batch()
    factory.batches.get_with_tasks.return_value = batch
    service = TaskServiceImpl(factory, scheduler)
    service._remember_batch(batch)

    await service.delete_batch(7)

    factory.tasks.update.assert_awaited_once_with(batch.tasks[0])
    factory.batches.delete.assert_awaited_once_with(7)
    scheduler.remove.assert_called_once_with(7)
    assert 7 not in service._batches
    assert batch.tasks[0].id not in service._tasks


@pytest.mark.asyncio
async def test_restore_pending_batches_enqueues_only_unfinished_tasks():
    factory = FakeRepositoryFactory()
    scheduler = Mock()
    batch = make_batch(
        tasks=[
            make_task(1, TaskStatus.SUBMITTED),
            make_task(2, TaskStatus.SUCCEEDED),
        ]
    )
    factory.batches.list_all.side_effect = [[make_batch()], []]
    factory.batches.get_with_tasks.return_value = batch
    service = TaskServiceImpl(factory, scheduler)

    await service.restore_pending_batches()

    scheduled_batch = scheduler.append.call_args.args[0]
    assert scheduled_batch.id == batch.id
    assert scheduled_batch.tasks == [batch.tasks[0]]


@pytest.mark.asyncio
async def test_register_worker_persists_then_registers_with_load_balancer():
    factory = FakeRepositoryFactory()
    balancer = Mock()
    balancer.verify_worker = AsyncMock(return_value=False)
    balancer.get_registered_worker = AsyncMock(return_value=None)
    balancer.register = AsyncMock()
    worker = make_worker()
    factory.workers.add.return_value = worker
    service = WorkerServiceImpl(factory, balancer)

    result = await service.register_worker(worker)

    assert result is WorkerService.ConnectionStatusEnum.CONNECTION_STATUS_OK
    factory.workers.add.assert_awaited_once_with(worker)
    balancer.register.assert_awaited_once_with(worker)
    assert worker.heartbeat is not None


@pytest.mark.asyncio
async def test_reregistered_worker_updates_repository_and_replaces_balancer_entry():
    factory = FakeRepositoryFactory()
    balancer = Mock()
    worker = make_worker()
    balancer.verify_worker = AsyncMock(return_value=True)
    balancer.get_registered_worker = AsyncMock(return_value=worker)
    balancer.unregister = AsyncMock(return_value=worker)
    balancer.register = AsyncMock()
    factory.workers.get_by_id.return_value = worker
    factory.workers.update.return_value = worker
    service = WorkerServiceImpl(factory, balancer)

    await service.register_worker(worker)

    factory.workers.update.assert_awaited_once_with(worker)
    balancer.unregister.assert_awaited_once_with(worker.id)
    balancer.register.assert_awaited_once_with(worker)


@pytest.mark.asyncio
async def test_heartbeat_updates_registered_worker_and_rejects_unknown_worker():
    factory = FakeRepositoryFactory()
    worker = make_worker()
    balancer = Mock()
    balancer.get_registered_worker = AsyncMock(return_value=None)
    service = WorkerServiceImpl(factory, balancer)

    assert (
        await service.compute_heartbeat(worker.id)
        is WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_NOT_REGISTERED
    )

    balancer.get_registered_worker.return_value = worker
    assert (
        await service.compute_heartbeat(worker.id)
        is WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_OK
    )
    assert worker.heartbeat is not None


@pytest.mark.asyncio
async def test_shutdown_worker_updates_repository_and_unregisters_worker():
    factory = FakeRepositoryFactory()
    balancer = Mock()
    worker = make_worker()
    balancer.get_registered_worker = AsyncMock(return_value=worker)
    balancer.verify_worker = AsyncMock(return_value=True)
    balancer.unregister = AsyncMock(return_value=worker)
    service = WorkerServiceImpl(factory, balancer)

    assert await service.shutdown_worker(worker.id) is True

    assert worker.status is WorkerStatus.OFFLINE
    assert worker.heartbeat is None
    factory.workers.update.assert_awaited_once_with(worker)
    balancer.unregister.assert_awaited_once_with(worker.id)


@pytest.mark.asyncio
async def test_shutdown_unknown_worker_returns_false():
    factory = FakeRepositoryFactory()
    balancer = Mock()
    balancer.get_registered_worker = AsyncMock(return_value=None)
    service = WorkerServiceImpl(factory, balancer)

    assert await service.shutdown_worker(uuid4()) is False
    factory.workers.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_restore_known_workers_starts_fresh_utc_heartbeat():
    factory = FakeRepositoryFactory()
    balancer = Mock()
    balancer.register = AsyncMock()
    worker = make_worker()
    worker.heartbeat = None
    factory.workers.list_all.return_value = [worker]
    service = WorkerServiceImpl(factory, balancer)

    await service.restore_known_workers()

    balancer.register.assert_awaited_once_with(worker)
    assert worker.heartbeat is not None
    assert worker.heartbeat.tzinfo is not None
    assert worker.current_workload == 0
