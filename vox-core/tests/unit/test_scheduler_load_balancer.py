import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from src.application.scheduler.load_balancer import (
    RoundRobinLoadBalancer,
    WorkerIdNotRegisteredError,
)
from src.domain.entities import Worker
from src.domain.value_objects.enums import WorkerRuntime, WorkerStatus


@pytest.fixture
def sample_worker() -> Worker:
    return Worker(
        id=uuid4(),
        name="Worker Alpha",
        ip="127.0.0.1",
        port=8080,
        runtime=WorkerRuntime.GPU,
        simultaneous_capacity=2,
        status=WorkerStatus.IDLE,
        languages_supported=["en", "pt"],
    )


@pytest.fixture
def sample_worker_list(sample_worker: Worker) -> list[Worker]:
    return [
        sample_worker,
        Worker(
            id=uuid4(),
            name="Worker Beta",
            ip="127.0.0.1",
            port=8081,
            runtime=WorkerRuntime.CPU,
            simultaneous_capacity=2,
            languages_supported=["en"],
        ),
    ]


@pytest.fixture
def balancer() -> RoundRobinLoadBalancer:
    return RoundRobinLoadBalancer()


@pytest.mark.asyncio
async def test_register_worker_and_duplicate_are_idempotent(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
) -> None:
    await balancer.register(sample_worker)
    await balancer.register(sample_worker)

    assert balancer.workers[sample_worker.id] is sample_worker
    assert list(balancer.language_deques["en"]).count(sample_worker.id) == 1
    assert await balancer.verify_worker(sample_worker.id)
    assert await balancer.get_registered_worker(sample_worker.id) is sample_worker


@pytest.mark.asyncio
async def test_unregister_worker_at_head_removes_it_immediately(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
) -> None:
    await balancer.register(sample_worker)

    assert await balancer.unregister(sample_worker.id) is sample_worker
    assert not await balancer.verify_worker(sample_worker.id)
    assert not balancer.language_deques["en"]


@pytest.mark.asyncio
async def test_unregister_worker_not_at_head_uses_lazy_removal(
    balancer: RoundRobinLoadBalancer,
    sample_worker_list: list[Worker],
) -> None:
    first, second = sample_worker_list
    await balancer.register(first)
    await balancer.register(second)

    await balancer.unregister(second.id)

    assert second.id not in balancer.workers
    assert second.id in balancer.to_be_removed_worker_id_sets["en"]
    assert await balancer.get_worker("en") is first
    assert await balancer.get_worker("en") is first
    assert second.id not in balancer.language_deques["en"]
    assert second.id not in balancer.to_be_removed_worker_id_sets["en"]


@pytest.mark.asyncio
async def test_unregister_missing_worker_raises(
    balancer: RoundRobinLoadBalancer,
) -> None:
    with pytest.raises(WorkerIdNotRegisteredError):
        await balancer.unregister(uuid4())


@pytest.mark.asyncio
async def test_reregister_before_lazy_cleanup_does_not_duplicate_queue_entry(
    balancer: RoundRobinLoadBalancer,
    sample_worker_list: list[Worker],
) -> None:
    first, second = sample_worker_list
    await balancer.register(first)
    await balancer.register(second)
    await balancer.unregister(second.id)

    await balancer.register(second)

    assert list(balancer.language_deques["en"]).count(second.id) == 1
    assert second.id not in balancer.to_be_removed_worker_id_sets["en"]


@pytest.mark.asyncio
async def test_register_refresh_removes_worker_from_languages_no_longer_supported(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
) -> None:
    await balancer.register(sample_worker)
    refreshed_worker = Worker(
        id=sample_worker.id,
        name=sample_worker.name,
        ip=sample_worker.ip,
        port=sample_worker.port,
        runtime=sample_worker.runtime,
        languages_supported=["de"],
    )

    await balancer.register(refreshed_worker)

    assert sample_worker.id not in balancer.language_deques["en"]
    assert sample_worker.id not in balancer.language_deques["pt"]
    assert list(balancer.language_deques["de"]).count(sample_worker.id) == 1


@pytest.mark.asyncio
async def test_get_worker_round_robins_available_workers(
    balancer: RoundRobinLoadBalancer,
    sample_worker_list: list[Worker],
) -> None:
    first, second = sample_worker_list
    await balancer.register(first)
    await balancer.register(second)

    assert await balancer.get_worker("en") is first
    assert await balancer.get_worker("en") is second


@pytest.mark.asyncio
async def test_get_worker_skips_full_worker_and_waits_until_available(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
) -> None:
    await balancer.register(sample_worker)
    sample_worker.current_workload = sample_worker.simultaneous_capacity

    pending = asyncio.create_task(balancer.get_worker("en"))
    await asyncio.sleep(0)
    assert sample_worker.id in balancer.full_worker_id_set
    assert not pending.done()

    sample_worker.current_workload -= 1
    await balancer.mark_worker_as_available(sample_worker.id)
    assert await asyncio.wait_for(pending, timeout=0.2) is sample_worker


@pytest.mark.asyncio
async def test_get_worker_waiter_wakes_after_registration(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
) -> None:
    pending = asyncio.create_task(balancer.get_worker("en"))
    await asyncio.sleep(0)

    await balancer.register(sample_worker)

    assert await asyncio.wait_for(pending, timeout=0.2) is sample_worker


@pytest.mark.asyncio
async def test_mark_available_rejects_unknown_worker(
    balancer: RoundRobinLoadBalancer,
) -> None:
    with pytest.raises(WorkerIdNotRegisteredError):
        await balancer.mark_worker_as_available(uuid4())


@pytest.mark.asyncio
async def test_verify_worker_includes_workers_at_capacity(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
) -> None:
    await balancer.register(sample_worker)
    sample_worker.current_workload = sample_worker.simultaneous_capacity

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(balancer.get_worker("en"), timeout=0.01)
    assert sample_worker.id in balancer.full_worker_id_set
    assert await balancer.verify_worker(sample_worker.id)


@pytest.mark.asyncio
async def test_concurrent_coroutine_operations_preserve_registry_and_queues() -> None:
    balancer = RoundRobinLoadBalancer()
    workers = [
        Worker(
            id=uuid4(),
            name=f"worker-{index}",
            ip="127.0.0.1",
            port=8000 + index,
            runtime=WorkerRuntime.CPU,
            languages_supported=["en"],
        )
        for index in range(40)
    ]

    await asyncio.gather(*(balancer.register(worker) for worker in workers))
    await asyncio.gather(*(balancer.unregister(worker.id) for worker in workers[::2]))

    remaining_ids = {worker.id for worker in workers[1::2]}
    removed_ids = {worker.id for worker in workers[::2]}
    assert set(balancer.workers) == remaining_ids
    assert set(balancer.language_deques["en"]) - removed_ids == remaining_ids
    assert balancer.to_be_removed_worker_id_sets["en"] == (
        set(balancer.language_deques["en"]) & removed_ids
    )


@pytest.mark.asyncio
async def test_monitor_expires_worker_using_utc_heartbeat() -> None:
    balancer = RoundRobinLoadBalancer(
        heartbeat_interval=0.005,
        heartbeat_timeout=0.01,
    )
    worker = Worker(
        id=uuid4(),
        name="stale worker",
        ip="127.0.0.1",
        port=8080,
        runtime=WorkerRuntime.CPU,
        heartbeat=datetime.now(timezone.utc) - timedelta(seconds=1),
        languages_supported=["en"],
    )
    await balancer.register(worker)

    await balancer.start()
    try:

        async def wait_until_removed() -> None:
            while await balancer.verify_worker(worker.id):
                await asyncio.sleep(0.001)

        await asyncio.wait_for(wait_until_removed(), timeout=0.2)
    finally:
        await balancer.stop()

    assert not await balancer.verify_worker(worker.id)


@pytest.mark.asyncio
async def test_start_and_stop_are_idempotent_and_restartable(
    balancer: RoundRobinLoadBalancer,
) -> None:
    await balancer.start()
    first_monitor = balancer._monitor_task
    await balancer.start()

    assert balancer._monitor_task is first_monitor
    assert first_monitor is not None and not first_monitor.done()

    await balancer.stop()
    assert first_monitor.done()
    assert balancer._monitor_task is None

    await balancer.stop()
    await balancer.start()
    second_monitor = balancer._monitor_task
    assert second_monitor is not None and second_monitor is not first_monitor
    await balancer.stop()


@pytest.mark.asyncio
async def test_concurrent_start_and_stop_leave_no_monitor_running(
    balancer: RoundRobinLoadBalancer,
) -> None:
    await asyncio.gather(balancer.start(), balancer.start())
    monitor = balancer._monitor_task
    assert monitor is not None and not monitor.done()

    await asyncio.gather(balancer.stop(), balancer.stop())

    assert balancer._monitor_task is None
    assert monitor.done()
