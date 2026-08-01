from datetime import datetime, timezone
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from src.infrastructure.persistence.models import Base
from src.domain.entities import Task, Batch, Worker, TaskStatus, WorkerStatus
from src.infrastructure.persistence.repositories import (
    TaskRepository,
    BatchRepository,
    WorkerRepository,
)


@pytest_asyncio.fixture
async def async_session():
    """Cria uma engine e sessão de teste isolada em memória usando SQLite."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_add_and_get_task(async_session: AsyncSession):
    batch_repo = BatchRepository(async_session)
    task_repo = TaskRepository(async_session)

    # 1. Cria Lote Relacionado
    batch = Batch(id="batch-01", created_at=datetime.now(timezone.utc))
    await batch_repo.add(batch)

    # 2. Cria Entidade Task
    task = Task(
        id="task-100",
        batch_id="batch-01",
        status=TaskStatus.SUBMITTED,
        artifact={"cmd": "echo hello"},
        filename="input.txt",
        size=512,
    )

    # 3. Executa a Inserção via Repositório
    saved_task = await task_repo.add(task)
    assert saved_task.id == "task-100"

    # 4. Busca a Task pelo ID
    retrieved_task = await task_repo.get_by_id("task-100")
    assert retrieved_task is not None
    assert retrieved_task.id == "task-100"
    assert retrieved_task.status == TaskStatus.SUBMITTED
    assert retrieved_task.artifact == {"cmd": "echo hello"}


@pytest.mark.asyncio
async def test_get_tasks_by_status(async_session: AsyncSession):
    batch_repo = BatchRepository(async_session)
    task_repo = TaskRepository(async_session)

    await batch_repo.add(Batch(id="batch-02", created_at=datetime.now(timezone.utc)))

    t1 = Task(
        id="task-1",
        batch_id="batch-02",
        status=TaskStatus.SUBMITTED,
        artifact={},
        filename="a1.wav",
        size=200,
    )
    t2 = Task(
        id="task-2",
        batch_id="batch-02",
        status=TaskStatus.RUNNING,
        artifact={},
        filename="a2.wav",
        size=201,
    )
    t3 = Task(
        id="task-3",
        batch_id="batch-02",
        status=TaskStatus.SUBMITTED,
        artifact={},
        filename="a3.wav",
        size=202,
    )

    await task_repo.add(t1)
    await task_repo.add(t2)
    await task_repo.add(t3)

    pending_tasks = await task_repo.get_by_status(TaskStatus.SUBMITTED)
    assert len(pending_tasks) == 2
    assert {t.id for t in pending_tasks} == {"task-1", "task-3"}


@pytest.mark.asyncio
async def test_worker_repository(async_session: AsyncSession):
    worker_repo = WorkerRepository(async_session)

    w1 = Worker(
        id="w-1",
        name="Worker Alpha",
        ip="127.0.0.1",
        port=8080,
        runtime="python3.11",
        status=WorkerStatus.IDLE,
    )
    w2 = Worker(
        id="w-2",
        name="Worker Beta",
        ip="127.0.0.2",
        port=8080,
        runtime="python3.11",
        status=WorkerStatus.BUSY,
    )

    await worker_repo.add(w1)
    await worker_repo.add(w2)

    available = await worker_repo.get_available_workers()
    assert len(available) == 1
    assert available[0].id == "w-1"
