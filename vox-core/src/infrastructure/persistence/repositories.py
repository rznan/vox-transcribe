from sqlalchemy.orm.strategy_options import selectinload

from src.domain.entities import Batch, TaskAttempt, Task, Worker

from sqlalchemy.ext.asyncio import (
    AsyncSession,
)
from collections.abc import Sequence
from src.domain.value_objects.enums import TaskStatus, WorkerStatus
from src.infrastructure.persistence.models import (
    BatchModel,
    TaskAttemptModel,
    TaskModel,
    WorkerModel,
)

import src.infrastructure.persistence.mappers as mappers

from typing import Callable, Generic, Type, TypeVar, cast

from sqlalchemy import inspect, select
from sqlalchemy.orm import InstanceState

import src.domain.interfaces.repositories as rc

# entidade de domínio
T = TypeVar("T")
# entidade modelo do SqlAlchemy
M = TypeVar("M")
# id da entidade de domínio
ID = TypeVar("ID")


class BaseRepository(rc.BaseRepository[T, ID], Generic[T, M, ID]):

    def __init__(
        self,
        session: AsyncSession,
        model_cls: Type[M],
        to_model: Callable[[T], M],
        to_domain: Callable[[M], T],
    ):
        self.session = session
        self.model_cls = model_cls
        self.to_model = to_model
        self.to_domain = to_domain

    async def add(self, entity: T) -> T:
        model = self.to_model(entity)
        self.session.add(model)
        await self.session.flush()
        return self.to_domain(model)

    async def delete(self, id: ID) -> bool:
        model = self.session.get(self.model_cls, id)

        if model is None:
            return False

        await self.session.delete(model)
        await self.session.flush()

        state = cast(InstanceState, inspect(model))

        return state.deleted

    async def update(self, entity: T) -> T:
        model = self.to_model(entity)
        await self.session.merge(model)
        await self.session.flush()
        return self.to_domain(model)

    async def get_by_id(self, id: ID) -> T | None:
        model = await self.session.get(self.model_cls, id)
        if model == None:
            return None
        return self.to_domain(model)

    async def list_all(self, limit: int = 100, offset: int = 0) -> list[T]:
        stmt = select(self.model_cls).limit(limit).offset(offset)
        models = await self.session.scalars(stmt)
        models = models.all()
        return [self.to_domain(m) for m in models]


class TaskAttemptRepository(BaseRepository[TaskAttempt, TaskAttemptModel, int]):
    def __init__(self, session: AsyncSession):
        super().__init__(
            session,
            TaskAttemptModel,
            mappers.task_attempt_to_model,
            mappers.model_to_task_attempt,
        )


class TaskRepository(BaseRepository[Task, TaskModel, str], rc.TaskRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(
            session, TaskModel, mappers.task_to_model, mappers.model_to_task
        )

    async def get_by_status(
        self, status: TaskStatus, limit: int = 100, offset=0
    ) -> list[Task]:
        """Recupera tarefas filtradas por status."""
        stmt = (
            select(TaskModel)
            .limit(limit)
            .offset(offset)
            .where(TaskModel.status == status)
        )
        models = await self.session.scalars(stmt)
        models = models.all()
        return [self.to_domain(m) for m in models]

    async def get_pending_tasks(self, limit: int = 50, offset=0) -> list[Task]:
        """Recupera tarefas prontas para serem escalonadas/executadas."""
        stmt = (
            select(TaskModel)
            .limit(limit)
            .offset(offset)
            .filter(
                TaskModel.status.in_(
                    [TaskStatus.SUBMITTED, TaskStatus.QUEUED, TaskStatus.REQUEUED]
                )
            )
        )
        models = await self.session.scalars(stmt)
        models = models.all()
        return [self.to_domain(m) for m in models] or []

    async def get_by_batch_id(self, batch_id: str) -> list[Task]:
        """Recupera todas as tarefas associadas a um Lote (Batch)."""
        stmt = select(TaskModel).where(TaskModel.batch_id == batch_id)
        models = (await self.session.scalars(stmt)).all()
        return [self.to_domain(m) for m in models]


class BatchRepository(BaseRepository[Batch, BatchModel, str], rc.BatchRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(
            session, BatchModel, mappers.batch_to_model, mappers.model_to_batch
        )

    async def get_with_tasks(self, batch_id: str) -> Batch | None:
        """Carrega um Batch garantindo que suas Tasks associadas estejam preenchidas."""
        stmt = (
            select(BatchModel)
            .options(selectinload(BatchModel.tasks))
            .where(BatchModel.id == batch_id)
        )
        model = (await self.session.execute(stmt)).scalar_one()

        return mappers.model_to_batch_with_tasks(model)


class WorkerRepository(BaseRepository[Worker, WorkerModel, str], rc.WorkerRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(
            session, WorkerModel, mappers.worker_to_model, mappers.model_to_worker
        )

    async def get_available_workers(self) -> list[Worker]:
        """Retorna trabalhadores em estado de disponibilidade (IDLE)."""
        stmt = select(WorkerModel).where(WorkerModel.status == WorkerStatus.IDLE)
        models = (await self.session.scalars(stmt)).all()
        return [self.to_domain(m) for m in models]

    async def get_stale_workers(self, timeout_seconds: int) -> list[Worker]:
        """Retorna workers que não enviam heartbeat há mais tempo que o timeout."""
        stmt = select(WorkerModel).where(WorkerModel.status == WorkerStatus.OFFLINE)
        models = (await self.session.scalars(stmt)).all()
        return [self.to_domain(m) for m in models]
