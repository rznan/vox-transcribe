from sqlalchemy.orm import Session

from src.domain.entities import Batch, TaskAttempt, Task, Worker
from src.infrastructure.persistence.models import (
    BatchModel,
    TaskAttemptModel,
    TaskModel,
    WorkerModel,
)

import src.infrastructure.persistence.mappers as mappers

from collections.abc import Sequence
from typing import Callable, Generic, Type, TypeVar, cast

from sqlalchemy import inspect, select
from sqlalchemy.orm import InstanceState, Session

from src.domain.persistence.repository import IRepository

# entidade de domínio
T = TypeVar("T")
# entidade modelo do SqlAlchemy
M = TypeVar("M")
# id da entidade de domínio
ID = TypeVar("ID")


class BaseRepository(IRepository[T, ID], Generic[T, M, ID]):

    def __init__(
        self,
        session: Session,
        model_cls: Type[M],
        to_model: Callable[[T], M],
        to_domain: Callable[[M], T],
    ):
        self.session = session
        self.model_cls = model_cls
        self.to_model = to_model
        self.to_domain = to_domain

    def add(self, entity: T) -> T:
        model = self.to_model(entity)
        self.session.add(model)
        self.session.flush()
        return self.to_domain(model)

    def delete(self, id: ID) -> bool:
        model = self.session.get(self.model_cls, id)

        if model is None:
            return False

        self.session.delete(model)
        self.session.flush()

        state = cast(InstanceState, inspect(model))

        return state.deleted

    def update(self, entity: T) -> T:
        model = self.to_model(entity)
        self.session.merge(model)
        self.session.flush()
        return self.to_domain(model)

    def get_by_id(self, id: ID) -> T | None:
        model = self.session.get(self.model_cls, id)
        if model == None:
            return None
        return self.to_domain(model)

    def list_all(self, limit: int = 100, offset: int = 0) -> Sequence[T]:
        stmt = select(self.model_cls).limit(limit).offset(offset)
        models = self.session.scalars(stmt).all()
        return [self.to_domain(m) for m in models]


class TaskAttemptRepository(BaseRepository[TaskAttempt, TaskAttemptModel, int]):
    def __init__(self, session: Session):
        super().__init__(
            session,
            TaskAttemptModel,
            mappers.task_attempt_to_model,
            mappers.model_to_task_attempt,
        )


class TaskRepository(BaseRepository[Task, TaskModel, str]):
    def __init__(self, session: Session):
        super().__init__(
            session, TaskModel, mappers.task_to_model, mappers.model_to_task
        )


class BatchRepository(BaseRepository[Batch, BatchModel, str]):
    def __init__(self, session: Session):
        super().__init__(
            session, BatchModel, mappers.batch_to_model, mappers.model_to_batch
        )


class WorkerRepository(BaseRepository[Worker, WorkerModel, str]):
    def __init__(self, session: Session):
        super().__init__(
            session, WorkerModel, mappers.worker_to_model, mappers.model_to_worker
        )
