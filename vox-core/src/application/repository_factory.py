from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncContextManager, Protocol

from src.domain.interfaces.repositories import BatchRepository, TaskRepository, WorkerRepository


@dataclass(slots=True)
class RepositorySet:
    """Repositories sharing one transaction/session for an application operation."""

    batches: BatchRepository
    tasks: TaskRepository
    workers: WorkerRepository


class RepositoryFactory(Protocol):
    def __call__(self) -> AsyncContextManager[RepositorySet]: ...
