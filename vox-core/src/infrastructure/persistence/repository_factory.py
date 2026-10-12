from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from src.application.repository_factory import RepositorySet
from src.infrastructure.persistence.database import Database, db
from src.infrastructure.persistence.repositories import (
    BatchRepository,
    TaskRepository,
    WorkerRepository,
)


class SQLAlchemyRepositoryFactory:
    """Creates a transaction-scoped repository set using ``Database.session``."""

    def __init__(self, database: Database | None = None) -> None:
        self.database = database or db

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[RepositorySet]:
        async with self.database.session() as session:
            yield RepositorySet(
                batches=BatchRepository(session),
                tasks=TaskRepository(session),
                workers=WorkerRepository(session),
            )
