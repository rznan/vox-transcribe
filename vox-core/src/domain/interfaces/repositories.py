from abc import ABC, abstractmethod
from collections.abc import Sequence

from typing import Generic, Sequence, TypeVar, Optional

from src.domain.entities import Batch, Task, Worker, TaskStatus

# entidade de domínio
T = TypeVar("T")
# id da entidade de domínio
ID = TypeVar("ID")


class BaseRepository(ABC, Generic[T, ID]):

    @abstractmethod
    async def add(self, entity: T) -> T:
        """Adiona uma nova entidade na persistencia"""
        pass

    @abstractmethod
    async def delete(self, id: ID) -> bool:
        """Remove uma entidade pelo ID"""
        pass

    @abstractmethod
    async def update(self, entity: T) -> T:
        """Atualiza o estado da entidade existente"""
        pass

    @abstractmethod
    async def get_by_id(self, id: ID) -> T | None:
        """Busca uma entidade pelo ID"""
        pass

    @abstractmethod
    async def list_all(self, limit: int = 100, offset: int = 0) -> Sequence[T]:
        """Lista as entidades com filtro básico"""
        pass


class TaskRepository(BaseRepository[Task, int]):
    """Contrato de persistência especializado para Tasks."""

    @abstractmethod
    async def get_by_status(
        self, status: TaskStatus, limit: int = 100, offset=0
    ) -> Sequence[Task]:
        """Recupera tarefas filtradas por status."""
        pass

    @abstractmethod
    async def get_pending_tasks(self, limit: int = 50, offset=0) -> Sequence[Task]:
        """Recupera tarefas prontas para serem escalonadas/executadas."""
        pass

    @abstractmethod
    async def get_by_batch_id(self, batch_id: int) -> Sequence[Task]:
        """Recupera todas as tarefas associadas a um Lote (Batch)."""
        pass


class WorkerRepository(BaseRepository[Worker, str]):
    """Contrato de persistência especializado para Workers."""

    pass


class BatchRepository(BaseRepository[Batch, int]):
    """Contrato de persistência especializado para Batches."""

    @abstractmethod
    async def get_with_tasks(self, batch_id: int) -> Optional[Batch]:
        """Carrega um Batch garantindo que suas Tasks associadas estejam preenchidas."""
        pass
