from abc import ABC, abstractmethod
from collections.abc import Sequence

from typing import Generic, List, Sequence, TypeVar, Optional

from src.domain.entities import Batch, Task, Worker, TaskStatus


class TaskService(ABC):
    @abstractmethod
    async def create_batch(self, new_batch: Batch) -> Batch:
        """Registra um batch novo no repositório e o agenda no task_scheduler"""
        pass

    @abstractmethod
    async def delete_batch(self, batch_id: int) -> None:
        """
        Remove os dados do batch com id específicado.
        Cancela qualquer execução em andamento antes de deletar
        """
        pass

    @abstractmethod
    async def get_by_id(self, task_id: int) -> Task | None:
        """
        Retorna a task completa (com tentativas e resultado) solicitada
        """
        pass

    @abstractmethod
    async def list_batches(self, limit: int = 100, offset: int = 0) -> List[Batch]:
        """
        Lista os batches armazenados em memória
        """
        pass

    @abstractmethod
    async def cancel_batch(self, batch_id: int) -> Batch:
        """
        Cancela a execução das tarefas não concluídas de um batch
        """
