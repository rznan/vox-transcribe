from abc import ABC, abstractmethod
from enum import Enum
from typing import List
from uuid import UUID

from src.domain.entities import Batch, Task, Worker


class WorkerService(ABC):

    class ConnectionStatusEnum(str, Enum):
        CONNECTION_STATUS_OK = "CONNECTION_STATUS_OK"
        CONNECTION_STATUS_REJECTED = "CONNECTION_STATUS_REJECTED"


    class HeartbeatResponseEnum(str, Enum):
        HEARTBEAT_RESPONSE_OK = "HEARTBEAT_RESPONSE_OK"
        HEARTBEAT_RESPONSE_TIMEOUT = "HEARTBEAT_RESPONSE_TIMEOUT"
        HEARTBEAT_RESPONSE_NOT_REGISTERED = "HEARTBEAT_RESPONSE_NOT_REGISTERED"

    @abstractmethod
    async def register_worker(self, worker: Worker) -> ConnectionStatusEnum:
        "Registra um worker no sistema"
        pass

    @abstractmethod
    async def compute_heartbeat(self, worker_id: UUID) -> HeartbeatResponseEnum:
        "Computa um hearbeat recebido do worker com id específicado"
        pass

    @abstractmethod
    async def shutdown_worker(self, worker_id: UUID) -> bool:
        "Registra o desligamento o worker"
        pass


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
    async def get_task(self, task_id: int) -> Task | None:
        """
        Retorna a task completa (com tentativas e resultado) solicitada
        """
        pass

    @abstractmethod
    async def get_batch(self, batch_id: int) -> Batch | None:
        """
        Retorna o batch completo (com tasks resumidas) solicitada
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
