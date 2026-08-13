from abc import ABC, abstractmethod
from collections import deque
from uuid import UUID

from src.domain.entities import Worker


class WorkerIdNotRegisteredError(Exception):
    """Lançada quando tenta-se remover/acessar um worker não registrado no balanceador de carga"""

    pass


class LoadBalancer(ABC):

    @abstractmethod
    def register(self, worker: Worker) -> None:
        """Registra um novo worker no balanceador de carga"""
        pass

    @abstractmethod
    def unregister(self, worker_id: UUID) -> None:
        """Remove um worker do balanceador de carga"""
        pass

    @abstractmethod
    def get_worker(self) -> Worker | None:
        """Retorna um worker disponível"""
        pass

    @abstractmethod
    def mark_worker_as_available(self, worker_id: UUID) -> None:
        """Tenta marcar um worker como disponível"""
        pass


class RoundRobinLoadBalancer(LoadBalancer):

    def __init__(self) -> None:
        self.workers: dict[UUID, Worker] = dict()
        self.worker_id_deque: deque[UUID] = deque()
        self.to_be_removed_worker_id_set: set[UUID] = set()
        self.full_worker_id_set: set[UUID] = set()

    def register(self, worker: Worker) -> None:
        if worker.id not in self.workers:
            self.workers[worker.id] = worker
            self.worker_id_deque.append(worker.id)
            self.to_be_removed_worker_id_set.discard(worker.id)
            self.full_worker_id_set.discard(worker.id)

    def unregister(self, worker_id: UUID) -> None:
        if worker_id not in self.workers:
            raise WorkerIdNotRegisteredError(
                f"O id {worker_id} não está registrado no balanceador"
            )

        self.workers.pop(worker_id)

        if self.worker_id_deque[0] == worker_id:
            self.worker_id_deque.popleft()
        else:
            self.to_be_removed_worker_id_set.add(worker_id)
            self.full_worker_id_set.discard(worker_id)

    def get_worker(self) -> Worker | None:
        while self.worker_id_deque:
            worker_id = self.worker_id_deque[0]

            # Remove workers desregistrados da fila
            if worker_id in self.to_be_removed_worker_id_set:
                self.worker_id_deque.popleft()
                self.to_be_removed_worker_id_set.remove(worker_id)
                continue

            worker = self.workers[worker_id]

            # Remove workers cheios da fila
            if not worker.is_available():
                self.worker_id_deque.popleft()
                self.full_worker_id_set.add(worker_id)
                continue

            # Se passou pelas validações, rotaciona a fila e retorna o worker
            self.worker_id_deque.rotate(-1)
            return worker

        return None

    def mark_worker_as_available(self, worker_id: UUID) -> None:
        if worker_id not in self.workers:
            raise WorkerIdNotRegisteredError(
                f"O id {worker_id} não está registrado no balanceador"
            )

        worker = self.workers[worker_id]
        if worker.is_available():
            self.worker_id_deque.append(worker.id)
            self.full_worker_id_set.discard(worker_id)
