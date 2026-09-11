import asyncio
from abc import ABC, abstractmethod
from collections import deque, defaultdict
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
    def unregister(self, worker_id: UUID) -> Worker:
        """Remove um worker do balanceador de carga"""
        pass

    @abstractmethod
    async def get_worker(self, language: str) -> Worker:
        """Retorna um worker disponível com base na linguagem solicitada"""
        pass

    @abstractmethod
    def mark_worker_as_available(self, worker_id: UUID) -> None:
        """Tenta marcar um worker como disponível"""
        pass


class RoundRobinLoadBalancer(LoadBalancer):

    def __init__(self) -> None:
        self.workers: dict[UUID, Worker] = dict()

        self.language_deques: dict[str, deque[UUID]] = defaultdict(deque)

        self.to_be_removed_worker_id_sets: dict[str, set[UUID]] = defaultdict(set)

        self.full_worker_id_set: set[UUID] = set()

        self._worker_available_events: dict[str, asyncio.Event] = defaultdict(
            asyncio.Event
        )

    def register(self, worker: Worker) -> None:
        if worker.id not in self.workers:
            self.workers[worker.id] = worker
            self.full_worker_id_set.discard(worker.id)

            for lang in worker.languages_supported:
                self.language_deques[lang].append(worker.id)
                self.to_be_removed_worker_id_sets[lang].discard(worker.id)

                if worker.is_available():
                    self._worker_available_events[lang].set()

    def unregister(self, worker_id: UUID) -> Worker:
        if worker_id not in self.workers:
            raise WorkerIdNotRegisteredError(
                f"O id {worker_id} não está registrado no balanceador"
            )

        worker = self.workers.pop(worker_id)
        self.full_worker_id_set.discard(worker_id)

        # Remove o worker de todas as filas de linguagem às quais ele pertence
        for lang in worker.languages_supported:
            if (
                self.language_deques[lang]
                and self.language_deques[lang][0] == worker_id
            ):
                self.language_deques[lang].popleft()
            else:
                self.to_be_removed_worker_id_sets[lang].add(worker_id)

        return worker

    async def get_worker(self, language: str) -> Worker:
        while True:
            lang_deque = self.language_deques[language]

            if not lang_deque:
                self._worker_available_events[language].clear()
                await self._worker_available_events[language].wait()
                continue

            worker_id = lang_deque[0]

            # Remove workers desregistrados da fila específica
            if worker_id in self.to_be_removed_worker_id_sets[language]:
                lang_deque.popleft()
                self.to_be_removed_worker_id_sets[language].remove(worker_id)
                continue

            worker = self.workers[worker_id]

            # Remove workers cheios da fila
            if not worker.is_available():
                lang_deque.popleft()
                self.full_worker_id_set.add(worker_id)
                continue

            lang_deque.rotate(-1)
            return worker

    def mark_worker_as_available(self, worker_id: UUID) -> None:
        if worker_id not in self.workers:
            raise WorkerIdNotRegisteredError(
                f"O id {worker_id} não está registrado no balanceador"
            )

        worker = self.workers[worker_id]
        if worker.is_available():
            self.full_worker_id_set.discard(worker_id)
            for lang in worker.languages_supported:
                self.language_deques[lang].append(worker.id)
                self._worker_available_events[lang].set()
