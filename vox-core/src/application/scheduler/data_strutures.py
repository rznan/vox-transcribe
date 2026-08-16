import asyncio

from collections import deque
from src.domain.entities import Batch, Task


class QueueMismatchError(Exception):
    """Lançada quando uma tarefa tenta ser inserida na fila errada."""

    def __init__(self, task_id: int, batch_id: int) -> None:
        self.task_id = task_id
        self.batch_id = batch_id
        super().__init__(f"A tarefa {task_id} não pertence ao batch {batch_id}")


class QueueEmptyError(Exception):
    """Lançada quando tenta-se remover um item de uma fila vazia."""

    pass


class BatchNotFoundError(Exception):
    """Lançada quando não é possível encontrar um batch com id especificado."""

    pass


class TaskQueue:

    def __init__(self, batch: Batch) -> None:
        self.batch = batch
        self.queue: deque[Task] = deque(batch.tasks)

        self.items: dict[int, Task] = {
            task.id: task for task in batch.tasks if task.id is not None
        }

        # Cache para busca O(1) na hora de evitar duplicatas
        self._queued_task_ids: set[int] = {
            task.id for task in batch.tasks if task.id is not None
        }

    def __len__(self) -> int:
        """Permite usar a função nativa len() na classe."""
        return len(self.queue)

    def dequeue(self) -> Task:
        try:
            task = self.queue.popleft()
            self._queued_task_ids.remove(task.id)
            return task
        except IndexError:
            raise QueueEmptyError(f"A fila do batch {self.batch.id} está vazia.")

    def enqueue(self, task_id: int) -> None:
        if task_id not in self.items:
            raise QueueMismatchError(task_id, self.batch.id)

        if task_id not in self._queued_task_ids:
            task = self.items[task_id]
            self.queue.append(task)
            self._queued_task_ids.add(task_id)


class BatchCircularList:
    def __init__(self) -> None:
        self.queue: deque[TaskQueue] = deque()
        self.items: dict[int, TaskQueue] = {}
        self._queued_batch_ids: set[int] = set()

        self._batch_available = asyncio.Event()

    def __len__(self) -> int:
        return len(self.items)

    def append(self, batch: Batch) -> None:
        if batch.id not in self._queued_batch_ids:
            tq = TaskQueue(batch)
            self.items[batch.id] = tq
            self.queue.append(tq)
            self._queued_batch_ids.add(batch.id)

            self._batch_available.set()

    async def getNext(self) -> TaskQueue:
        while True:
            if not self.queue:
                self._batch_available.clear()
                await self._batch_available.wait()
                continue

            tq = self.queue[0]

            if tq.batch.id not in self._queued_batch_ids:
                self.queue.popleft()
                continue

            self.queue.rotate(-1)
            return tq

    def remove(self, batch_id: int) -> TaskQueue:
        if batch_id in self._queued_batch_ids:
            self._queued_batch_ids.remove(batch_id)
            tq = self.items[batch_id]
            del self.items[batch_id]
            return tq
        raise BatchNotFoundError(
            f"A lista de batches não contem um batch com id: {batch_id}"
        )

    def requeueTask(self, task: Task) -> None:
        if task.batch_id in self.items:
            tq = self.items[task.batch_id]
            tq.enqueue(task_id=task.id)
            self._batch_available.set()
        else:
            raise BatchNotFoundError(
                f"A lista de batches não contem um batch com id: {task.batch_id}"
            )
