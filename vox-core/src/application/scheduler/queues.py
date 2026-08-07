from collections import deque
from src.domain.entities import Batch, Task


class QueueMismatchError(Exception):
    """Lançada quando uma tarefa tenta ser inserida na fila errada"""

    def __init__(self, task_id: int, batch_id: int) -> None:
        self.task_id = task_id
        self.batch_id = batch_id
        self.message = f"A tarefa {task_id} não pertence ao batch {batch_id}"
        super().__init__(self.message)


class TaskQueue:

    def __init__(self, batch: Batch) -> None:
        self.batch = batch
        self.queue: deque[Task] = deque(batch.tasks)
        self.items: dict[int, Task] = dict()
        self.length = batch.tasks.__len__()

        for task in batch.tasks:
            if task.id != None:
                self.items[task.id] = task

    def dequeue(self) -> Task:
        return self.queue.popleft()

    def enqueue(self, task_id: int) -> None:
        item_task: Task
        try:
            item_task = self.items[task_id]
        except KeyError:
            raise QueueMismatchError(task_id, self.batch.id)

        if item_task not in self.queue:
            self.queue.append(item_task)


class BatchCircularQueue:

    def __init__(self) -> None:
        self.queue: deque[TaskQueue] = deque()
        self.items: dict[int, TaskQueue] = dict()
        self.length: int = 0

    def enqueue(self, batch: Batch):
        tq = TaskQueue(batch)
        self.items[batch.id] = tq
        self.queue.append(tq)
        self.length += 1

    def dequeue(self) -> TaskQueue:
        return self.queue.popleft()

    def remove(self, batch_id: int):
        for tq in self.queue:
            if tq.batch.id == batch_id:
                self.queue.remove(tq)
                break
