from collections import deque
from src.domain.entities import Batch, Task


class QueueMismatchError(Exception):
    """Lançada quando uma tarefa tenta ser inserida na fila errada"""

    def __init__(self, task_id: int, batch_id: int) -> None:
        self.task_id = task_id
        self.batch_id = batch_id
        self.message = f"A tarefa {task_id} não pertence ao batch {batch_id}"
        super().__init__(self.message)


class BatchCompletedError(Exception):
    """Lançada quando há a tentativa de obter uma tarefa não processada de um batch sem tarefas não processadas"""

    def __init__(self, batch_id: int) -> None:
        self.batch_id = batch_id
        self.message = f"O batch {batch_id} não apresenta tarefas não processadas"
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

    def get(self):
        return self.queue.popleft()

    def put(self, task_id: int):
        item_task = self.items[task_id]

        if item_task == None:
            raise QueueMismatchError(task_id, self.batch.id)
        self.queue.append(item_task)


class BatchQueue:

    def __init__(self) -> None:
        self.queue: deque[TaskQueue] = deque()
        self.items: dict[int, TaskQueue] = dict()

    def put(self, batch: Batch):
        tq = TaskQueue(batch)
        self.items[batch.id] = tq
        self.queue.append(tq)

    def getTask(self) -> Task:
        tq = self.queue[0]
        task = tq.get()

        if task == None and tq.batch.completed_tasks == tq.length:
            self.queue.pop()
            raise BatchCompletedError(tq.batch.id)

        self.queue.rotate(-1)

        return task

    def requeueTask(self, batch_id: int, task_id: int):
        tq = self.items[batch_id]
        tq.put(task_id)

    def removeBatch(self, batch_id: int):
        for tq in self.queue:
            if tq.batch.id == batch_id:
                self.queue.remove(tq)
                break
