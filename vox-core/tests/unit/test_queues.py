from datetime import datetime

import pytest

from src.domain.entities import Batch, Task
from src.domain.value_objects.enums import TaskStatus
from src.application.scheduler.queues import QueueMismatchError, TaskQueue


def create_batch() -> tuple[Batch, Task]:
    batch = Batch(_id=0, created_at=datetime.now())

    task = Task(
        _id=0,
        _batch_id=0,
        status=TaskStatus.SUBMITTED,
        artifact={"cmd": "echo hello"},
        filename="input.txt",
        size=512,
    )

    batch.tasks.append(task)

    return batch, task


def create_Taskqueue_with_a_element() -> tuple[TaskQueue, Batch, Task]:
    batch, task = create_batch()
    return TaskQueue(batch), batch, task


class TestTaskQueue:

    def test_TaskQueue_initialization(self):

        queue, _, task = create_Taskqueue_with_a_element()
        assert queue.length == 1
        assert queue.items[0] == task

    def test_dequeue_with_a_task_returns_a_task(self):
        taskQueue, _, task = create_Taskqueue_with_a_element()

        retrieved_task = taskQueue.dequeue()

        assert retrieved_task == task

    def test_dequeue_without_a_task_raises_IndexError(self):
        taskQueue, _, _ = create_Taskqueue_with_a_element()

        taskQueue.dequeue()
        with pytest.raises(IndexError):
            taskQueue.dequeue()

    def test_enqueueing_a_matching_task_works(self):
        taskQueue, _, _ = create_Taskqueue_with_a_element()

        taskQueue.dequeue()

        taskQueue.enqueue(0)

        assert taskQueue.length == 1

    def test_enqueueing_a_mismatching_task_raises_QueueMismatchError(
        self,
    ):
        taskQueue, _, _ = create_Taskqueue_with_a_element()

        taskQueue.dequeue()

        with pytest.raises(QueueMismatchError):
            taskQueue.enqueue(1)

    def test_enqueueing_a_task_doesnt_generate_duplicates(self):
        taskQueue, _, task = create_Taskqueue_with_a_element()
        taskQueue.enqueue(task.id)
        assert taskQueue.length == 1
