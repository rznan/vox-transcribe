from datetime import datetime
import pytest

from src.domain.entities import Batch, Task
from src.domain.value_objects.enums import TaskStatus
from src.application.scheduler.queues import (
    QueueMismatchError,
    QueueEmptyError,
    TaskQueue,
)


@pytest.fixture
def sample_task() -> Task:
    return Task(
        _id=0,
        _batch_id=0,
        status=TaskStatus.SUBMITTED,
        artifact={"cmd": "echo hello"},
        filename="input.txt",
        size=512,
    )


@pytest.fixture
def sample_batch(sample_task: Task) -> Batch:
    batch = Batch(_id=0, created_at=datetime.now())
    batch.tasks.append(sample_task)
    return batch


@pytest.fixture
def task_queue(sample_batch: Batch) -> TaskQueue:
    return TaskQueue(sample_batch)


class TestTaskQueue:

    def test_initialization_sets_correct_length_and_items(
        self, task_queue: TaskQueue, sample_task: Task
    ):
        assert len(task_queue) == 1
        assert task_queue.items[0] == sample_task

    def test_dequeue_returns_first_task(self, task_queue: TaskQueue, sample_task: Task):
        retrieved_task = task_queue.dequeue()

        assert retrieved_task == sample_task
        assert len(task_queue) == 0

    def test_dequeue_on_empty_queue_raises_error(self, task_queue: TaskQueue):
        task_queue.dequeue()  # Esvazia a fila

        with pytest.raises(QueueEmptyError):
            task_queue.dequeue()

    def test_enqueue_valid_task_adds_to_queue(
        self, task_queue: TaskQueue, sample_task: Task
    ):
        task_queue.dequeue()

        task_queue.enqueue(sample_task.id)

        assert len(task_queue) == 1
        assert task_queue.queue[-1] == sample_task

    def test_enqueue_mismatching_task_raises_error(self, task_queue: TaskQueue):
        task_queue.dequeue()
        invalid_task_id = 999

        with pytest.raises(QueueMismatchError) as exc_info:
            task_queue.enqueue(invalid_task_id)

        assert exc_info.value.task_id == invalid_task_id

    def test_enqueue_duplicate_task_is_ignored(
        self, task_queue: TaskQueue, sample_task: Task
    ):
        initial_length = len(task_queue)

        task_queue.enqueue(sample_task.id)

        assert len(task_queue) == initial_length
