from datetime import datetime
import pytest

from src.domain.entities import Batch, Task
from src.domain.value_objects.enums import TaskStatus
from src.application.scheduler.data_strutures import (
    BatchCircularList,
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


@pytest.fixture
def batch_circular_queue(sample_batch: Batch) -> BatchCircularList:
    queue = BatchCircularList()
    queue.append(sample_batch)
    return queue


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


class TestBatchCircularQueue:

    def test_iniitalization_sets_correct_length_and_items(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        assert len(batch_circular_queue) == 1
        assert batch_circular_queue.items[0].batch == sample_batch

    def test_get_returns_first_batch(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        retrieved_task_queue = batch_circular_queue.getNext()
        assert retrieved_task_queue.batch == sample_batch

    def test_remove_updates_queued_set(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.remove(sample_batch.id)
        assert sample_batch.id not in batch_circular_queue._queued_batch_ids

    def test_remove_updates_the_length_correctly(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.remove(sample_batch.id)
        assert len(batch_circular_queue) == 0

    def test_dequeue_on_empty_queue_raises_error(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.remove(sample_batch.id)

        with pytest.raises(QueueEmptyError):
            batch_circular_queue.getNext()

    def test_batch_not_in_set_is_removed_when_attempted_to_get(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.remove(sample_batch.id)
        try:
            batch_circular_queue.getNext()
        except:
            pass

        assert len(batch_circular_queue.queue) == 0

    def test_duplicate_batch_is_ignored(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.append(sample_batch)
        assert len(batch_circular_queue) == 1
