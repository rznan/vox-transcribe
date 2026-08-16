import asyncio
import pytest

from datetime import datetime

from src.domain.entities import Batch, Task
from src.domain.value_objects.enums import TaskStatus
from src.application.scheduler.data_strutures import (
    BatchCircularList,
    QueueMismatchError,
    QueueEmptyError,
    BatchNotFoundError,
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


class TestBatchCircularList:

    def test_initialization_sets_correct_length_and_items(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        assert len(batch_circular_queue) == 1
        assert batch_circular_queue.items[0].batch == sample_batch

    @pytest.mark.asyncio
    async def test_get_returns_first_batch(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        retrieved_task_queue = await batch_circular_queue.getNext()
        assert retrieved_task_queue.batch == sample_batch

    def test_remove_returns_the_correct_task_queue(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        result = batch_circular_queue.remove(sample_batch.id)
        assert result.batch == sample_batch

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

    def test_remove_raises_batch_not_found_error(
        self, batch_circular_queue: BatchCircularList
    ):
        sample_batch_id = 999

        with pytest.raises(BatchNotFoundError) as exc_info:
            batch_circular_queue.remove(sample_batch_id)

        assert "A lista de batches não contem um batch com id: 999" in str(
            exc_info.value
        )

    @pytest.mark.asyncio
    async def test_get_next_blocks_on_empty_queue(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.remove(sample_batch.id)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(batch_circular_queue.getNext(), timeout=0.1)

    @pytest.mark.asyncio
    async def test_batch_not_in_set_is_removed_when_attempted_to_get(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.remove(sample_batch.id)

        # We wrap in wait_for because getNext will pop the removed batch,
        # realize the queue is empty, and start waiting indefinitely.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(batch_circular_queue.getNext(), timeout=0.1)

        assert len(batch_circular_queue.queue) == 0

    def test_duplicate_batch_is_ignored(
        self, batch_circular_queue: BatchCircularList, sample_batch: Batch
    ):
        batch_circular_queue.append(sample_batch)
        assert len(batch_circular_queue) == 1

    def test_requeue_task_success(
        self, batch_circular_queue: BatchCircularList, sample_task: Task, mocker
    ):
        tq = batch_circular_queue.items[sample_task.batch_id]
        mocker.patch.object(tq, "enqueue")

        batch_circular_queue._batch_available.clear()

        batch_circular_queue.requeueTask(sample_task)

        tq.enqueue.assert_called_once_with(task_id=sample_task.id)

        assert batch_circular_queue._batch_available.is_set()

    def test_requeue_task_raises_batch_not_found_error(
        self, batch_circular_queue: BatchCircularList, sample_task: Task
    ):
        sample_task._batch_id = 999

        with pytest.raises(BatchNotFoundError) as exc_info:
            batch_circular_queue.requeueTask(sample_task)

        assert "A lista de batches não contem um batch com id: 999" in str(
            exc_info.value
        )
