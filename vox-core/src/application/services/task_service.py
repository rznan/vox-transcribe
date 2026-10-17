from __future__ import annotations

from src.application.repository_factory import RepositoryFactory
from src.application.scheduler.data_strutures import (
    BatchCircularList,
    BatchNotFoundError,
)
from src.domain.entities import Batch, Task
from src.domain.interfaces.services import TaskService
from src.domain.value_objects.enums import TaskStatus


class TaskServiceImpl(TaskService):
    """Manages batch and task lifecycle operations."""

    TERMINAL_STATUSES = {
        TaskStatus.CANCELLED,
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
    }

    def __init__(
        self,
        repository_factory: RepositoryFactory,
        scheduler: (
            BatchCircularList | None
        ) = None,  # TODO: update to the proper mechanism when the scheduler implementation gets ready
    ) -> None:
        self.repository_factory = repository_factory
        self.scheduler = scheduler

        self._batches: dict[int, Batch] = {}
        self._tasks: dict[int, Task] = {}

    async def restore_pending_batches(self) -> None:
        """Restore persisted batches and enqueue unfinished tasks."""
        offset = 0
        page_size = 100

        while True:
            async with self.repository_factory() as repositories:
                batch_summaries = await repositories.batches.list_all(
                    limit=page_size,
                    offset=offset,
                )

                if not batch_summaries:
                    return

                batches = []

                for summary in batch_summaries:
                    batch = await repositories.batches.get_with_tasks(summary.id)

                    if batch is not None:
                        batches.append(batch)

            for batch in batches:
                self._remember_batch(batch)
                self._enqueue_pending_tasks(batch)

            if len(batch_summaries) < page_size:
                return

            offset += len(batch_summaries)

    async def create_batch(self, new_batch: Batch) -> Batch:
        """Validate, persist, and schedule a new batch."""
        self._validate_new_batch(new_batch)

        for task in new_batch.tasks:
            task.status = TaskStatus.SUBMITTED

        async with self.repository_factory() as repositories:
            created_batch = await repositories.batches.add(new_batch)

            persisted_batch = (
                await repositories.batches.get_with_tasks(created_batch.id)
                or created_batch
            )

        self._remember_batch(persisted_batch)
        self._enqueue_batch(persisted_batch)

        return persisted_batch

    async def delete_batch(self, batch_id: int) -> None:
        """Cancel unfinished tasks and delete a batch."""
        batch = await self._require_batch(batch_id)

        async with self.repository_factory() as repositories:
            await self._cancel_unfinished_tasks(
                batch,
                repositories.tasks,
            )

            deleted = await repositories.batches.delete(batch_id)

            if not deleted:
                raise LookupError(f"Batch {batch_id} não encontrado.")

        self._remove_from_scheduler(batch_id)
        self._batches.pop(batch_id, None)
        self._forget_batch_tasks(batch)

    async def get_task(self, task_id: int) -> Task | None:
        """Retrieve a task from memory or persistence."""
        task = self._tasks.get(task_id)

        if task is not None:
            return task

        async with self.repository_factory() as repositories:
            task = await repositories.tasks.get_by_id(task_id)

        if task is not None:
            self._tasks[task.id] = task

        return task

    async def get_batch(self, batch_id: int) -> Batch | None:
        """Retrieve a batch from memory or persistence."""
        batch = self._batches.get(batch_id)

        if batch is not None:
            return batch

        async with self.repository_factory() as repositories:
            batch = await repositories.batches.get_with_tasks(batch_id)

        if batch is not None:
            self._remember_batch(batch)

        return batch

    async def list_batches(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Batch]:
        """List batches and reuse cached instances when available."""
        async with self.repository_factory() as repositories:
            summaries = await repositories.batches.list_all(
                limit=limit,
                offset=offset,
            )

            batches = []

            for summary in summaries:
                batch = await repositories.batches.get_with_tasks(summary.id)

                if batch is not None:
                    batches.append(batch)

        result = []

        for batch in batches:
            cached_batch = self._batches.get(batch.id)
            selected_batch = cached_batch or batch

            self._remember_batch(selected_batch)
            result.append(selected_batch)

        return result

    async def cancel_batch(self, batch_id: int) -> Batch:
        """Cancel unfinished tasks without deleting the batch."""
        batch = await self._require_batch(batch_id)

        async with self.repository_factory() as repositories:
            await self._cancel_unfinished_tasks(
                batch,
                repositories.tasks,
            )

        self._remove_from_scheduler(batch_id)
        self._remember_batch(batch)

        return batch

    def _validate_new_batch(self, batch: Batch) -> None:
        """Reject empty batches and tasks in terminal states."""
        if not batch.tasks:
            raise ValueError("O batch precisa conter ao menos uma tarefa.")

        for task in batch.tasks:
            if task.status in self.TERMINAL_STATUSES:
                raise ValueError(
                    "Não é possível submeter uma tarefa em estado terminal."
                )

    async def _require_batch(self, batch_id: int) -> Batch:
        """Retrieve a batch or raise an error when it does not exist."""
        batch = await self.get_batch(batch_id)

        if batch is None:
            raise LookupError(f"Batch {batch_id} não encontrado.")

        return batch

    async def _cancel_unfinished_tasks(
        self,
        batch: Batch,
        task_repository,
    ) -> None:
        """Cancel and persist tasks that have not reached a terminal state."""
        for task in batch.tasks:
            if task.status in self.TERMINAL_STATUSES:
                continue

            task.status = TaskStatus.CANCELLED
            await task_repository.update(task)

    def _remember_batch(self, batch: Batch) -> None:
        """Cache a batch and its persisted tasks."""
        self._batches[batch.id] = batch

        for task in batch.tasks:
            if task._id is not None:
                self._tasks[task.id] = task

    def _forget_batch_tasks(self, batch: Batch) -> None:
        """Remove a batch's tasks from the in-memory cache."""
        for task in batch.tasks:
            if task._id is not None:
                self._tasks.pop(task.id, None)

    def _enqueue_batch(self, batch: Batch) -> None:
        """Add a batch to the scheduler when one is configured."""
        if self.scheduler is not None:
            self.scheduler.append(batch)

    def _enqueue_pending_tasks(self, batch: Batch) -> None:
        """Schedule only unfinished tasks from a restored batch."""
        pending_tasks = [task for task in batch.tasks if not task.is_finished()]

        if not pending_tasks or self.scheduler is None:
            return

        pending_batch = Batch(
            _id=batch.id,
            created_at=batch.created_at,
            tasks=pending_tasks,
        )

        self.scheduler.append(pending_batch)

    def _remove_from_scheduler(self, batch_id: int) -> None:
        """Remove a batch from the scheduler if it is queued."""
        if self.scheduler is None:
            return

        try:
            self.scheduler.remove(batch_id)
        except BatchNotFoundError:
            pass
