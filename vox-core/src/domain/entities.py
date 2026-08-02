from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID
from typing import Any
from uuid import UUID

from sqlalchemy import Uuid

from src.domain.value_objects.enums import TaskAttemptStatus, TaskStatus, WorkerStatus

# ==========================
# Entidades
# ==========================


@dataclass(slots=True)
class TaskAttempt:

    task_id: str
    worker_id: str | None

    status: TaskAttemptStatus

    id: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    logs: str | None = None

    @property
    def duration(self):
        if self.started_at and self.finished_at:
            return self.finished_at - self.started_at
        return None


@dataclass(slots=True)
class Task:

    status: TaskStatus

    artifact: dict[str, Any]  # TODO: avaliar remover

    filename: str

    size: int

    batch_id: int | None = None

    id: str | None = None

    result_text: str | None = None

    attempts: list[TaskAttempt] = field(default_factory=list)

    @property
    def current_attempt(self):
        return self.attempts[-1] if self.attempts else None

    def create_attempt(self) -> TaskAttempt:
        if self.id != None:

            attempt = TaskAttempt(
                id=None,
                task_id=self.id,
                worker_id=None,
                status=TaskAttemptStatus.PENDING,
            )

            self.attempts.append(attempt)

            return attempt

        raise  # TODO execções

    def is_finished(self):

        return self.status in {
            TaskStatus.SUCCEEDED,
            TaskStatus.CANCELLED,
            TaskStatus.FAILED,
        }


@dataclass(slots=True)
class Batch:

    created_at: datetime

    id: int | None = None

    tasks: list[Task] = field(
        default_factory=list
    )  # TODO: permitir nulo para checar se veio vazio do repositório ou realamente não tem tasks

    @property
    def completed_tasks(self):

        return sum(task.is_finished() for task in self.tasks)


@dataclass(slots=True)
class Worker:

    id: UUID

    name: str

    ip: str

    port: int

    runtime: str

    status: WorkerStatus = WorkerStatus.IDLE

    heartbeat: datetime | None = None

    simultaneous_capacity: int = 1

    languages_supported: list[str] = field(default_factory=list)

    def is_available(self):

        return self.status == WorkerStatus.IDLE
