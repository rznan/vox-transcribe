from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID
from typing import Any

from src.domain.value_objects.enums import (
    TaskAttemptStatus,
    TaskStatus,
    WorkerRuntime,
    WorkerStatus,
)

# ==========================
# Entidades
# ==========================


@dataclass(slots=True)
class TaskAttempt:

    task_id: int
    worker_id: UUID

    status: TaskAttemptStatus

    _id: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    logs: str | None = None

    @property
    def duration(self):
        if self.started_at and self.finished_at:
            return self.finished_at - self.started_at
        return None

    @property
    def id(self) -> int:
        if self.id == None:
            raise ValueError(
                "Tentativa de acessar o id de uma tentativa não persistida."
            )
        return self.id


@dataclass(slots=True)
class Task:

    status: TaskStatus

    artifact: dict[str, Any]  # TODO: avaliar remover

    filename: str

    size: int

    _batch_id: int | None = None

    _id: int | None = None

    result_text: str | None = None

    attempts: list[TaskAttempt] = field(default_factory=list)

    @property
    def current_attempt(self):
        return self.attempts[-1] if self.attempts else None

    @property
    def id(self) -> int:
        if self._id == None:
            raise ValueError("Tentativa de acessar o id de uma Task não persistida.")
        return self._id

    @property
    def batch_id(self) -> int:
        if self._batch_id == None:
            raise ValueError("Tentativa de acessar o id de uma Task não persistida.")
        return self._batch_id

    def create_attempt(self) -> TaskAttempt:
        if self.id != None:

            attempt = TaskAttempt(
                _id=None,
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

    _id: int | None = None

    tasks: list[Task] = field(
        default_factory=list
    )  # TODO: permitir nulo para checar se veio vazio do repositório ou realamente não tem tasks

    @property
    def completed_tasks(self):
        return sum(task.is_finished() for task in self.tasks)

    @property
    def id(self) -> int:
        if self._id == None:
            raise ValueError("Tentativa de acessar o id de uma Task não persistida.")
        return self._id


@dataclass(slots=True)
class Worker:

    id: UUID

    name: str

    ip: str

    port: int

    runtime: WorkerRuntime

    status: WorkerStatus = WorkerStatus.IDLE

    heartbeat: datetime | None = None

    simultaneous_capacity: int = 1  # TODO: tornar readonly

    current_workload: int = 0

    languages_supported: list[str] = field(default_factory=list)

    def is_available(self):
        if self.current_workload >= self.simultaneous_capacity:
            self.status = WorkerStatus.FULL
        elif self.current_workload > 0:
            self.status = WorkerStatus.BUSY
        else:
            self.status = WorkerStatus.IDLE

        return self.status != WorkerStatus.FULL
