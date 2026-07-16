from __future__ import annotations

from _typeshed import Self
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

# ==========================
# Enums
# ==========================


class TaskStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REQUEUED = "REQUEUED"


class TaskAttemptStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"


# ==========================
# Entidades
# ==========================


@dataclass(slots=True)
class TaskAttempt:

    id: int | None
    task_id: str
    worker_id: str | None

    status: TaskAttemptStatus

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

    id: str

    batch_id: str

    status: TaskStatus

    artifact: dict[str, Any]  # TODO: avaliar remover

    filename: str

    size: int

    result_text: str | None = None

    attempts: list[TaskAttempt] = field(default_factory=list)

    @property
    def current_attempt(self):
        return self.attempts[-1] if self.attempts else None

    def create_attempt(self) -> TaskAttempt:

        attempt = TaskAttempt(
            id=None, task_id=self.id, worker_id=None, status=TaskAttemptStatus.PENDING
        )

        self.attempts.append(attempt)

        return attempt

    def is_finished(self):

        return self.status in {
            TaskStatus.SUCCEEDED,
            TaskStatus.CANCELLED,
            TaskStatus.FAILED,
        }


@dataclass(slots=True)
class Batch:

    id: str

    created_at: datetime

    tasks: list[Task] = field(default_factory=list)

    @property
    def completed_tasks(self):

        return sum(task.is_finished() for task in self.tasks)


@dataclass(slots=True)
class Worker:

    id: str

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
