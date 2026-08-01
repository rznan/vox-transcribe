from __future__ import annotations

from src.domain.entities import Batch, Task, TaskAttempt, Worker
from src.infrastructure.persistence.models import (
    BatchModel,
    TaskAttemptModel,
    TaskModel,
    WorkerModel,
)

# ==========================================
# Task Mappers
# ==========================================


def task_to_model(task: Task) -> TaskModel:
    """Converte uma entidade Task do domínio para TaskModel (ORM)."""
    model = TaskModel(
        id=task.id,
        batch_id=task.batch_id,
        status=task.status,
        artifact=task.artifact,
        filename=task.filename,
        size=task.size,
        result_text=task.result_text,
        attempts=[task_attempt_to_model(attempt) for attempt in task.attempts],
    )
    return model


def model_to_task(model: TaskModel) -> Task:
    """Converte um TaskModel (ORM) para a entidade Task do domínio."""
    attempts = [
        model_to_task_attempt(attempt_model) for attempt_model in (model.attempts or [])
    ]

    return Task(
        id=model.id,
        batch_id=model.batch_id,
        status=model.status,
        artifact=model.artifact,
        filename=model.filename,
        size=model.size,
        result_text=model.result_text,
        attempts=attempts,
    )


# ==========================================
# Worker Mappers
# ==========================================


def worker_to_model(worker: Worker) -> WorkerModel:
    """Converte uma entidade Worker do domínio para WorkerModel (ORM)."""
    model = WorkerModel(
        id=worker.id,
        name=worker.name,
        ip=worker.ip,
        port=worker.port,
        runtime=worker.runtime,
        status=worker.status,
        heartbeat=worker.heartbeat,
        simultaneous_capacity=worker.simultaneous_capacity,
        languages_supported=list(worker.languages_supported),
    )
    return model


def model_to_worker(model: WorkerModel) -> Worker:
    """Converte um WorkerModel (ORM) para a entidade Worker do domínio."""
    return Worker(
        id=model.id,
        name=model.name,
        ip=str(model.ip) if model.ip is not None else "",
        port=model.port,
        runtime=model.runtime,
        status=model.status,
        heartbeat=model.heartbeat,
        simultaneous_capacity=model.simultaneous_capacity,
        languages_supported=list(model.languages_supported or []),
    )


# ==========================================
# TaskAttempt Mappers
# ==========================================


def task_attempt_to_model(attempt: TaskAttempt) -> TaskAttemptModel:
    """Converte uma entidade TaskAttempt do domínio para TaskAttemptModel (ORM)."""
    model = TaskAttemptModel(
        task_id=attempt.task_id,
        worker_id=attempt.worker_id,
        status=attempt.status,
        started_at=attempt.started_at,
        finished_at=attempt.finished_at,
        logs=attempt.logs,
    )

    if attempt.id is not None:
        model.id = attempt.id

    return model


def model_to_task_attempt(model: TaskAttemptModel) -> TaskAttempt:
    """Converte um TaskAttemptModel (ORM) para a entidade TaskAttempt do domínio."""
    return TaskAttempt(
        id=model.id,
        task_id=model.task_id,
        worker_id=model.worker_id,
        status=model.status,
        started_at=model.started_at,
        finished_at=model.finished_at,
        logs=model.logs,
    )


# ==========================================
# Batch Mappers
# ==========================================


def batch_to_model(batch: Batch) -> BatchModel:
    """Converte uma entidade Batch do domínio para BatchModel (ORM)."""
    model = BatchModel(
        id=batch.id,
        created_at=batch.created_at,
        tasks=[task_to_model(task) for task in batch.tasks],
    )
    return model


def model_to_batch(model: BatchModel) -> Batch:
    """Converte um BatchModel (ORM) para a entidade Batch do domínio."""

    return Batch(
        id=model.id,
        created_at=model.created_at,
        tasks=[],
    )


def model_to_batch_with_tasks(model: BatchModel) -> Batch:
    """Converte um BatchModel (ORM) para a entidade Batch do domínio."""
    tasks = [model_to_task(task_model) for task_model in (model.tasks or [])]

    return Batch(id=model.id, created_at=model.created_at, tasks=tasks)
