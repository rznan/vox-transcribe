from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
)

from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.dialects.postgresql import ENUM as PG_ENUM

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from src.domain.value_objects.enums import TaskStatus, TaskAttemptStatus, WorkerStatus


class Base(DeclarativeBase):
    pass


class BatchModel(Base):

    __tablename__ = "batches"

    id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now,
        nullable=False,
    )

    tasks: Mapped[list["TaskModel"]] = relationship(
        back_populates="batch", cascade="all, delete-orphan"
    )


class TaskModel(Base):

    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
    )

    batch_id: Mapped[str] = mapped_column(
        ForeignKey("batches.id", ondelete="CASCADE"),
        nullable=False,
    )

    status: Mapped[TaskStatus] = mapped_column(
        PG_ENUM(TaskStatus, name="task_status"),
        nullable=False,
        default=TaskStatus.SUBMITTED,
    )

    artifact: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
    )

    filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )

    size: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    result_text: Mapped[str | None]

    version: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
    )

    attempts: Mapped[list["TaskAttemptModel"]] = relationship(
        back_populates="task", cascade="all, delete-orphan", lazy="selectin"
    )

    batch: Mapped["BatchModel"] = relationship(
        back_populates="tasks",
    )


class TaskAttemptModel(Base):

    __tablename__ = "task_attempts"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )

    task_id: Mapped[str] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )

    worker_id: Mapped[str | None] = mapped_column(
        ForeignKey("workers.id", ondelete="SET NULL"),
    )

    status: Mapped[TaskAttemptStatus] = mapped_column(
        PG_ENUM(TaskAttemptStatus, name="task_attempt_status"),
        nullable=False,
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now,
        nullable=False,
    )

    finished_at: Mapped[datetime | None]

    logs: Mapped[str | None]

    task: Mapped["TaskModel"] = relationship(
        back_populates="attempts",
    )

    worker: Mapped["WorkerModel"] = relationship(
        back_populates="attempts",
    )


class WorkerModel(Base):

    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(
        String(50),
        primary_key=True,
    )

    name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    ip: Mapped[INET] = mapped_column(
        INET,
        nullable=False,
    )

    port: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    runtime: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
    )

    status: Mapped[WorkerStatus] = mapped_column(  # TODO: avaliar remover
        PG_ENUM(WorkerStatus, name="worker_status"),
        default=WorkerStatus.IDLE,
        nullable=False,
    )

    heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
    )

    simultaneous_capacity: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
    )

    languages_supported: Mapped[list[str]] = mapped_column(
        JSONB,  # TODO: rever
        default=list,
        nullable=False,
    )

    attempts: Mapped[list["TaskAttemptModel"]] = relationship(
        back_populates="worker",
    )
