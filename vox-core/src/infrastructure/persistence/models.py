from __future__ import annotations

from datetime import datetime

from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, Enum, ForeignKey, Integer, String, JSON, Uuid

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from src.domain.value_objects.enums import (
    TaskStatus,
    TaskAttemptStatus,
    WorkerRuntime,
    WorkerStatus,
)


class Base(DeclarativeBase):
    pass


class BatchModel(Base):

    __tablename__ = "batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

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

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    batch_id: Mapped[int] = mapped_column(
        ForeignKey("batches.id", ondelete="CASCADE"),
        nullable=False,
    )

    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus),
        nullable=False,
        default=TaskStatus.SUBMITTED,
    )

    artifact: Mapped[dict] = mapped_column(
        JSON,
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

    task_id: Mapped[int] = mapped_column(
        ForeignKey("tasks.id", ondelete="CASCADE"),
        nullable=False,
    )

    worker_id: Mapped[UUID] = mapped_column(
        ForeignKey("workers.id", ondelete="SET NULL"),
    )

    status: Mapped[TaskAttemptStatus] = mapped_column(
        Enum(TaskAttemptStatus),
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

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)

    name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
    )

    ip: Mapped[str] = mapped_column(
        String(45),
        nullable=False,
    )

    port: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    runtime: Mapped[WorkerRuntime] = mapped_column(
        Enum(WorkerRuntime),
        nullable=False,
    )

    simultaneous_capacity: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
    )

    languages_supported: Mapped[list[str]] = mapped_column(
        JSON,
        default=list,
        nullable=False,
    )

    attempts: Mapped[list["TaskAttemptModel"]] = relationship(
        back_populates="worker",
    )
