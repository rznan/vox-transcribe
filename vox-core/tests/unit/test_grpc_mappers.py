import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.domain.entities import Batch, Task, TaskAttempt, TaskAttemptStatus, TaskStatus
from src.transport.grpc.generated import task_service_pb2 as pb2
from src.transport.grpc.mappers import (
    attempt_domain_to_proto,
    batch_domain_to_full_proto,
    batch_domain_to_lite_proto,
    batch_domain_to_submit_response,
    list_batches_domain_to_proto,
    submit_batch_request_to_domain,
    submit_task_request_to_domain,
    task_domain_to_full_proto,
    task_domain_to_lite_proto,
)


@pytest.fixture
def sample_task() -> Task:
    task = Task(
        _id=100,
        _batch_id=10,
        status=TaskStatus.SUBMITTED,
        artifact={"cmd": "echo hello"},
        filename="input.txt",
        size=512,
    )

    attempt = TaskAttempt(
        _id=1,
        task_id=100,
        worker_id=uuid4(),
        status=TaskAttemptStatus.PENDING,
        started_at=datetime(2026, 8, 22, 10, 0, 0, tzinfo=timezone.utc),
    )
    task.attempts.append(attempt)

    return task


@pytest.fixture
def sample_batch(sample_task: Task) -> Batch:
    batch = Batch(
        _id=10, created_at=datetime(2026, 8, 21, 10, 0, 0, tzinfo=timezone.utc)
    )
    batch.tasks.append(sample_task)
    return batch


# ============================================================================
# Request para Domain
# ============================================================================


def test_submit_task_request_to_domain_valid_json():
    request = pb2.SubmitTaskRequestProto(
        filename="test.txt", size=500, artifact_json='{"key": "value"}'
    )

    task = submit_task_request_to_domain(request)

    assert isinstance(task, Task)
    assert task.status == TaskStatus.SUBMITTED
    assert task.filename == "test.txt"
    assert task.size == 500
    assert task.artifact == {"key": "value"}
    # O ID começa nulo na criação antes de persistir
    with pytest.raises(ValueError):
        _ = task.id


def test_submit_task_request_to_domain_invalid_json():
    request = pb2.SubmitTaskRequestProto(
        filename="test.txt", size=500, artifact_json="invalid-json"
    )

    task = submit_task_request_to_domain(request)

    # Em caso de erro de JSON, o mapper trata o erro e devolve dicionário vazio
    assert task.artifact == {}
    assert task.filename == "test.txt"


def test_submit_batch_request_to_domain():
    request = pb2.SubmitBatchRequestProto(
        tasks=[
            pb2.SubmitTaskRequestProto(filename="1.txt"),
            pb2.SubmitTaskRequestProto(filename="2.txt"),
        ]
    )

    batch = submit_batch_request_to_domain(request)

    assert isinstance(batch, Batch)
    assert isinstance(batch.created_at, datetime)
    assert len(batch.tasks) == 2
    assert batch.tasks[0].filename == "1.txt"
    assert batch.tasks[1].filename == "2.txt"


# ============================================================================
# Domain para Response/Proto
# ============================================================================


def test_batch_domain_to_submit_response(sample_batch: Batch):
    response = batch_domain_to_submit_response(sample_batch)

    assert isinstance(response, pb2.SubmitBatchResponseProto)
    assert response.batch_id == 10
    assert response.created_at.seconds > 0


def test_task_domain_to_lite_proto(sample_task: Task):
    proto = task_domain_to_lite_proto(sample_task)

    assert isinstance(proto, pb2.TaskLiteProto)
    assert proto.id == 100
    assert proto.filename == "input.txt"
    assert proto.size == 512
    assert proto.attempt_count == 1
    assert proto.status == pb2.TASK_STATUS_SUBMITTED


def test_attempt_domain_to_proto(sample_task: Task):
    attempt = sample_task.attempts[0]
    proto = attempt_domain_to_proto(attempt)

    assert isinstance(proto, pb2.TaskAttemptProto)
    assert proto.id == 1
    assert proto.worker_id == str(attempt.worker_id)
    assert proto.status == pb2.TASK_ATTEMPT_STATUS_PENDING
    assert proto.started_at.seconds > 0
    assert proto.finished_at.seconds == 0  # Não finalizado ainda (Timestamp default)
    assert proto.logs == ""


def test_task_domain_to_full_proto(sample_task: Task):
    sample_task.result_text = "testes"

    proto = task_domain_to_full_proto(sample_task)

    assert isinstance(proto, pb2.TaskFullProto)
    assert proto.id == 100
    assert proto.attempt_count == 1
    assert len(proto.attempts) == 1
    assert proto.attempts[0].id == 1
    assert proto.attempts[0].status == pb2.TASK_ATTEMPT_STATUS_PENDING
    assert proto.result == "testes"
    assert json.loads(proto.artifact_json) == {"cmd": "echo hello"}


def test_task_domain_to_full_proto_missing_optionals(sample_task: Task):
    # Removendo atributos opcionais para validar a resiliência do mapper
    sample_task.artifact = {}
    sample_task.result_text = None

    proto = task_domain_to_full_proto(sample_task)

    assert proto.attempt_count == 1
    assert proto.result == ""
    assert proto.artifact_json == ""


def test_batch_domain_to_lite_proto(sample_batch: Batch):
    proto = batch_domain_to_lite_proto(sample_batch)

    assert isinstance(proto, pb2.BatchLiteProto)
    assert proto.id == 10
    assert proto.total_jobs_number == 1
    assert proto.completed_jobs_number == 0
    assert proto.created_at.seconds > 0


def test_batch_domain_to_lite_proto_with_completed_task(sample_batch: Batch):
    sample_batch.tasks[0].status = TaskStatus.SUCCEEDED
    proto_updated = batch_domain_to_lite_proto(sample_batch)
    assert proto_updated.completed_jobs_number == 1


def test_batch_domain_to_full_proto(sample_batch: Batch):
    proto = batch_domain_to_full_proto(sample_batch)

    assert isinstance(proto, pb2.BatchFullProto)
    assert proto.id == 10
    assert proto.total_jobs_number == 1
    assert proto.completed_jobs_number == 0
    assert len(proto.tasks) == 1
    assert isinstance(proto.tasks[0], pb2.TaskLiteProto)


def test_list_batches_domain_to_proto(sample_batch: Batch):
    batches = [sample_batch, sample_batch]

    response = list_batches_domain_to_proto(batches)

    assert isinstance(response, pb2.ListBatchesResponseProto)
    assert len(response.batches) == 2
    assert isinstance(response.batches[0], pb2.BatchLiteProto)
