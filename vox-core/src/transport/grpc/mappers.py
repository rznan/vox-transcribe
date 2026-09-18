import json
import logging
from datetime import datetime
from typing import List

from google.protobuf.timestamp_pb2 import Timestamp

from src.domain.entities import Batch, Task, TaskAttempt, TaskAttemptStatus, TaskStatus
from src.transport.grpc.generated import task_service_pb2 as task_pb2

logger = logging.getLogger(__name__)

STATUS_DOMAIN_TO_PROTO = {
    TaskStatus.SUBMITTED: task_pb2.TASK_STATUS_SUBMITTED,
    TaskStatus.QUEUED: task_pb2.TASK_STATUS_QUEUED,
    TaskStatus.ASSIGNED: task_pb2.TASK_STATUS_ASSIGNED,
    TaskStatus.RUNNING: task_pb2.TASK_STATUS_RUNNING,
    TaskStatus.CANCEL_REQUESTED: task_pb2.TASK_STATUS_CANCEL_REQUESTED,
    TaskStatus.CANCELLED: task_pb2.TASK_STATUS_CANCELLED,
    TaskStatus.SUCCEEDED: task_pb2.TASK_STATUS_SUCCEEDED,
    TaskStatus.FAILED: task_pb2.TASK_STATUS_FAILED,
    TaskStatus.REQUEUED: task_pb2.TASK_STATUS_REQUEUED,
}

STATUS_ATTEMPT_DOMAIN_TO_PROTO = {
    TaskAttemptStatus.PENDING: task_pb2.TASK_ATTEMPT_STATUS_PENDING,
    TaskAttemptStatus.RUNNING: task_pb2.TASK_ATTEMPT_STATUS_RUNNING,
    TaskAttemptStatus.SUCCESS: task_pb2.TASK_ATTEMPT_STATUS_SUCCESS,
    TaskAttemptStatus.FAILED: task_pb2.TASK_ATTEMPT_STATUS_FAILED,
    TaskAttemptStatus.TIMED_OUT: task_pb2.TASK_ATTEMPT_STATUS_TIMED_OUT,
    TaskAttemptStatus.CANCELLED: task_pb2.TASK_ATTEMPT_STATUS_CANCELLED,
}

# =============================
#       TASKS & BATCHES
# =============================


def submit_task_request_to_domain(request: task_pb2.SubmitTaskRequestProto) -> Task:
    artifact_data = {}
    if request.artifact_json:
        try:
            artifact_data = json.loads(request.artifact_json)
        except json.JSONDecodeError as e:
            logger.warning(
                f"Falha ao decodificar artifact_json: {e}. Usando dicionário vazio."
            )

    return Task(
        status=TaskStatus.SUBMITTED,
        artifact=artifact_data,
        filename=request.filename,
        size=request.size,
        language=request.language,
    )


def submit_batch_request_to_domain(request: task_pb2.SubmitBatchRequestProto) -> Batch:
    tasks = [submit_task_request_to_domain(t) for t in request.tasks]
    return Batch(created_at=datetime.now(), tasks=tasks)


def batch_domain_to_submit_response(batch: Batch) -> task_pb2.SubmitBatchResponseProto:
    ts = Timestamp()
    ts.FromDatetime(batch.created_at)

    return task_pb2.SubmitBatchResponseProto(
        batch_id=batch.id,
        created_at=ts,
    )


def task_domain_to_lite_proto(task: Task) -> task_pb2.TaskLiteProto:
    proto_status = STATUS_DOMAIN_TO_PROTO.get(
        task.status, task_pb2.TASK_STATUS_UNSPECIFIED
    )
    attempts = getattr(task, "attempts", [])

    return task_pb2.TaskLiteProto(
        id=task.id,
        filename=task.filename,
        status=proto_status,
        size=task.size,
        attempt_count=len(attempts),
        language=task.language,
    )


def attempt_domain_to_proto(attempt: TaskAttempt) -> task_pb2.TaskAttemptProto:
    """
    Mapeia a entidade TaskAttempt para sua versão no gRPC.
    Lida com segurança com os IDs que possam não estar persistidos.
    """
    proto_status = STATUS_ATTEMPT_DOMAIN_TO_PROTO.get(
        attempt.status, task_pb2.TASK_ATTEMPT_STATUS_UNSPECIFIED
    )

    proto = task_pb2.TaskAttemptProto(
        status=proto_status,
        worker_id=str(attempt.worker_id) if attempt.worker_id else "",
        logs=attempt.logs or "",
    )

    handle_id_and_datetime_to_proto_conversion(attempt, proto)

    return proto


def handle_id_and_datetime_to_proto_conversion(
    attempt: TaskAttempt, proto: task_pb2.TaskAttemptProto
):
    attempt_id = getattr(attempt, "_id", None)
    if attempt_id is not None:
        proto.id = attempt_id

    if attempt.started_at:
        proto.started_at.FromDatetime(attempt.started_at)
    if attempt.finished_at:
        proto.finished_at.FromDatetime(attempt.finished_at)


def task_domain_to_full_proto(task: Task) -> task_pb2.TaskFullProto:
    proto_status = STATUS_DOMAIN_TO_PROTO.get(
        task.status, task_pb2.TASK_STATUS_UNSPECIFIED
    )
    attempts = getattr(task, "attempts", [])

    # Serializa o artefato de volta para string JSON se existir
    artifact_data = getattr(task, "artifact", None)
    artifact_json_str = json.dumps(artifact_data) if artifact_data else ""

    return task_pb2.TaskFullProto(
        id=task.id,
        filename=task.filename,
        status=proto_status,
        size=task.size,
        attempt_count=len(attempts),
        language=task.language,
        result=getattr(task, "result_text", "") or "",
        artifact_json=artifact_json_str,
        attempts=[attempt_domain_to_proto(a) for a in attempts],
    )


def batch_domain_to_lite_proto(batch: Batch) -> task_pb2.BatchLiteProto:
    ts = Timestamp()
    ts.FromDatetime(batch.created_at)
    tasks = getattr(batch, "tasks", [])

    return task_pb2.BatchLiteProto(
        id=batch.id,
        created_at=ts,
        total_jobs_number=len(tasks),
        completed_jobs_number=batch.completed_tasks,
    )


def batch_domain_to_full_proto(batch: Batch) -> task_pb2.BatchFullProto:
    ts = Timestamp()
    ts.FromDatetime(batch.created_at)
    tasks = getattr(batch, "tasks", [])

    return task_pb2.BatchFullProto(
        id=batch.id,
        created_at=ts,
        total_jobs_number=len(tasks),
        completed_jobs_number=batch.completed_tasks,
        tasks=[task_domain_to_lite_proto(t) for t in tasks],
    )


def list_batches_domain_to_proto(
    batches: List[Batch],
) -> task_pb2.ListBatchesResponseProto:
    return task_pb2.ListBatchesResponseProto(
        batches=[batch_domain_to_lite_proto(b) for b in batches]
    )
