import json
import logging
from datetime import datetime
from typing import List

from google.protobuf.timestamp_pb2 import Timestamp
from src.domain.entities import Batch, Task, TaskStatus
from src.transport.grpc.generated import task_service_pb2 as pb2

logger = logging.getLogger(__name__)

STATUS_DOMAIN_TO_PROTO = {
    TaskStatus.SUBMITTED: pb2.TASK_STATUS_SUBMITTED,
    TaskStatus.QUEUED: pb2.TASK_STATUS_QUEUED,
    TaskStatus.ASSIGNED: pb2.TASK_STATUS_ASSIGNED,
    TaskStatus.RUNNING: pb2.TASK_STATUS_RUNNING,
    TaskStatus.CANCEL_REQUESTED: pb2.TASK_STATUS_CANCEL_REQUESTED,
    TaskStatus.CANCELLED: pb2.TASK_STATUS_CANCELLED,
    TaskStatus.SUCCEEDED: pb2.TASK_STATUS_SUCCEEDED,
    TaskStatus.FAILED: pb2.TASK_STATUS_FAILED,
    TaskStatus.REQUEUED: pb2.TASK_STATUS_REQUEUED,
}


def submit_task_request_to_domain(request: pb2.SubmitTaskRequestProto) -> Task:
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
    )


def submit_batch_request_to_domain(request: pb2.SubmitBatchRequestProto) -> Batch:
    tasks = [submit_task_request_to_domain(t) for t in request.tasks]
    return Batch(created_at=datetime.now(), tasks=tasks)


def batch_domain_to_submit_response(batch: Batch) -> pb2.SubmitBatchResponseProto:
    ts = Timestamp()
    ts.FromDatetime(batch.created_at)

    return pb2.SubmitBatchResponseProto(
        batch_id=batch.id,
        created_at=ts,
    )


def task_domain_to_lite_proto(task: Task) -> pb2.TaskLiteProto:
    proto_status = STATUS_DOMAIN_TO_PROTO.get(task.status, pb2.TASK_STATUS_UNSPECIFIED)
    attempts = task.attempts or []

    return pb2.TaskLiteProto(
        id=task.id,
        filename=task.filename,
        status=proto_status,
        size=task.size,
        attempt_count=len(attempts),
    )


def task_domain_to_full_proto(task: Task) -> pb2.TaskFullProto:
    proto_status = STATUS_DOMAIN_TO_PROTO.get(task.status, pb2.TASK_STATUS_UNSPECIFIED)
    attempts = task.attempts or []

    # Serializa o artefato de volta para string JSON
    artifact_json_str = json.dumps(task.artifact)

    return pb2.TaskFullProto(
        id=task.id,
        filename=task.filename,
        status=proto_status,
        size=task.size,
        attempt_count=len(attempts),
        result=task.result_text or "",
        artifact_json=artifact_json_str,
    )


def batch_domain_to_lite_proto(batch: Batch) -> pb2.BatchLiteProto:
    ts = Timestamp()
    ts.FromDatetime(batch.created_at)

    return pb2.BatchLiteProto(
        id=batch.id,
        created_at=ts,
        total_jobs_number=len(batch.tasks),
        completed_jobs_number=batch.completed_tasks,
    )


def batch_domain_to_full_proto(batch: Batch) -> pb2.BatchFullProto:
    ts = Timestamp()
    ts.FromDatetime(batch.created_at)

    return pb2.BatchFullProto(
        id=batch.id,
        created_at=ts,
        total_jobs_number=len(batch.tasks),
        completed_jobs_number=batch.completed_tasks,
        tasks=[task_domain_to_lite_proto(t) for t in batch.tasks],
    )


def list_batches_domain_to_proto(batches: List[Batch]) -> pb2.ListBatchesResponseProto:
    return pb2.ListBatchesResponseProto(
        batches=[batch_domain_to_lite_proto(b) for b in batches]
    )
