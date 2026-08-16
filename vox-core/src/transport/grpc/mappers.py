from datetime import datetime
import json

from google.protobuf.timestamp_pb2 import Timestamp
from src.domain.entities import Batch, Task, TaskStatus
from src.transport.grpc.generated import task_service_pb2 as pb2

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


def submit_batch_request_to_domain(request: pb2.SubmitBatchRequestProto) -> Batch:
    tasks = [submit_task_request_to_domain(t) for t in request.tasks]
    created_at = datetime.now()
    return Batch(created_at, tasks=tasks)


def submit_task_request_to_domain(request: pb2.SubmitTaskRequestProto) -> Task:
    artifact_data = {}
    if request.artifact_json:
        artifact_data = json.loads(request.artifact_json)

    return Task(
        status=TaskStatus.SUBMITTED,
        artifact=artifact_data,
        filename=request.filename,
        size=request.size,
    )


def batch_domain_to_submit_response(batch: Batch) -> pb2.SubmitBatchResponseProto:
    tasks_proto = [task_domain_to_submit_response(t) for t in batch.tasks]
    return pb2.SubmitBatchResponseProto(
        batch_id=batch.id,
        created_at=Timestamp().FromDatetime(batch.created_at),
        tasks=tasks_proto,
    )


def task_domain_to_submit_response(task: Task) -> pb2.SubmitTaskResponseProto:
    proto_status = STATUS_DOMAIN_TO_PROTO.get(task.status, pb2.TASK_STATUS_UNSPECIFIED)
    return pb2.SubmitTaskResponseProto(
        task_id=task.id, status=proto_status, message="ok"
    )


def task_domain_to_status_response(task: Task) -> pb2.GetTaskStatusResponseProto:
    proto_status = STATUS_DOMAIN_TO_PROTO.get(task.status, pb2.TASK_STATUS_UNSPECIFIED)
    return pb2.GetTaskStatusResponseProto(
        task_id=task.id,
        status=proto_status,
        result_text=task.result_text or "",  # TODO: avaliar como enviar futuramente
        attempt_count=len(task.attempts),
    )
