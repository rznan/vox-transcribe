import json
import logging
from datetime import datetime
from uuid import UUID
from typing import List

from google.protobuf.timestamp_pb2 import Timestamp

from src.domain.value_objects.enums import WorkerRuntime, WorkerStatus
from src.domain.entities import Batch, Task, TaskAttempt, TaskAttemptStatus, TaskStatus, Worker
from src.transport.grpc.generated import task_service_pb2 as task_pb2
from src.transport.grpc.generated import core_worker_internal_communication_pb2 as cw_pb2
from src.domain.interfaces.services import WorkerService

logger = logging.getLogger(__name__)

# =============================
#       DICIONÁRIOS DE ENUMS TASK
# =============================


TASK_STATUS_DOMAIN_TO_PROTO = {
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

TASK_STATUS_ATTEMPT_DOMAIN_TO_PROTO = {
    TaskAttemptStatus.PENDING: task_pb2.TASK_ATTEMPT_STATUS_PENDING,
    TaskAttemptStatus.RUNNING: task_pb2.TASK_ATTEMPT_STATUS_RUNNING,
    TaskAttemptStatus.SUCCESS: task_pb2.TASK_ATTEMPT_STATUS_SUCCESS,
    TaskAttemptStatus.FAILED: task_pb2.TASK_ATTEMPT_STATUS_FAILED,
    TaskAttemptStatus.TIMED_OUT: task_pb2.TASK_ATTEMPT_STATUS_TIMED_OUT,
    TaskAttemptStatus.CANCELLED: task_pb2.TASK_ATTEMPT_STATUS_CANCELLED,
}


# =============================
#       DICIONÁRIOS DE ENUMS WORKER
# =============================

WORKER_RUNTIME_PROTO_TO_DOMAIN = {
    cw_pb2.WORKER_RUNTIME_CPU: WorkerRuntime.CPU,
    cw_pb2.WORKER_RUNTIME_GPU: WorkerRuntime.GPU,
}

WORKER_STATUS_PROTO_TO_DOMAIN = {
    cw_pb2.WORKER_STATUS_IDLE: WorkerStatus.IDLE,
    cw_pb2.WORKER_STATUS_BUSY: WorkerStatus.BUSY,
    cw_pb2.WORKER_STATUS_FULL: WorkerStatus.FULL,
}

WORKER_CONNECTION_STATUS_DOMAIN_TO_PROTO = {
    WorkerService.ConnectionStatusEnum.CONNECTION_STATUS_OK: cw_pb2.CONNECTION_STATUS_OK,
    WorkerService.ConnectionStatusEnum.CONNECTION_STATUS_REJECTED: cw_pb2.CONNECTION_STATUS_REJECTED,
}

WORKER_HEARTBEAT_RESPONSE_DOMAIN_TO_PROTO = {
    WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_OK: cw_pb2.HEARTBEAT_RESPONSE_OK,
    WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_TIMEOUT: cw_pb2.HEARTBEAT_RESPONSE_TIMEOUT,
    WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_NOT_REGISTERED: cw_pb2.HEARTBEAT_RESPONSE_NOT_REGISTERED,
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
    proto_status = TASK_STATUS_DOMAIN_TO_PROTO.get(
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
    proto_status = TASK_STATUS_ATTEMPT_DOMAIN_TO_PROTO.get(
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
    proto_status = TASK_STATUS_DOMAIN_TO_PROTO.get(
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


# =============================
#             WORKER
# =============================


def worker_entry_request_to_domain(request: cw_pb2.WorkerEntryRequestProto) -> Worker:
    """
    Transforma a requisição de registro gRPC em uma entidade de domínio Worker.
    """
    try:
        worker_id = UUID(request.id)
    except ValueError as e:
        logger.error(f"UUID inválido recebido no registro do worker: {request.id}")
        raise ValueError("ID do worker inválido") from e

    runtime = WORKER_RUNTIME_PROTO_TO_DOMAIN.get(
        request.runtime, WorkerRuntime.CPU
    )  # Fallback seguro para CPU

    return Worker(
        id=worker_id,
        name=request.name,
        ip=request.ip,
        port=request.port,
        runtime=runtime,
        simultaneous_capacity=request.simultaneous_capacity,
        languages_supported=list(request.languages),
        # Inicializamos com status IDLE e carga 0 por padrão num novo registro
        status=WorkerStatus.IDLE,
        current_workload=0,
    )


def connection_status_to_proto(
    status: WorkerService.ConnectionStatusEnum, worker_id: UUID, master_id: UUID
) -> cw_pb2.WorkerEntryResponseProto:
    """
    Transforma o status da conexão gerado pelo domínio na resposta gRPC.
    """
    proto_status = WORKER_CONNECTION_STATUS_DOMAIN_TO_PROTO.get(
        status, cw_pb2.CONNECTION_STATUS_UNSPECIFIED
    )

    return cw_pb2.WorkerEntryResponseProto(
        connection_status=proto_status,
        worker_id=str(worker_id),
        master_id=str(master_id),
    )

def heartbeat_response_to_proto(
    status: WorkerService.HeartbeatResponseEnum,
) -> cw_pb2.HeartbeatResponseProto:
    """
    Transforma a resposta de heartbeat do domínio na mensagem de resposta gRPC.
    """
    proto_status = WORKER_HEARTBEAT_RESPONSE_DOMAIN_TO_PROTO.get(
        status, cw_pb2.HEARTBEAT_RESPONSE_UNSPECIFIED
    )

    return cw_pb2.HeartbeatResponseProto(response=proto_status)

def shutdown_response_to_proto(success: bool) -> cw_pb2.WorkerShutdownResponseProto:
    """
    Transforma o booleano de sucesso da operação na resposta gRPC.
    """
    return cw_pb2.WorkerShutdownResponseProto(success=success)

