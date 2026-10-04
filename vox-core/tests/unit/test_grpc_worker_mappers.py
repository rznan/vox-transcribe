import uuid
import pytest

from src.domain.entities import Worker
from src.domain.value_objects.enums import WorkerRuntime, WorkerStatus
from src.domain.interfaces.services import WorkerService
from src.transport.grpc.generated import core_worker_internal_communication_pb2 as cw_pb2
from src.transport.grpc.mappers import (
    worker_entry_request_to_domain,
    connection_status_to_proto,
    heartbeat_response_to_proto,
    shutdown_response_to_proto,
)


@pytest.fixture
def valid_worker_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def valid_master_id() -> uuid.UUID:
    return uuid.uuid4()


# ============================================================================
# Request para Domain
# ============================================================================


def test_worker_entry_request_to_domain_valid(valid_worker_id: uuid.UUID):
    """Testa se uma requisição válida mapeia corretamente para a entidade Worker."""
    request = cw_pb2.WorkerEntryRequestProto(
        id=str(valid_worker_id),
        name="worker-node-1",
        ip="192.168.0.10",
        port=50051,
        runtime=cw_pb2.WORKER_RUNTIME_GPU,
        simultaneous_capacity=4,
        languages=["pt-br", "en-us"],
    )

    worker = worker_entry_request_to_domain(request)

    assert isinstance(worker, Worker)
    assert worker.id == valid_worker_id
    assert worker.name == "worker-node-1"
    assert worker.ip == "192.168.0.10"
    assert worker.port == 50051
    assert worker.runtime == WorkerRuntime.GPU
    assert worker.simultaneous_capacity == 4
    assert worker.languages_supported == ["pt-br", "en-us"]
    assert worker.status == WorkerStatus.IDLE
    assert worker.current_workload == 0


def test_worker_entry_request_to_domain_invalid_uuid():
    """Testa se o mapper rejeita um UUID malformado protegendo a camada de domínio."""
    request = cw_pb2.WorkerEntryRequestProto(
        id="isso-nao-e-um-uuid",
        name="worker-node-2",
        ip="192.168.0.11",
        port=50051,
        runtime=cw_pb2.WORKER_RUNTIME_CPU,
    )

    with pytest.raises(ValueError, match="ID do worker inválido"):
        worker_entry_request_to_domain(request)


def test_worker_entry_request_to_domain_fallback_runtime(valid_worker_id: uuid.UUID):
    """Testa se um runtime não especificado sofre fallback seguro para CPU."""
    request = cw_pb2.WorkerEntryRequestProto(
        id=str(valid_worker_id),
        name="worker-node-3",
        ip="192.168.0.12",
        port=50051,
        # Simulando uma versão de Worker desatualizada que envia UNSPECIFIED ou o protobuf omitiu
        runtime=cw_pb2.WORKER_RUNTIME_UNSPECIFIED,
    )

    worker = worker_entry_request_to_domain(request)
    assert worker.runtime == WorkerRuntime.CPU


# ============================================================================
# Domain para Response/Proto
# ============================================================================


def test_connection_status_to_proto_ok(
    valid_worker_id: uuid.UUID, valid_master_id: uuid.UUID
):
    """Garante o mapeamento de uma conexão aceita (OK)."""
    response = connection_status_to_proto(
        status=WorkerService.ConnectionStatusEnum.CONNECTION_STATUS_OK,
        worker_id=valid_worker_id,
        master_id=valid_master_id,
    )

    assert isinstance(response, cw_pb2.WorkerEntryResponseProto)
    assert response.connection_status == cw_pb2.CONNECTION_STATUS_OK
    assert response.worker_id == str(valid_worker_id)
    assert response.master_id == str(valid_master_id)


def test_connection_status_to_proto_rejected(
    valid_worker_id: uuid.UUID, valid_master_id: uuid.UUID
):
    """Garante o mapeamento de uma conexão rejeitada."""
    response = connection_status_to_proto(
        status=WorkerService.ConnectionStatusEnum.CONNECTION_STATUS_REJECTED,
        worker_id=valid_worker_id,
        master_id=valid_master_id,
    )

    assert response.connection_status == cw_pb2.CONNECTION_STATUS_REJECTED


def test_heartbeat_response_to_proto_ok():
    """Garante o mapeamento do status do heartbeat de OK."""
    response = heartbeat_response_to_proto(
        WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_OK
    )

    assert isinstance(response, cw_pb2.HeartbeatResponseProto)
    assert response.response == cw_pb2.HEARTBEAT_RESPONSE_OK


def test_heartbeat_response_to_proto_timeout():
    """Garante o mapeamento do status do heartbeat de TIMEOUT."""
    response = heartbeat_response_to_proto(
        WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_TIMEOUT
    )

    assert response.response == cw_pb2.HEARTBEAT_RESPONSE_TIMEOUT


def test_shutdown_response_to_proto_success():
    """Valida a criação do proto para sinal de desligamento bem sucedido."""
    response = shutdown_response_to_proto(True)

    assert isinstance(response, cw_pb2.WorkerShutdownResponseProto)
    assert response.success is True


def test_shutdown_response_to_proto_failure():
    """Valida a criação do proto para sinal de desligamento com falha."""
    response = shutdown_response_to_proto(False)

    assert response.success is False
