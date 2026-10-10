from uuid import uuid4
from unittest.mock import AsyncMock, MagicMock, create_autospec

import grpc
import pytest

from src.domain.interfaces.services import WorkerService
from src.domain.value_objects.enums import WorkerRuntime, WorkerStatus
from src.transport.grpc.generated import (
    core_worker_internal_communication_pb2 as pb2,
)
from src.transport.grpc.services import MasterNodeServiceImpl


class MockRpcError(Exception):
    """Sentinel exception raised by the mocked gRPC context abort."""


@pytest.fixture
def master_id():
    return uuid4()


@pytest.fixture
def worker_id():
    return uuid4()


@pytest.fixture
def mock_worker_service():
    return create_autospec(WorkerService, instance=True)


@pytest.fixture
def servicer(mock_worker_service, master_id):
    return MasterNodeServiceImpl(mock_worker_service, master_id)


@pytest.fixture
def mock_context():
    context = MagicMock(spec=grpc.aio.ServicerContext)
    context.abort = AsyncMock(side_effect=MockRpcError)
    return context


class TestMasterNodeServiceImpl:
    @pytest.mark.asyncio
    async def test_register_worker_success(
        self, servicer, mock_worker_service, master_id, worker_id, mock_context
    ):
        mock_worker_service.register_worker.return_value = (
            WorkerService.ConnectionStatusEnum.CONNECTION_STATUS_OK
        )
        request = pb2.WorkerEntryRequestProto(
            id=str(worker_id),
            name="worker-1",
            ip="127.0.0.1",
            port=50051,
            runtime=pb2.WORKER_RUNTIME_GPU,
            simultaneous_capacity=4,
            languages=["en", "pt"],
        )

        response = await servicer.RegisterWorker(request, mock_context)

        assert response.connection_status == pb2.CONNECTION_STATUS_OK
        assert response.worker_id == str(worker_id)
        assert response.master_id == str(master_id)
        registered_worker = mock_worker_service.register_worker.await_args.args[0]
        assert registered_worker.id == worker_id
        assert registered_worker.name == "worker-1"
        assert registered_worker.runtime == WorkerRuntime.GPU
        assert registered_worker.status == WorkerStatus.IDLE
        assert registered_worker.languages_supported == ["en", "pt"]

    @pytest.mark.asyncio
    async def test_register_worker_invalid_uuid(self, servicer, mock_worker_service, mock_context):
        request = pb2.WorkerEntryRequestProto(id="not-a-uuid")

        with pytest.raises(MockRpcError):
            await servicer.RegisterWorker(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INVALID_ARGUMENT, "ID do worker inválido"
        )
        mock_worker_service.register_worker.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_worker_internal_error(
        self, servicer, mock_worker_service, worker_id, mock_context
    ):
        mock_worker_service.register_worker.side_effect = RuntimeError("database down")
        request = pb2.WorkerEntryRequestProto(id=str(worker_id))

        with pytest.raises(MockRpcError):
            await servicer.RegisterWorker(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INTERNAL,
            "Erro interno ao processar solicitação de entrada do worker.",
        )

    @pytest.mark.asyncio
    async def test_send_heartbeat_success(
        self, servicer, mock_worker_service, master_id, worker_id, mock_context
    ):
        mock_worker_service.compute_heartbeat.return_value = (
            WorkerService.HeartbeatResponseEnum.HEARTBEAT_RESPONSE_OK
        )
        request = pb2.HeartbeatProto(
            master_id=str(master_id),
            worker_id=str(worker_id),
            worker_status=pb2.WORKER_STATUS_BUSY,
        )

        response = await servicer.SendHeartbeat(request, mock_context)

        assert response.response == pb2.HEARTBEAT_RESPONSE_OK
        mock_worker_service.compute_heartbeat.assert_awaited_once_with(worker_id)

    @pytest.mark.asyncio
    async def test_send_heartbeat_rejects_wrong_master(
        self, servicer, mock_worker_service, worker_id, mock_context
    ):
        request = pb2.HeartbeatProto(master_id=str(uuid4()), worker_id=str(worker_id))

        with pytest.raises(MockRpcError):
            await servicer.SendHeartbeat(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.PERMISSION_DENIED, "master_id does not match"
        )
        mock_worker_service.compute_heartbeat.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_send_heartbeat_rejects_invalid_worker_uuid(
        self, servicer, mock_worker_service, master_id, mock_context
    ):
        request = pb2.HeartbeatProto(master_id=str(master_id), worker_id="bad-id")

        with pytest.raises(MockRpcError):
            await servicer.SendHeartbeat(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INVALID_ARGUMENT, "worker_id is not a valid UUID"
        )
        mock_worker_service.compute_heartbeat.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_send_heartbeat_internal_error(
        self, servicer, mock_worker_service, master_id, worker_id, mock_context
    ):
        mock_worker_service.compute_heartbeat.side_effect = RuntimeError("failed")
        request = pb2.HeartbeatProto(master_id=str(master_id), worker_id=str(worker_id))

        with pytest.raises(MockRpcError):
            await servicer.SendHeartbeat(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INTERNAL,
            "Erro interno ao processar o sinal de vida do worker.",
        )

    @pytest.mark.asyncio
    async def test_shutdown_worker_success(
        self, servicer, mock_worker_service, master_id, worker_id, mock_context
    ):
        mock_worker_service.shutdown_worker.return_value = True
        request = pb2.WorkerShutdownProto(
            master_id=str(master_id), worker_id=str(worker_id)
        )

        response = await servicer.ShutdownWorker(request, mock_context)

        assert response.success is True
        mock_worker_service.shutdown_worker.assert_awaited_once_with(worker_id)

    @pytest.mark.asyncio
    async def test_shutdown_worker_false_result(
        self, servicer, mock_worker_service, master_id, worker_id, mock_context
    ):
        mock_worker_service.shutdown_worker.return_value = False
        request = pb2.WorkerShutdownProto(
            master_id=str(master_id), worker_id=str(worker_id)
        )

        response = await servicer.ShutdownWorker(request, mock_context)

        assert response.success is False

    @pytest.mark.asyncio
    async def test_shutdown_worker_rejects_wrong_master(
        self, servicer, mock_worker_service, worker_id, mock_context
    ):
        request = pb2.WorkerShutdownProto(
            master_id=str(uuid4()), worker_id=str(worker_id)
        )

        with pytest.raises(MockRpcError):
            await servicer.ShutdownWorker(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.PERMISSION_DENIED, "master_id does not match"
        )
        mock_worker_service.shutdown_worker.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_shutdown_worker_rejects_invalid_worker_uuid(
        self, servicer, mock_worker_service, master_id, mock_context
    ):
        request = pb2.WorkerShutdownProto(master_id=str(master_id), worker_id="bad-id")

        with pytest.raises(MockRpcError):
            await servicer.ShutdownWorker(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INVALID_ARGUMENT, "worker_id is not a valid UUID"
        )
        mock_worker_service.shutdown_worker.assert_not_awaited()
