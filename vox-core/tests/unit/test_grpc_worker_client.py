import pytest
import grpc
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from src.domain.entities import Task, TaskAttempt, TaskAttemptStatus, Worker
from src.domain.value_objects.enums import TaskAttemptStatus, TaskStatus
from src.domain.value_objects.enums import WorkerRuntime, WorkerStatus
from src.transport.grpc.worker_client import GrpcWorkerClient
from src.transport.grpc.generated import core_worker_internal_communication_pb2 as cw_pb2


@pytest.fixture
def worker() -> Worker:
    return Worker(
        id=uuid4(),
        name="worker-gpu-1",
        ip="192.168.0.50",
        port=50052,
        runtime=WorkerRuntime.GPU,
        simultaneous_capacity=2,
    )


@pytest.fixture
def task() -> Task:
    return Task(
        _id=1,
        _batch_id=10,
        status=TaskStatus.RUNNING,
        artifact={},
        filename="input_audio.wav",
        size=2048,
        language="pt-br",
    )


@pytest.fixture
def attempt() -> TaskAttempt:
    return TaskAttempt(
        _id=1,
        task_id=1,
        worker_id=uuid4(),
        status=TaskAttemptStatus.RUNNING,
    )


@pytest.fixture
def grpc_client() -> GrpcWorkerClient:
    return GrpcWorkerClient()


# ============================================================================
# Testes para request_processing (Etapa 1)
# ============================================================================


@pytest.mark.asyncio
@patch("src.transport.grpc.worker_client.grpc.aio.insecure_channel")
async def test_request_processing_accepted(
    mock_insecure_channel, grpc_client: GrpcWorkerClient, worker: Worker
):
    """Testa se o client retorna True quando o worker aceita o processamento."""
    mock_stub = AsyncMock()
    mock_stub.RequestProcessing.return_value = cw_pb2.ProcessRequestResponseProto(
        response=cw_pb2.PROCESS_RESPONSE_ACCEPTED
    )

    mock_channel = AsyncMock()
    mock_channel.__aenter__.return_value = mock_channel
    mock_insecure_channel.return_value = mock_channel

    with patch(
        "src.transport.grpc.worker_client.cw_pb2_grpc.WorkerNodeServiceStub",
        return_value=mock_stub,
    ):
        accepted = await grpc_client.request_processing(
            worker=worker,
            task_id=1,
            task_size=2048,
            language="pt-br",
            master_id=uuid4(),
        )

        assert accepted is True
        mock_stub.RequestProcessing.assert_awaited_once()


@pytest.mark.asyncio
@patch("src.transport.grpc.worker_client.grpc.aio.insecure_channel")
async def test_request_processing_rejected(
    mock_insecure_channel, grpc_client: GrpcWorkerClient, worker: Worker
):
    """Testa se o client retorna False quando o worker rejeita (ex: FULL)."""
    mock_stub = AsyncMock()
    mock_stub.RequestProcessing.return_value = cw_pb2.ProcessRequestResponseProto(
        response=cw_pb2.PROCESS_RESPONSE_REJECTED_FULL
    )

    mock_channel = AsyncMock()
    mock_channel.__aenter__.return_value = mock_channel
    mock_insecure_channel.return_value = mock_channel

    with patch(
        "src.transport.grpc.worker_client.cw_pb2_grpc.WorkerNodeServiceStub",
        return_value=mock_stub,
    ):
        accepted = await grpc_client.request_processing(
            worker=worker,
            task_id=1,
            task_size=2048,
            language="pt-br",
            master_id=uuid4(),
        )

        assert accepted is False


@pytest.mark.asyncio
@patch("src.transport.grpc.worker_client.grpc.aio.insecure_channel")
async def test_request_processing_grpc_error(
    mock_insecure_channel, grpc_client: GrpcWorkerClient, worker: Worker
):
    """Testa se o client lida com falhas de rede (AioRpcError) retornando False com segurança."""
    mock_stub = AsyncMock()
    # Simula uma queda de conexão levantando erro do gRPC
    mock_stub.RequestProcessing.side_effect = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=grpc.aio.Metadata(),
        trailing_metadata=grpc.aio.Metadata(),
        details="Worker offline",
    )

    mock_channel = AsyncMock()
    mock_channel.__aenter__.return_value = mock_channel
    mock_insecure_channel.return_value = mock_channel

    with patch(
        "src.transport.grpc.worker_client.cw_pb2_grpc.WorkerNodeServiceStub",
        return_value=mock_stub,
    ):
        accepted = await grpc_client.request_processing(
            worker=worker,
            task_id=1,
            task_size=2048,
            language="pt-br",
            master_id=uuid4(),
        )

        assert accepted is False


# ============================================================================
# Testes para execute_task (Etapa 2 - Streaming)
# ============================================================================


@pytest.mark.asyncio
@patch("src.transport.grpc.worker_client.grpc.aio.insecure_channel")
async def test_execute_task_success(
    mock_insecure_channel,
    grpc_client: GrpcWorkerClient,
    worker: Worker,
    task: Task,
    attempt: TaskAttempt,
    tmp_path,
):
    """Testa o envio de arquivo por streaming e o recebimento de sucesso do worker."""
    # Cria um arquivo de áudio temporário real para o gerador de chunks ler
    audio_file = tmp_path / "input_audio.wav"
    audio_file.write_bytes(b"dados binarios de audio simulados para teste do whisper")

    mock_stub = AsyncMock()
    mock_stub.ExecuteTask.return_value = cw_pb2.TaskResultsProto(
        task_id=1,
        attempt_id=1,
        final_status=cw_pb2.ATTEMPT_STATUS_SUCCESS,
        result_text="Olá mundo da transcrição",
        logs="Processado com GPU em 1.2s",
    )

    mock_channel = AsyncMock()
    mock_channel.__aenter__.return_value = mock_channel
    mock_insecure_channel.return_value = mock_channel

    with patch(
        "src.transport.grpc.worker_client.cw_pb2_grpc.WorkerNodeServiceStub",
        return_value=mock_stub,
    ):
        updated_attempt = await grpc_client.execute_task(
            worker=worker,
            task=task,
            attempt=attempt,
        )

        # Validações
        assert updated_attempt.status == TaskAttemptStatus.SUCCESS
        assert task.result_text == "Olá mundo da transcrição"
        assert updated_attempt.logs == "Processado com GPU em 1.2s"
        mock_stub.ExecuteTask.assert_awaited_once()


@pytest.mark.asyncio
@patch("src.transport.grpc.worker_client.grpc.aio.insecure_channel")
async def test_execute_task_deadline_exceeded(
    mock_insecure_channel,
    grpc_client: GrpcWorkerClient,
    worker: Worker,
    task: Task,
    attempt: TaskAttempt,
    tmp_path,
):
    """Testa o tratamento de timeout (DEADLINE_EXCEEDED) durante o processamento pesado."""
    audio_file = tmp_path / "input_audio.wav"
    audio_file.write_bytes(b"audio longo...")

    mock_stub = AsyncMock()
    mock_stub.ExecuteTask.side_effect = grpc.aio.AioRpcError(
        code=grpc.StatusCode.DEADLINE_EXCEEDED,
        initial_metadata=grpc.aio.Metadata(),
        trailing_metadata=grpc.aio.Metadata(),
        details="Tempo limite excedido",
    )

    mock_channel = AsyncMock()
    mock_channel.__aenter__.return_value = mock_channel
    mock_insecure_channel.return_value = mock_channel

    with patch(
        "src.transport.grpc.worker_client.cw_pb2_grpc.WorkerNodeServiceStub",
        return_value=mock_stub,
    ):
        updated_attempt = await grpc_client.execute_task(
            worker=worker,
            task=task,
            attempt=attempt,
        )

        # O status deve ser mapeado para TIMED_OUT conforme planejado
        assert updated_attempt.status == TaskAttemptStatus.TIMED_OUT
        assert updated_attempt.logs != None
        assert "DEADLINE_EXCEEDED" in updated_attempt.logs
