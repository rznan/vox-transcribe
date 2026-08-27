from uuid import uuid4

import grpc
import pytest

from unittest.mock import AsyncMock, create_autospec, MagicMock
from datetime import datetime, timezone

from src.domain.value_objects.enums import TaskAttemptStatus, TaskStatus
from src.domain.interfaces.services import TaskService
from src.transport.grpc.services import TaskGrpcServiceImpl
from src.transport.grpc.generated import task_service_pb2 as pb2
from src.domain.entities import Task, TaskAttempt, Batch


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


# Exceção sentinela para simular a parada do contexto
class MockRpcError(Exception):
    pass


class TestTaskGrpcServiceImpl:

    @pytest.fixture
    def mock_task_service(self):
        """Cria um mock estrito baseado na interface TaskService."""
        return create_autospec(TaskService, instance=True)

    @pytest.fixture
    def servicer(self, mock_task_service):
        """Instancia o Servicer de Tasks injetando o mock do serviço."""
        return TaskGrpcServiceImpl(task_service=mock_task_service)

    @pytest.fixture
    def mock_context(self):
        """Mock do contexto gRPC para capturar códigos de status e detalhes."""
        context = MagicMock(spec=grpc.aio.ServicerContext)
        context.abort = AsyncMock(side_effect=MockRpcError)
        return context

    # ==========================================
    # GetBatch
    # ==========================================

    @pytest.mark.asyncio
    async def test_get_batch_with_data(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        sample_batch,
        mock_context,
    ):
        mock_task_service.get_batch.return_value = sample_batch
        request = pb2.GetBatchRequestProto(batch_id=sample_batch.id)

        response = await servicer.GetBatch(request, mock_context)

        assert response.batch.id == sample_batch.id
        assert response.batch.total_jobs_number == 1

    @pytest.mark.asyncio
    async def test_get_batch_empty(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        mock_context,
    ):
        id = 999
        mock_task_service.get_batch.return_value = None
        request = pb2.GetBatchRequestProto(batch_id=id)

        with pytest.raises(MockRpcError):
            await servicer.GetBatch(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.NOT_FOUND, f"Batch {id} não encontrado."
        )

    # ==========================================
    # SubmitBatch
    # ==========================================

    @pytest.mark.asyncio
    async def test_submit_batch_success(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        sample_batch,
        mock_context,
    ):
        request = pb2.SubmitBatchRequestProto(
            tasks=[
                pb2.SubmitTaskRequestProto(
                    artifact_json='{"cmd": "run"}', filename="test.txt", size=1024
                )
            ]
        )
        mock_task_service.create_batch.return_value = sample_batch

        response = await servicer.SubmitBatch(request, mock_context)

        assert response.batch_id == sample_batch.id

        mock_task_service.create_batch.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_submit_batch_value_error(
        self, servicer: TaskGrpcServiceImpl, mock_task_service, mock_context
    ):
        request = pb2.SubmitBatchRequestProto()
        # Força o mock a levantar um ValueError para simular dados inválidos
        mock_task_service.create_batch.side_effect = ValueError(
            "Dados inválidos no batch"
        )

        with pytest.raises(MockRpcError):
            await servicer.SubmitBatch(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INVALID_ARGUMENT, "Dados inválidos no batch"
        )

    @pytest.mark.asyncio
    async def test_submit_batch_internal_error(
        self, servicer: TaskGrpcServiceImpl, mock_task_service, mock_context
    ):
        request = pb2.SubmitBatchRequestProto()
        # Força o mock a levantar uma exceção genérica
        mock_task_service.create_batch.side_effect = Exception(
            "Falha catastrófica no banco"
        )

        with pytest.raises(MockRpcError):
            await servicer.SubmitBatch(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.INTERNAL, "Erro interno ao processar a submissão do lote."
        )

    # ==========================================
    # GetTask
    # ==========================================

    @pytest.mark.asyncio
    async def test_get_task_exists(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        sample_task,
        mock_context,
    ):
        mock_task_service.get_task.return_value = sample_task
        request = pb2.GetTaskRequestProto(task_id=sample_task.id)

        response = await servicer.GetTask(request, mock_context)

        assert response.task.id == sample_task.id
        assert response.task.filename == sample_task.filename

    @pytest.mark.asyncio
    async def test_get_task_not_exists(
        self, servicer: TaskGrpcServiceImpl, mock_task_service, mock_context
    ):
        mock_task_service.get_task.return_value = None
        request = pb2.GetTaskRequestProto(task_id=999)

        with pytest.raises(MockRpcError):
            await servicer.GetTask(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.NOT_FOUND, "Task 999 não encontrada."
        )

    # ==========================================
    # ListBatches
    # ==========================================

    @pytest.mark.asyncio
    async def test_list_batches_with_data(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        sample_batch,
        mock_context,
    ):
        mock_task_service.list_batches.return_value = [sample_batch]
        request = pb2.ListBatchesRequestProto(limit=10, offset=0)

        response = await servicer.ListBatches(request, mock_context)

        assert len(response.batches) == 1
        assert response.batches[0].id == sample_batch.id
        mock_task_service.list_batches.assert_awaited_once_with(limit=10, offset=0)

    @pytest.mark.asyncio
    async def test_list_batches_empty(
        self, servicer: TaskGrpcServiceImpl, mock_task_service, mock_context
    ):
        mock_task_service.list_batches.return_value = []
        request = pb2.ListBatchesRequestProto(limit=10, offset=0)

        response = await servicer.ListBatches(request, mock_context)

        assert len(response.batches) == 0

    # ==========================================
    # CancelBatch
    # ==========================================

    @pytest.mark.asyncio
    async def test_cancel_batch_exists(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        sample_batch,
        mock_context,
    ):
        mock_task_service.get_batch.return_value = sample_batch
        mock_task_service.cancel_batch.return_value = sample_batch
        request = pb2.CancelBatchRequestProto(batch_id=sample_batch.id)

        response = await servicer.CancelBatch(request, mock_context)

        assert response.batch.id == sample_batch.id
        mock_task_service.cancel_batch.assert_awaited_once_with(sample_batch.id)

    @pytest.mark.asyncio
    async def test_cancel_batch_not_exists(
        self, servicer: TaskGrpcServiceImpl, mock_task_service, mock_context
    ):
        mock_task_service.get_batch.return_value = None
        request = pb2.CancelBatchRequestProto(batch_id=999)

        with pytest.raises(MockRpcError):
            await servicer.CancelBatch(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.NOT_FOUND, "Batch 999 não encontrado."
        )

    # ==========================================
    # DeleteBatch
    # ==========================================

    @pytest.mark.asyncio
    async def test_delete_batch_exists(
        self,
        servicer: TaskGrpcServiceImpl,
        mock_task_service,
        sample_batch,
        mock_context,
    ):
        mock_task_service.get_batch.return_value = sample_batch
        request = pb2.DeleteBatchRequestProto(batch_id=sample_batch.id)

        response = await servicer.DeleteBatch(request, mock_context)

        assert response.success is True
        mock_task_service.delete_batch.assert_awaited_once_with(sample_batch.id)

    @pytest.mark.asyncio
    async def test_delete_batch_not_exists(
        self, servicer: TaskGrpcServiceImpl, mock_task_service, mock_context
    ):
        mock_task_service.get_batch.return_value = None
        request = pb2.DeleteBatchRequestProto(batch_id=999)

        with pytest.raises(MockRpcError):
            await servicer.DeleteBatch(request, mock_context)

        mock_context.abort.assert_awaited_once_with(
            grpc.StatusCode.NOT_FOUND, "Batch 999 não encontrado para exclusão."
        )
