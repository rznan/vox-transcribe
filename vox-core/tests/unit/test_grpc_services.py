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

    @pytest.mark.asyncio
    async def test_get_batch_with_available_batch(
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
    async def test_get_batch_with_no_available_batch(
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

    # Test Cases para Implementar:
    # SubmitBatch:
    #   1. dados corretos passam corretamente
    #   2. algum dado errado para testar valueError
    #   3. lançar exceção para testar um erro interno ao submter
    # GetBatch:
    #   1. batch existe                                             [ x ]
    #   2. batch não existe                                         [ x ]
    # GetTask:
    #   1. Task existe
    #   2. Task não existe
    # ListBatches:
    #   1. com batches existentes
    #   2. sem batches existentes
    #   5. offset out of range
    #   4. limite inválido
    # CancelBatch:
    #   1. batch existe
    #   2. batch não existe
    # DeleteBatch:
    #   1. batch existe
    #   2. batch não existe
