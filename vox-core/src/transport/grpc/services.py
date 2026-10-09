import re
import uuid
import functools
import logging
from typing import Callable, Any

import grpc

import src.transport.grpc.mappers as grpc_mappers
from src.domain.interfaces.services import TaskService, WorkerService
from src.domain.entities import Batch, Task

from src.transport.grpc.generated import (
    core_worker_internal_communication_pb2 as cw_pb2,
)
from src.transport.grpc.generated import (
    core_worker_internal_communication_pb2_grpc as cw_pb2_grpc,
)
from src.transport.grpc.generated import task_service_pb2 as task_pb2
from src.transport.grpc.generated import task_service_pb2_grpc as task_pb2_grpc

logger = logging.getLogger(__name__)

def handle_grpc_errors(action_description: str) -> Callable:
    """
    Decorador para centralizar o tratamento de exceções nos métodos gRPC,
    mapeando exceções do Python para os status do gRPC apropriados.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(self: Any, request: Any, context: grpc.aio.ServicerContext, *args: Any, **kwargs: Any) -> Any:
            try:
                return await func(self, request, context, *args, **kwargs)
            except PermissionError as e:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(e))
            except ValueError as e:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
            except LookupError as e:
                await context.abort(grpc.StatusCode.NOT_FOUND, str(e))
            except Exception as e:
                logger.error(f"Erro interno ao {action_description}: {e}", exc_info=True)
                await context.abort(
                    grpc.StatusCode.INTERNAL,
                    f"Erro interno ao {action_description}.",
                )
        return wrapper
    return decorator


class MasterNodeServiceImpl(cw_pb2_grpc.MasterNodeServiceServicer):
    """
    Implementação da camada de transporte gRPC para o servidor de workers.
    """

    def __init__(self, worker_service: WorkerService, master_id: uuid.UUID) -> None:
        self.master_id = master_id
        self.worker_service = worker_service

    def _verify_master_id(self, request_master_id: str) -> None:
        """Valida se o master_id da requisição corresponde ao ID deste Master Node."""
        if request_master_id != str(self.master_id):
            raise PermissionError("master_id does not match")

    def _parse_uuid(self, uuid_str: str, param_name: str = "worker_id") -> uuid.UUID:
        """Converte uma string para UUID ou lança ValueError padronizado."""
        try:
            return uuid.UUID(uuid_str)
        except (ValueError, TypeError, AttributeError):
            raise ValueError(f"{param_name} is not a valid UUID")

    @handle_grpc_errors("processar solicitação de entrada do worker")
    async def RegisterWorker(
        self,
        request: cw_pb2.WorkerEntryRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> cw_pb2.WorkerEntryResponseProto:
        worker = grpc_mappers.worker_entry_request_to_domain(request)
        result = await self.worker_service.register_worker(worker)
        return grpc_mappers.connection_status_to_proto(result, worker.id, self.master_id)

    @handle_grpc_errors("processar o sinal de vida do worker")
    async def SendHeartbeat(
        self,
        request: cw_pb2.HeartbeatProto,
        context: grpc.aio.ServicerContext,
    ) -> cw_pb2.HeartbeatResponseProto:
        self._verify_master_id(request.master_id)
        worker_id = self._parse_uuid(request.worker_id)

        result = await self.worker_service.compute_heartbeat(worker_id)
        return grpc_mappers.heartbeat_response_to_proto(result)

    @handle_grpc_errors("desligar o worker")
    async def ShutdownWorker(
        self,
        request: cw_pb2.WorkerShutdownProto,
        context: grpc.aio.ServicerContext,
    ) -> cw_pb2.WorkerShutdownResponseProto:
        self._verify_master_id(request.master_id)
        worker_id = self._parse_uuid(request.worker_id)

        result = await self.worker_service.shutdown_worker(worker_id)
        return grpc_mappers.shutdown_response_to_proto(result)

class TaskGrpcServiceImpl(task_pb2_grpc.TaskGrpcServiceServicer):
    """
    Implementação da camada de transporte gRPC para o servidor de tarefas.
    """

    def __init__(self, task_service: TaskService) -> None:
        self.task_service = task_service

    def _verify_batch_not_None(self, batch: Batch | None, requested_batch_id) -> Batch:
        if not batch:
            raise LookupError(f"Batch {requested_batch_id} não encontrado.")
        return batch

    def _verify_task_not_None(self, task: Task | None, requested_task_id) -> Task:
        if not task:
            raise LookupError(f"Task {requested_task_id} não encontrada.")
        return task

    @handle_grpc_errors("submeter o batch")
    async def SubmitBatch(
        self,
        request: task_pb2.SubmitBatchRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> task_pb2.SubmitBatchResponseProto:
        requested_batch = grpc_mappers.submit_batch_request_to_domain(request)
        resulting_batch = await self.task_service.create_batch(requested_batch)
        return grpc_mappers.batch_domain_to_submit_response(resulting_batch)


    @handle_grpc_errors("buscar o batch")
    async def GetBatch(
        self, request: task_pb2.GetBatchRequestProto, context: grpc.aio.ServicerContext
    ) -> task_pb2.GetBatchResponseProto:
        batch = await self.task_service.get_batch(request.batch_id)
        batch = self._verify_batch_not_None(batch, request.batch_id)

        return task_pb2.GetBatchResponseProto(
            batch=grpc_mappers.batch_domain_to_full_proto(batch)
        )

    @handle_grpc_errors("buscar a task")
    async def GetTask(
        self, request: task_pb2.GetTaskRequestProto, context: grpc.aio.ServicerContext
    ) -> task_pb2.GetTaskResponseProto:
        task = await self.task_service.get_task(request.task_id)
        task = self._verify_task_not_None(task, request.task_id)

        return task_pb2.GetTaskResponseProto(
            task=grpc_mappers.task_domain_to_full_proto(task)
        )

    @handle_grpc_errors("listar os batches")
    async def ListBatches(
        self,
        request: task_pb2.ListBatchesRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> task_pb2.ListBatchesResponseProto:
        batches = await self.task_service.list_batches(
            limit=request.limit, offset=request.offset
        )
        return grpc_mappers.list_batches_domain_to_proto(batches)

    @handle_grpc_errors("cancelar o batch")
    async def CancelBatch(
        self,
        request: task_pb2.CancelBatchRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> task_pb2.CancelBatchResponseProto:
        batch = await self.task_service.get_batch(request.batch_id)
        batch = self._verify_batch_not_None(batch, request.batch_id)

        cancelled_batch = await self.task_service.cancel_batch(request.batch_id)
        return task_pb2.CancelBatchResponseProto(
            batch=grpc_mappers.batch_domain_to_lite_proto(cancelled_batch)
        )

    @handle_grpc_errors("deletar o batch")
    async def DeleteBatch(
        self,
        request: task_pb2.DeleteBatchRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> task_pb2.DeleteBatchResponseProto:
        batch = await self.task_service.get_batch(request.batch_id)
        batch = self._verify_batch_not_None(batch, request.batch_id)

        await self.task_service.delete_batch(request.batch_id)
        return task_pb2.DeleteBatchResponseProto(success=True)
