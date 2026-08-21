import logging

import grpc

from src.domain.interfaces.services import TaskService
from src.transport.grpc.generated import task_service_pb2 as pb2
from src.transport.grpc.generated import task_service_pb2_grpc as pb2_grpc
import src.transport.grpc.mappers as grpc_mappers

logger = logging.getLogger(__name__)


class TaskGrpcServiceImpl(pb2_grpc.TaskGrpcServiceServicer):
    """
    Implementação da camada de transporte gRPC.
    """

    def __init__(self, task_service: TaskService) -> None:
        self.task_service = task_service

    async def SubmitBatch(
        self,
        request: pb2.SubmitBatchRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> pb2.SubmitBatchResponseProto:
        try:
            requested_batch = grpc_mappers.submit_batch_request_to_domain(request)
            resulting_batch = await self.task_service.create_batch(requested_batch)
            return grpc_mappers.batch_domain_to_submit_response(resulting_batch)

        except ValueError as e:
            logger.warning(f"Erro de validação ao submeter batch: {e}")
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))
        except Exception as e:
            logger.error(f"Erro interno ao submeter batch: {e}", exc_info=True)
            await context.abort(
                grpc.StatusCode.INTERNAL,
                "Erro interno ao processar a submissão do lote.",
            )

    async def GetBatch(
        self, request: pb2.GetBatchRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.GetBatchResponseProto:
        batch = await self.task_service.get_batch(request.batch_id)
        if not batch:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"Batch {request.batch_id} não encontrado."
            )

        return pb2.GetBatchResponseProto(
            batch=grpc_mappers.batch_domain_to_full_proto(batch)
        )

    async def GetTask(
        self, request: pb2.GetTaskRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.GetTaskResponseProto:
        task = await self.task_service.get_task(request.task_id)
        if not task:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"Task {request.task_id} não encontrada."
            )

        return pb2.GetTaskResponseProto(
            task=grpc_mappers.task_domain_to_full_proto(task)
        )

    async def ListBatches(
        self, request: pb2.ListBatchesRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.ListBatchesResponseProto:
        batches = await self.task_service.list_batches(
            limit=request.limit, offset=request.offset
        )
        return grpc_mappers.list_batches_domain_to_proto(batches)

    async def CancelBatch(
        self, request: pb2.CancelBatchRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.CancelBatchResponseProto:
        batch = await self.task_service.get_batch(request.batch_id)
        if not batch:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"Batch {request.batch_id} não encontrado."
            )

        cancelled_batch = await self.task_service.cancel_batch(request.batch_id)
        return pb2.CancelBatchResponseProto(
            batch=grpc_mappers.batch_domain_to_lite_proto(cancelled_batch)
        )

    async def DeleteBatch(
        self, request: pb2.DeleteBatchRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.DeleteBatchResponseProto:
        batch = await self.task_service.get_batch(request.batch_id)
        if not batch:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Batch {request.batch_id} não encontrado para exclusão.",
            )

        await self.task_service.delete_batch(request.batch_id)
        return pb2.DeleteBatchResponseProto(success=True)
