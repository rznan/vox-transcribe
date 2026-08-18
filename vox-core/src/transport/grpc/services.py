import grpc
from src.transport.grpc.generated import task_service_pb2 as pb2
from src.transport.grpc.generated import task_service_pb2_grpc as pb2_grpc
from src.transport.grpc.mappers import (
    cancel_batch_domain_to_response,
    list_batches_domain_to_proto,
    submit_batch_request_to_domain,
    batch_domain_to_submit_response,
    task_domain_to_status_response,
)
from src.domain.interfaces.services import TaskService


class TaskGrpcServiceImpl(pb2_grpc.TaskGrpcServiceServicer):

    def __init__(self, task_service: TaskService) -> None:
        self.task_service = task_service

    async def SubmitBatch(
        self,
        request: pb2.SubmitBatchRequestProto,
        context: grpc.aio.ServicerContext,
    ) -> pb2.SubmitBatchResponseProto:
        requested_batch = submit_batch_request_to_domain(request)
        resulting_batch = await self.task_service.create_batch(requested_batch)
        return batch_domain_to_submit_response(resulting_batch)

    async def GetTaskStatus(
        self, request: pb2.GetTaskStatusRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.GetTaskStatusResponseProto:
        task = await self.task_service.get_by_id(request.task_id)
        if not task:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"Task {request.task_id} não encontrada."
            )

        return task_domain_to_status_response(task)

    async def ListBatches(
        self, request: pb2.ListBatchesRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.ListBatchesResponseProto:

        batches = await self.task_service.list_batches(request.limit, request.offset)
        return list_batches_domain_to_proto(batches)

    async def CancelBatch(
        self, request: pb2.CancelBatchRequestProto, context: grpc.aio.ServicerContext
    ) -> pb2.CancelBatchResponseProto:
        batch = await self.task_service.get_by_id(request.batch_id)
        if not batch:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"Batch {request.batch_id} não encontrado."
            )

        cancelled_batch = await self.task_service.cancel_batch(request.batch_id)
        return cancel_batch_domain_to_response(cancelled_batch)
