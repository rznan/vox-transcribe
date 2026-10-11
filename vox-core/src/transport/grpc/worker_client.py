import logging
import os
from uuid import UUID

import grpc

from src.domain.entities import (
    Task,
    TaskAttempt,
    TaskAttemptStatus,
    Worker,
)
from src.transport.grpc.generated import (
    core_worker_internal_communication_pb2 as cw_pb2,
)
from src.transport.grpc.generated import (
    core_worker_internal_communication_pb2_grpc as cw_pb2_grpc,
)


logger = logging.getLogger(__name__)

# Tamanho máximo de cada bloco de áudio enviado pelo stream: 1 MiB.
CHUNK_SIZE = 1024 * 1024

# Timeout das chamadas gRPC, em segundos.
PROCESSING_REQUEST_TIMEOUT = 5.0
TASK_EXECUTION_TIMEOUT = 1800.0

STATUS_PROTO_TO_DOMAIN_ATTEMPT = {
    cw_pb2.ATTEMPT_STATUS_PENDING: TaskAttemptStatus.PENDING,
    cw_pb2.ATTEMPT_STATUS_RUNNING: TaskAttemptStatus.RUNNING,
    cw_pb2.ATTEMPT_STATUS_SUCCESS: TaskAttemptStatus.SUCCESS,
    cw_pb2.ATTEMPT_STATUS_FAILED: TaskAttemptStatus.FAILED,
    cw_pb2.ATTEMPT_STATUS_TIMED_OUT: TaskAttemptStatus.TIMED_OUT,
    cw_pb2.ATTEMPT_STATUS_CANCELLED: TaskAttemptStatus.CANCELLED,
}


class GrpcWorkerClient:
    """Cliente gRPC do Master para comunicação com os Workers."""

    @staticmethod
    def _get_target(worker: Worker) -> str:
        """Retorna o endereço gRPC do Worker."""
        return f"{worker.ip}:{worker.port}"

    async def request_processing(
        self,
        worker: Worker,
        task_id: int,
        task_size: int,
        language: str,
        master_id: UUID,
    ) -> bool:
        """
        Verifica se o Worker aceita executar uma tarefa.

        Retorna True se a solicitação for aceita e False caso contrário.
        """
        target = self._get_target(worker)

        request = cw_pb2.ProcessRequestProto(
            master_id=str(master_id),
            task_id=task_id,
            task_size=task_size,
            language=language,
        )

        try:
            async with grpc.aio.insecure_channel(target) as channel:
                stub = cw_pb2_grpc.WorkerNodeServiceStub(channel)

                response: cw_pb2.ProcessRequestResponseProto = (
                    await stub.RequestProcessing(
                        request,
                        timeout=PROCESSING_REQUEST_TIMEOUT,
                    )
                )

                return (
                    response.response
                    == cw_pb2.PROCESS_RESPONSE_ACCEPTED
                )

        except grpc.aio.AioRpcError as rpc_error:
            logger.warning(
                "Falha na solicitação de processamento ao Worker %s: "
                "código=%s, detalhes=%s",
                worker.id,
                rpc_error.code(),
                rpc_error.details(),
            )
            return False

        except Exception:
            logger.exception(
                "Erro inesperado ao solicitar processamento ao Worker %s.",
                worker.id,
            )
            return False

    async def execute_task(
        self,
        worker: Worker,
        task: Task,
        attempt: TaskAttempt,
    ) -> TaskAttempt:
        """
        Transmite o arquivo de áudio ao Worker e registra o resultado
        da execução na tentativa e na tarefa.
        """
        target = self._get_target(worker)

        async def _generate_audio_chunks():
            """Produz mensagens gRPC com o áudio dividido em blocos."""
            filename = os.path.basename(task.filename)
            file_size = os.path.getsize(task.filename)

            with open(task.filename, "rb") as audio_file:
                while chunk := audio_file.read(CHUNK_SIZE):
                    yield cw_pb2.TaskDataProto(
                        task_id=task.id,
                        worker_id=str(worker.id),
                        attempt_id=attempt.id or 0,
                        filename=filename,
                        size=file_size,
                        audio_chunk=chunk,
                    )

        try:
            async with grpc.aio.insecure_channel(target) as channel:
                stub = cw_pb2_grpc.WorkerNodeServiceStub(channel)

                response: cw_pb2.TaskResultsProto = (
                    await stub.ExecuteTask(
                        _generate_audio_chunks(),
                        timeout=TASK_EXECUTION_TIMEOUT,
                    )
                )

            attempt.status = STATUS_PROTO_TO_DOMAIN_ATTEMPT.get(
                response.final_status,
                TaskAttemptStatus.FAILED,
            )
            attempt.logs = response.logs
            task.result_text = response.result_text

        except grpc.aio.AioRpcError as rpc_error:
            attempt.status = (
                TaskAttemptStatus.TIMED_OUT
                if rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                else TaskAttemptStatus.FAILED
            )

            attempt.logs = (
                f"Erro de RPC no Worker ({rpc_error.code()}): "
                f"{rpc_error.details()}"
            )

            logger.error("%s", attempt.logs)

        except Exception as exc:
            attempt.status = TaskAttemptStatus.FAILED
            attempt.logs = (
                "Erro interno no cliente Master ao despachar tarefa: "
                f"{exc}"
            )

            logger.exception("%s", attempt.logs)

        return attempt
