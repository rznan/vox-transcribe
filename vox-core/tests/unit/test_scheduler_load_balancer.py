import pytest
from uuid import uuid4

from src.domain.entities import Worker
from src.domain.value_objects.enums import WorkerRuntime, WorkerStatus
from src.application.scheduler.load_balancer import (
    RoundRobinLoadBalancer,
    WorkerIdNotRegisteredError,
)


@pytest.fixture
def sample_worker() -> Worker:
    """Fixture que retorna um worker padrão para os testes."""
    return Worker(
        id=uuid4(),
        name="Worker Alpha",
        ip="127.0.0.1",
        port=8080,
        runtime=WorkerRuntime.GPU,
        simultaneous_capacity=2,
        status=WorkerStatus.IDLE,
    )


@pytest.fixture
def sample_worker_list(sample_worker: Worker) -> list[Worker]:
    """Fixture que retorna uma lista com múltiplos workers."""
    return [
        sample_worker,
        Worker(
            id=uuid4(),
            name="Worker Beta",
            ip="127.0.0.1",
            port=8081,
            runtime=WorkerRuntime.CPU,
            simultaneous_capacity=2,
            status=WorkerStatus.IDLE,
        ),
    ]


@pytest.fixture
def balancer() -> RoundRobinLoadBalancer:
    """Fixture que inicializa o balanceador limpo para cada teste."""
    return RoundRobinLoadBalancer()


def test_register_new_worker(balancer: RoundRobinLoadBalancer, sample_worker: Worker):
    """Testa se inserir um worker novo o adiciona nas estruturas corretas."""
    balancer.register(sample_worker)

    assert sample_worker.id in balancer.workers
    assert sample_worker.id in balancer.worker_id_deque


def test_register_existing_worker_does_not_duplicate(
    balancer: RoundRobinLoadBalancer, sample_worker: Worker
):
    """Testa se inserir um worker que já está presente não duplica no deque."""
    balancer.register(sample_worker)
    balancer.register(sample_worker)  # Chamada duplicada proposital

    assert len(balancer.workers) == 1
    assert list(balancer.worker_id_deque).count(sample_worker.id) == 1


def test_register_worker_in_removal_set(
    balancer: RoundRobinLoadBalancer, sample_worker_list: list[Worker]
):
    """Testa se inserir um worker que está no set de remoção o retira do set."""
    w1, w2 = sample_worker_list
    balancer.register(w1)
    balancer.register(w2)

    # Ao remover w2 (que não é o índice 0 da fila), ele vai para o set de remoção
    balancer.unregister(w2.id)
    assert w2.id in balancer.to_be_removed_worker_id_set

    # Ao registrar novamente, ele deve sair do set de remoção
    balancer.register(w2)
    assert w2.id not in balancer.to_be_removed_worker_id_set


def test_unregister_worker_at_top(
    balancer: RoundRobinLoadBalancer, sample_worker: Worker
):
    """Testa se remover um worker no topo do deque o remove imediatamente."""
    balancer.register(sample_worker)
    balancer.unregister(sample_worker.id)

    assert sample_worker.id not in balancer.workers
    assert len(balancer.worker_id_deque) == 0


def test_unregister_unregistered_worker_raises_error(balancer: RoundRobinLoadBalancer):
    """Testa se remover um worker que não está presente sobe exceção WorkerIdNotRegisteredError."""
    with pytest.raises(WorkerIdNotRegisteredError):
        balancer.unregister(uuid4())


def test_unregister_worker_not_at_top_removes_later(
    balancer: RoundRobinLoadBalancer, sample_worker_list: list[Worker]
):
    """Testa se remover worker não no topo do deque o marca para remoção futura."""
    w1, w2 = sample_worker_list
    balancer.register(w1)
    balancer.register(w2)

    # w2 está na posição 1
    balancer.unregister(w2.id)

    # Ele ainda deve estar na fila, mas marcado para remover e fora do dict principal
    assert w2.id not in balancer.workers
    assert w2.id in balancer.worker_id_deque
    assert w2.id in balancer.to_be_removed_worker_id_set

    # Simulamos o balanceador rodando até o w2 chegar no topo.
    # Quando o get_worker for chamado, ele deve limpar o w2.
    balancer.get_worker()
    balancer.get_worker()

    assert w2.id not in balancer.worker_id_deque
    assert w2.id not in balancer.to_be_removed_worker_id_set


def test_get_worker_idle(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
):
    """Testa pegar worker vazio/disponível."""
    balancer.register(sample_worker)
    sample_worker.current_workload = 0

    worker = balancer.get_worker()
    assert worker == sample_worker


def test_get_worker_skips_removed_until_none(
    balancer: RoundRobinLoadBalancer, sample_worker_list: list[Worker]
):
    """Testa pegar um worker marcado para remover busca o próximo até retornar None."""
    w1, w2 = sample_worker_list
    balancer.register(w1)
    balancer.register(w2)

    balancer.unregister(w2.id)
    balancer.unregister(w1.id)

    worker = balancer.get_worker()

    assert worker is None
    assert len(balancer.worker_id_deque) == 0


def test_get_worker_full_removes_from_deque(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
):
    """Testa se pegar um worker cheio o remove do deque e adiciona no set de cheio."""
    balancer.register(sample_worker)
    sample_worker.current_workload = sample_worker.simultaneous_capacity

    worker = balancer.get_worker()

    assert worker is None
    assert sample_worker.id not in balancer.worker_id_deque
    assert sample_worker.id in balancer.full_worker_id_set


def test_mark_worker_as_available_inserts_in_deque(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
):
    """Testa marcar worker disponível insere-o na fila se ele estiver realmente disponível."""
    balancer.register(sample_worker)

    # Simulando o worker estando no estado 'cheio'
    sample_worker.current_workload = sample_worker.simultaneous_capacity
    balancer.get_worker()

    # Faz o worker se tornar disponível
    sample_worker.current_workload -= 1
    balancer.mark_worker_as_available(sample_worker.id)

    assert sample_worker.id in balancer.worker_id_deque
    assert sample_worker.id not in balancer.full_worker_id_set


def test_mark_worker_as_available_ignores_if_full(
    balancer: RoundRobinLoadBalancer,
    sample_worker: Worker,
):
    """Testa marcar worker disponível não insere-o na fila se ele não estiver disponível."""
    balancer.register(sample_worker)

    balancer.worker_id_deque.remove(sample_worker.id)
    balancer.full_worker_id_set.add(sample_worker.id)

    sample_worker.current_workload = sample_worker.simultaneous_capacity
    balancer.mark_worker_as_available(sample_worker.id)

    assert sample_worker.id not in balancer.worker_id_deque
    assert sample_worker.id in balancer.full_worker_id_set


def test_mark_worker_as_available_raises_error_if_unregistered(
    balancer: RoundRobinLoadBalancer,
):
    """Testa marcar um worker disponível sobe exceção caso não estiver no balanceador."""
    with pytest.raises(WorkerIdNotRegisteredError):
        balancer.mark_worker_as_available(uuid4())
