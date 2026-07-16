from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Generic, TypeVar

# entidade de domínio
T = TypeVar("T")
# id da entidade de domínio
ID = TypeVar("ID")


class IRepository(ABC, Generic[T, ID]):

    @abstractmethod
    def add(self, entity: T) -> T:
        """Adiona uma nova entidade na persistencia"""
        pass

    @abstractmethod
    def delete(self, id: ID) -> bool:
        """Remove uma entidade pelo ID"""
        pass

    @abstractmethod
    def update(self, entity: T) -> T:
        """Atualiza o estado da entidade existente"""
        pass

    @abstractmethod
    def get_by_id(self, id: ID) -> T | None:
        """Busca uma entidade pelo ID"""
        pass

    @abstractmethod
    def list_all(self, limit: int = 100, offset: int = 0) -> Sequence[T]:
        """Lista as entidades com filtro básico"""
        pass
