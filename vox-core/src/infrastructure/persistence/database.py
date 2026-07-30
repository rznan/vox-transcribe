import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sqlalchemy import URL

from src.infrastructure.persistence.models import Base


class DatabaseConfig:
    """Configurações de conexão com o banco postgresql."""

    def __init__(self) -> None:

        # TODO: adicionar no .env
        driver = os.getenv("DB_DRIVER", "postgresql+asyncpg")
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        database_name = os.getenv("DB_DATABASE_NAME")
        host = os.getenv("DB_HOST")

        self.database_url = URL.create(
            driver,
            username=username,
            password=password,
            host=host,
            database=database_name,
        )


class Database:
    """Encapsula a engine e a fabrica de sessões assíncronas do SQLAlchemy"""

    def __init__(self, config: DatabaseConfig | None = None) -> None:
        self.config = config or DatabaseConfig()

        self.engine: AsyncEngine = create_async_engine(
            self.config.database_url,
            echo=False,
            pool_size=10,
            pool_pre_ping=True,
        )

        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )

    async def create_tables(self) -> None:
        """Cria todas as tabelas registradas no Mapped Base (ideal para dev/testes)."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self) -> None:
        """Remove todas as tabelas (utilizado principalmente em suítes de teste)."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Gerenciador de contexto para criar e fechar a sessão com segurança.

        Realiza commit automático se nenhum erro ocorrer, ou rollback caso haja exceção.
        """
        async_session: AsyncSession = self.session_factory()
        try:
            yield async_session
            await async_session.commit()
        except Exception:
            await async_session.rollback()
            raise
        finally:
            await async_session.close()

    async def close(self) -> None:
        """Fecha o pool de conexões do banco de dados ao desligar a aplicação."""
        await self.engine.dispose()


db = Database()
