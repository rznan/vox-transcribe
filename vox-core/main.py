import asyncio
from src.domain.value_objects.enums import TaskStatus
from src.infrastructure.persistence.database import db
from src.infrastructure.persistence.repositories import BatchRepository, TaskRepository


async def main() -> None:
    async with db.session() as session:
        repo = BatchRepository(session)
        batch = await repo.get_with_tasks("batch-01")
        print("----w/tasks-----")
        print(batch)
        batch = await repo.get_by_id("batch-01")
        print("----wo/tasks----")
        print(batch)
        print()


if __name__ == "__main__":
    asyncio.run(main())
