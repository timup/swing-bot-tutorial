import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

# Construct the database URL
# Defaulting to placeholder values if environment variables are not set
# For production/deployment, these should be set via environment variables.
DB_USER = os.getenv("POSTGRES_USER", "swing_bot_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "swing_bot_db")

DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Create the SQLAlchemy async engine
# echo=True logs SQL queries, useful for development, disable for production
echo_sql = os.getenv("ECHO_SQL", "True").lower() == "true"
engine = create_async_engine(DATABASE_URL, echo=echo_sql)

# Create a configured "Session" class
# expire_on_commit=False prevents attributes from being expired after commit.
AsyncSessionFactory = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency that provides an async database session."""
    async with AsyncSessionFactory() as session:
        try:
            yield session
            # Optional: commit here if you want auto-commit per request/usage
            # await session.commit()
        except Exception:
            # Optional: rollback on exception
            # await session.rollback()
            raise
        # finally:
        # Session is automatically closed by the context manager
        # await session.close() # Not needed with async context manager


# Optional: A function to create tables initially without Alembic (for testing/dev)
# Should generally rely on Alembic migrations for schema management.
# from .db_models import Base
# async def create_db_and_tables():
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all) # Use with caution!
#         await conn.run_sync(Base.metadata.create_all)
#     await engine.dispose()
