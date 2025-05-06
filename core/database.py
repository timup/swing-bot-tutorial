import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from core.config import Config
from core.logging_config import get_logger

# Set up logging
logger = get_logger(__name__)

# Check for required database environment variables
db_env_vars = {
    "POSTGRES_USER": Config.DB_USER,
    "POSTGRES_PASSWORD": Config.DB_PASSWORD,
    "POSTGRES_HOST": Config.DB_HOST,
    "POSTGRES_PORT": Config.DB_PORT,
    "POSTGRES_DB": Config.DB_NAME,
}

# Only use fallback values in development, not production
if not Config.is_production():
    # Provide fallback values for development only
    if not db_env_vars["POSTGRES_USER"]:
        logger.warning("POSTGRES_USER not set. Using development fallback 'swing_bot_user'")
        db_env_vars["POSTGRES_USER"] = "swing_bot_user"
    
    if not db_env_vars["POSTGRES_PASSWORD"]:
        logger.warning("POSTGRES_PASSWORD not set. Using insecure development fallback")
        db_env_vars["POSTGRES_PASSWORD"] = "development_password_only"
    
    if not db_env_vars["POSTGRES_DB"]:
        logger.warning("POSTGRES_DB not set. Using development fallback 'swing_bot_db'")
        db_env_vars["POSTGRES_DB"] = "swing_bot_db"
else:
    # In production, require these variables
    missing_vars = [var for var, val in db_env_vars.items() if val is None]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

# Construct the database URL with the environment variables
DATABASE_URL = (
    f"postgresql+asyncpg://{db_env_vars['POSTGRES_USER']}:{db_env_vars['POSTGRES_PASSWORD']}"
    f"@{db_env_vars['POSTGRES_HOST']}:{db_env_vars['POSTGRES_PORT']}/{db_env_vars['POSTGRES_DB']}"
)

# Create the SQLAlchemy async engine
# echo=True logs SQL queries, useful for development, disable for production
echo_sql = Config.DEBUG
engine = create_async_engine(DATABASE_URL, echo=echo_sql)

# Create a configured "Session" class
# expire_on_commit=False prevents attributes from being expired after commit.
AsyncSessionFactory = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency that provides an async database session.
    
    Yields:
        AsyncSession: SQLAlchemy async session
        
    Raises:
        Exception: If database operations fail
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            await session.rollback()
            raise


# Optional: A function to create tables initially without Alembic (for testing/dev)
# Should generally rely on Alembic migrations for schema management.
# from .db_models import Base
# async def create_db_and_tables():
#     async with engine.begin() as conn:
#         await conn.run_sync(Base.metadata.drop_all) # Use with caution!
#         await conn.run_sync(Base.metadata.create_all)
#     await engine.dispose()
