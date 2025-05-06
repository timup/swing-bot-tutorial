"""Application configuration module."""

import os
from pathlib import Path
from typing import Optional


class Config:
    """Application configuration."""
    
    # Environment
    ENV: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = ENV == "development"
    
    # Path configuration
    BASE_DIR: Path = Path(__file__).parent.parent.resolve()
    DATA_DIR: Path = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    
    # Database configuration - See database.py for usage
    DB_USER: Optional[str] = os.getenv("POSTGRES_USER")
    DB_PASSWORD: Optional[str] = os.getenv("POSTGRES_PASSWORD")
    DB_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME: Optional[str] = os.getenv("POSTGRES_DB")
    # Add other environment variables as needed
    # e.g., API keys for external services
    OPENBB_API_KEY: Optional[str] = os.getenv("OPENBB_API_KEY") # Example, if ever needed
    POLYGON_API_KEY: Optional[str] = os.getenv("POLYGON_API_KEY")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE")
    
    # API configuration (for FastAPI when implemented)
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_WORKERS: int = int(os.getenv("API_WORKERS", "1"))
    
    @classmethod
    def yfinance_data_dir(cls) -> Path:
        """Get the directory for yfinance raw data."""
        return cls.RAW_DATA_DIR / "yfinance"
    
    @classmethod
    def openbb_data_dir(cls) -> Path:
        """Get the directory for OpenBB raw data."""
        return cls.RAW_DATA_DIR / "openbb"
    
    @classmethod
    def is_production(cls) -> bool:
        """Check if the application is running in production."""
        return cls.ENV == "production"
    
    @classmethod
    def is_testing(cls) -> bool:
        """Check if the application is running in test mode."""
        return cls.ENV == "testing" 