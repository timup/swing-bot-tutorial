"""Logging configuration for the swing-bot application."""

import logging
import os
import sys
from pathlib import Path

# Set default log level based on environment
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Mapping of string log levels to logging constants
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def configure_logging(log_level=DEFAULT_LOG_LEVEL, log_file=None):
    """
    Configure logging for the application.
    
    Args:
        log_level: String log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        
    Returns:
        Logger: Root logger instance
    """
    # Validate log level
    level = LOG_LEVELS.get(log_level.upper(), logging.INFO)
    
    # Configure basic logging
    handlers = [logging.StreamHandler(sys.stdout)]
    
    # Add file handler if log_file is provided
    if log_file:
        log_path = Path(log_file)
        # Ensure directory exists
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        handlers.append(file_handler)
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        handlers=handlers,
    )
    
    # Set SQLAlchemy logging to WARNING by default to reduce verbosity
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    
    # Disable yfinance verbose debug logs for cleaner console output
    logging.getLogger("yfinance").setLevel(logging.WARNING)
    
    # Return root logger
    return logging.getLogger()


def get_logger(name):
    """
    Get a named logger instance.
    
    Args:
        name: Logger name, typically __name__ or module name
        
    Returns:
        Logger: Configured logger instance
    """
    return logging.getLogger(name) 