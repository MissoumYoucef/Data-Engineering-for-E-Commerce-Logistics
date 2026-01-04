"""
Logging Module
==============

Provides structured logging for all ETL components.
Supports console and file output with configurable levels.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from .config import config


def setup_logger(
    name: str,
    level: Optional[str] = None,
    log_file: Optional[str] = None
) -> logging.Logger:
    """
    Create a configured logger instance.
    
    Args:
        name: Logger name (usually __name__ from the calling module)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for log output
        
    Returns:
        Configured Logger instance
        
    Example:
        logger = setup_logger(__name__)
        logger.info("Starting data extraction")
    """
    # Get settings from config or use defaults
    log_level = level or config.logging.get("level", "INFO")
    log_format = config.logging.get(
        "format", 
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    file_path = log_file or config.logging.get("file")
    if file_path:
        log_path = Path(file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class ETLLogger:
    """
    Context-aware logger for ETL operations.
    
    Provides methods that automatically include ETL stage context
    in log messages for easier debugging and monitoring.
    
    Usage:
        etl_logger = ETLLogger("extract")
        etl_logger.info("Fetching data from API", source="fake_store")
    """
    
    def __init__(self, stage: str, name: str = "logiflow"):
        """
        Initialize ETL logger with stage context.
        
        Args:
            stage: ETL stage (extract, transform, load)
            name: Base logger name
        """
        self.stage = stage
        self._logger = setup_logger(f"{name}.{stage}")
    
    def _format_message(self, message: str, **context) -> str:
        """Format message with context as key-value pairs."""
        if context:
            ctx_str = " | ".join(f"{k}={v}" for k, v in context.items())
            return f"[{self.stage.upper()}] {message} | {ctx_str}"
        return f"[{self.stage.upper()}] {message}"
    
    def debug(self, message: str, **context) -> None:
        """Log debug message with context."""
        self._logger.debug(self._format_message(message, **context))
    
    def info(self, message: str, **context) -> None:
        """Log info message with context."""
        self._logger.info(self._format_message(message, **context))
    
    def warning(self, message: str, **context) -> None:
        """Log warning message with context."""
        self._logger.warning(self._format_message(message, **context))
    
    def error(self, message: str, **context) -> None:
        """Log error message with context."""
        self._logger.error(self._format_message(message, **context))
    
    def critical(self, message: str, **context) -> None:
        """Log critical message with context."""
        self._logger.critical(self._format_message(message, **context))
