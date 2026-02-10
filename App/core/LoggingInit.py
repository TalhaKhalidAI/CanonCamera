# App/core/logging.py - FIXED VERSION (No GetEnvDate dependency)
import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

def setup_core_logging(log_path: Optional[str] = None) -> logging.Logger:
    """Setup logging for the core module without GetEnvDate dependency"""
    
    # Get log path from various sources (in order of priority)
    if log_path:
        # Use provided path
        LOG_PATH = log_path
    elif "LOG_PATH" in os.environ:
        # Use environment variable
        LOG_PATH = os.environ["LOG_PATH"]
    else:
        # Use default
        LOG_PATH = "logs"
    
    # Create log directory
    log_dir = Path(LOG_PATH)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "core.log"
    
    # Get log level from environment or use default
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(
        logging.Formatter('%(levelname)s - %(name)s - %(message)s')
    )
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log initialization
    logger.info(f"Core logging initialized. File: {log_file}")
    logger.info(f"Log level: {log_level_str}")
    
    return logger

# Initialize when module is imported
core_logger = setup_core_logging()

def get_core_logger(name: str = "App.core") -> logging.Logger:
    """Get a logger for core modules"""
    return logging.getLogger(name)

def get_module_logger(module_name: str) -> logging.Logger:
    """Get a logger for any module"""
    return logging.getLogger(f"App.{module_name}")