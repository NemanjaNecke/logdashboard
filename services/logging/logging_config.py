# services/logging/logging_config.py

import logging
import os
import datetime

def setup_logging():
    """
    Sets up the logging configuration.
    """
    logger = logging.getLogger()  # Get the root logger
    logger.setLevel(logging.DEBUG)  # Set the global logging level to INFO
    
    # To turn off logging completely
    # logger.setLevel(logging.CRITICAL + 1)  # No logs will be captured

    # To set logging level to WARNING
    # logger.setLevel(logging.WARNING)  # Only WARNING, ERROR, and CRITICAL logs will be captured
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Clear existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # File Handler
    log_file = os.path.join(log_dir, 'application.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)  # Set file handler level to INFO

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the file handler to the root logger
    logger.addHandler(file_handler)

    # (Optional) Console Handler - Remove or comment out if you don't want console logs
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.INFO)
    # console_handler.setFormatter(formatter)
    # logger.addHandler(console_handler)

    logger.info(f"Logging is set up. Level: {logger.level}, Log file: {log_file}")