import logging
import sys
from rich.logging import RichHandler
from .config import LOG_FILENAME

def setup_logger(level=logging.INFO, filename=LOG_FILENAME):
    """Sets up logging with RichHandler and file output, controlling library verbosity."""
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(level) # Set the root logger level

    # Remove existing handlers to prevent duplication if called again
    if logger.hasHandlers():
        logger.handlers.clear()

    # --- Handlers --- 
    # File Handler (with standard formatting)
    file_handler = logging.FileHandler(filename)
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Rich Handler (for console, let Rich handle formatting)
    rich_handler = RichHandler(rich_tracebacks=True, markup=True, show_path=False) # show_path=False can reduce clutter
    rich_handler.setLevel(level)
    logger.addHandler(rich_handler)

    logger.info(f"Logger configured: Level={logging.getLevelName(level)}, File='{filename}'")

    # --- Control Azure SDK Logger Verbosity --- 
    # Set higher level for verbose Azure libraries unless our main level is DEBUG
    if level > logging.DEBUG:
        azure_loggers = [
            'azure.identity', 
            'azure.mgmt', 
            'azure.core.pipeline.policies.http_logging_policy'
        ]
        for logger_name in azure_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)
            logger.debug(f"Set level for {logger_name} to WARNING")
    else:
        logger.debug("Main log level is DEBUG, keeping Azure SDK loggers verbose.")
        
    return logger # Return the configured root logger

# Example usage (if running this file directly)
if __name__ == "__main__":
    # Example of using the setup function
    logger = setup_logger(level=logging.DEBUG) 
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("Caught an exception!") 