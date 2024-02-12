"""Logger setup module.

This module configures the logging functionality, utilizing the Loguru library
for enhanced logging capabilities. It provides a convenient way to add file
logging and includes a decorator to log function calls, memory usage, and
errors.
"""

import psutil
from loguru import logger


def configure_logger():
    """Configure Loguru logger.

    The logs include the timestamp, log level, message, module, function, line
    number, and any additional information provided in the log call. The log
    level is set to INFO, capturing all log messages at this level and above.
    """
    logger.add(
        "logs/logging_loguru.log",
        format="{time} {level} {message} {module} {function} {line} {extra}",
        level="INFO",
    )


def logger_decorator(func):
    """Create a decorator to log function calls.

    This decorator uses the Loguru logger to log the start and end of the
    function call, along with memory usage information. It captures and
    logs any exceptions thrown by the function, re-raising them after logging.

    Parameters:
    ----------
    func : Callable
        The function to be decorated.

    Returns:
    -------
    Callable
        The wrapped function with logging functionality.
    """

    def wrapper(*args, **kwargs):
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 * 1024)
        available_mem_before = psutil.virtual_memory().available / (1024 * 1024)  # noqa

        logger.info(
            f"Calling function: {func.__name__} | Memory Before: {mem_before:.2f} MB | Available Memory: {available_mem_before:.2f} MB",  # noqa
        )
        try:
            result = func(*args, **kwargs)

            mem_after = process.memory_info().rss / (1024 * 1024)
            available_mem_after = psutil.virtual_memory().available / (
                1024 * 1024
            )  # noqa
            logger.info(
                f"Finishing function: {func.__name__} | Memory After: {mem_after:.2f} MB | Memory Used: {mem_after - mem_before:.2f} MB | Available Memory: {available_mem_after:.2f} MB",  # noqa
            )

            return result
        except Exception as e:
            logger.error(f"An error happened with {func.__name__}: {str(e)}")
            raise e

    return wrapper
