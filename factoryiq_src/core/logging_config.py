# core/logging_config.py
import logging
import os
from typing import Optional

# Define a TRACE level below DEBUG (numeric value 5)
TRACE_LEVEL_NUM = 5
if not hasattr(logging, "TRACE"):
    logging.addLevelName(TRACE_LEVEL_NUM, "TRACE")

def trace(self, message, *args, **kws):
    if self.isEnabledFor(TRACE_LEVEL_NUM):
        self._log(TRACE_LEVEL_NUM, message, args, **kws)
logging.Logger.trace = trace  # logger.trace(...)

def _map_verbosity_to_level(verbosity: int) -> int:
    """
    1 -> INFO   (start/end only)
    2 -> DEBUG  (milestones)
    3 -> TRACE  (very detailed)
    """
    if verbosity <= 1:
        return logging.INFO
    elif verbosity == 2:
        return logging.DEBUG
    else:
        return TRACE_LEVEL_NUM

def setup_logging(
    *,
    app_logger_name: str = "factoryiq",
    verbosity: Optional[int] = None,
    level: Optional[int] = None,
    preserve_root_handlers: bool = True,
    propagate_to_root: bool = False,
    fmt: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
) -> logging.Logger:
    """
    Initialize an application logger without clobbering Spark's handlers.
    Idempotent: safe to call multiple times; updates level if already configured.
    """
    # Resolve level from verbosity if provided
    if level is None:
        v_env = os.getenv("APP_LOG_VERBOSITY")
        if verbosity is None and v_env:
            try:
                verbosity = int(v_env)
            except Exception:
                verbosity = 2
        if verbosity is None:
            verbosity = 2  # default: mid verbosity
        level = _map_verbosity_to_level(verbosity)

    # Configure the app logger
    logger = logging.getLogger(app_logger_name)
    if getattr(logger, "_configured", False):
        logger.setLevel(level)
        for h in logger.handlers:
            h.setLevel(level)
        return logger

    logger.setLevel(level)
    logger.propagate = propagate_to_root

    # Remove only handlers on this app logger (leave root/Spark alone)
    for h in list(logger.handlers):
        logger.removeHandler(h)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt))
    handler.setLevel(level)
    logger.addHandler(handler)

    if not preserve_root_handlers:
        root = logging.getLogger()
        root.setLevel(max(root.level, logging.WARNING))

    logger._configured = True
    logger.debug("Logging configured (app=%s, level=%s)", app_logger_name, logging.getLevelName(level))
    return logger

def get_module_logger(module_name: str, app_logger_name: str = "factoryiq") -> logging.Logger:
    """
    Return a child logger (e.g., 'factoryiq.pipelines.gold_data_enrichment').
    """
    return logging.getLogger(f"{app_logger_name}.{module_name}")