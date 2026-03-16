# fiq_logging/logging_config.py
import logging, sys
from fiq_logging.la_handler import LogAnalyticsBatchHandler
from fiq_logging.constants import DEFAULT_LOG_TYPE, DEFAULT_LEVEL

def init_logging(
    logger_name: str,
    workspace_id: str,
    shared_key: str,
    level: str = DEFAULT_LEVEL,
    log_type: str = DEFAULT_LOG_TYPE,
    add_console: bool = True,
    batch_size: int = 50,
    flush_interval: float = 2.0,
    max_retries: int = 5,
    timeout_sec: int = 5,
):
    """
    Initialize a clean module logger:
    - Clears all existing handlers on root & module.
    - Disables propagation (no duplicates).
    - Adds exactly one console handler (optional).
    - Adds one Log Analytics batch handler (using provided ID/Key).
    Returns the configured logger.
    """
    # Quiet root
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.propagate = False
    root.setLevel(logging.NOTSET)

    # Module logger
    logger = logging.getLogger(logger_name)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    logger.propagate = False
    logger.setLevel(level)

    # Console handler (optional)
    if add_console:
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logger.addHandler(sh)

    # LAW handler
    la = LogAnalyticsBatchHandler(
        workspace_id=workspace_id,
        shared_key=shared_key,
        log_type=log_type,
        batch_size=batch_size,
        flush_interval=flush_interval,
        max_retries=max_retries,
        timeout_sec=timeout_sec
    )
    la.setLevel(level)
    logger.addHandler(la)

    return logger