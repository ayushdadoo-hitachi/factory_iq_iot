# fiq_logging/logging_config.py
import logging
import sys
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
    Initialize logging for Databricks:
    - Attach handlers to ROOT so all module loggers propagate to them.
    - Console handler (optional).
    - Log Analytics batch handler for LAW ingestion.
    Returns a convenient named logger (your app logger), but any
    'logging.getLogger(__name__)' in imported modules will also flow to LAW.
    """
    # Clean root handlers
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)

    # Console handler (optional)
    if add_console:
        sh = logging.StreamHandler(stream=sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        ))
        root.addHandler(sh)

    # LAW handler (attached to ROOT)
    la = LogAnalyticsBatchHandler(
        workspace_id=workspace_id,
        shared_key=shared_key,
        log_type=log_type,
        batch_size=batch_size,
        flush_interval=flush_interval,
        max_retries=max_retries,
        timeout_sec=timeout_sec,
    )
    la.setLevel(level)
    root.addHandler(la)

    # Your app logger (optional convenience)
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = True  # let it flow to ROOT handlers
    return logger

def quiet_noise():
    """
    Optional: reduce noisy libraries that you don't want in notebook or LAW.
    Call after init_logging().
    """
    for noisy in [
        "py4j", "py4j.clientserver",
        "urllib3", "requests",
        "shaded.databricks.spark",
        "delta", "azure",
    ]:
        logging.getLogger(noisy).setLevel(logging.WARN)