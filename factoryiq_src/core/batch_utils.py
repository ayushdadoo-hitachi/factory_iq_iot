# core/batch_utils.py
"""Batch helpers: derive batch_id and attach standard columns."""
import importlib

from pyspark.sql import DataFrame, functions as F

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def extract_batch_id_from_path(csv_path: str) -> str:
    """Derive batch_id from a CSV path."""
    name = csv_path.split("/")[-1]
    return name[:-4] if name.lower().endswith(".csv") else name


def add_batch_id_col(df: DataFrame, batch_id: str, colname: str = "batch_id") -> DataFrame:
    """Attach batch_id column."""
    logger.debug("add_batch_id_col: %s=%s", colname, batch_id)
    return df.withColumn(colname, F.lit(batch_id))