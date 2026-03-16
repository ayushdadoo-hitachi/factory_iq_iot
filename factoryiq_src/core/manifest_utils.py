# core/manifest_utils.py
"""Delta manifest helpers: init, exists, query, append."""
import importlib
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, functions as F

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        _ = spark.read.format("delta").load(path).limit(1).count()
        return True
    except Exception:
        return False


def init_manifest_if_needed(spark: SparkSession, manifest_path: str, schema: Optional[str] = None) -> None:
    """Create an empty Delta manifest if missing."""
    if not delta_exists(spark, manifest_path):
        sch = schema or "file_path STRING, processed_ts TIMESTAMP, status STRING"
        (spark.createDataFrame([], sch)
              .write.format("delta")
              .mode("overwrite")
              .save(manifest_path))
        logger.debug("init_manifest_if_needed: created %s", manifest_path)


def is_in_manifest(spark: SparkSession, manifest_path: str, file_path: str) -> bool:
    try:
        mf = spark.read.format("delta").load(manifest_path)
        return mf.where(F.col("file_path") == file_path).limit(1).count() > 0
    except Exception:
        return False


def append_manifest(spark: SparkSession, manifest_path: str, file_path: str, status: str) -> None:
    row = [(file_path, datetime.utcnow(), status)]
    df = spark.createDataFrame(row, ["file_path", "processed_ts", "status"])
    df.write.format("delta").mode("append").save(manifest_path)
    logger.debug("append_manifest: %s -> %s (%s)", file_path, manifest_path, status)