"""
Pipeline 0: Raw Robot File Ingestion (Stage 0 - Bronze)

Reads semi-structured robot CSV files from ADLS/DBFS where:
    - Row 1: metadata
    - Row 2: header
    - Row 3: metadata
    - Row 4+: data rows

Performs:
    - Header extraction
    - Column sanitization (Delta-safe)
    - Duplicate column handling

Writes clean output to Bronze (Delta format).
"""

import logging
import importlib
from pyspark.sql import SparkSession, DataFrame

import core.ingestion_utils as ciu
importlib.reload(ciu)

from core.ingestion_utils import read_and_prepare
from core.io_utils import write_delta


logger = logging.getLogger(__name__)


def run(
    spark: SparkSession,
    input_path: str,
    output_path: str
) -> DataFrame:
    """
    Stage 0 execution function.
    """

    logger.info("--------------------------------------------------")
    logger.info("🚀 Starting Stage 0 - Raw Ingestion")
    logger.info("--------------------------------------------------")

    logger.info("📥 Reading robot file from: %s", input_path)
    df = read_and_prepare(spark, input_path)

    # Avoid multiple Spark actions
    row_count = df.count()
    col_count = len(df.columns)

    logger.info("📊 Rows read: %d", row_count)
    logger.info("📑 Columns detected: %d", col_count)

    logger.info("💾 Writing Bronze Delta table to: %s", output_path)
    write_delta(df, output_path, mode="overwrite")

    logger.info("✅ Stage 0 completed successfully.")
    logger.info("--------------------------------------------------")

    return df