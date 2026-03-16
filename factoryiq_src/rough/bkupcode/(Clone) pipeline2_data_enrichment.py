# stage1_data_quality_pipeline.py
"""
Stage 1 ETL Pipeline – Production-ready
- Reads input Delta table
- Cleans and normalizes columns
- Applies reference schema and data types
- Writes output Delta table
"""

import logging
import importlib
from pyspark.sql import SparkSession, DataFrame

from core.io_utils import read_delta, write_delta
from core.schema_utils import filter_columns_by_reference

import core.cleaning_utils as cu
importlib.reload(cu)
from core.cleaning_utils import (
    clean_columns,
    regex_clean_columns,
    normalize_strings,
    apply_comma_to_dot,
    drop_rows_with_invalid_int_values,
    apply_datatype,
    apply_roundoff_to_numeric_columns,
    fix_and_parse_timestamp,
    filter_out_of_range_rows,
)

# -----------------------
# Configure logger
# -----------------------
logger = logging.getLogger("Stage2Pipeline")
logger.setLevel(logging.INFO)

if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

logger.propagate = False  # ⚡ key fix to avoid double logging


# -----------------------
# Pipeline function
# -----------------------
def run_pipeline2(
    spark: SparkSession,
    input_path: str,
    ref_wps_path: str
) -> DataFrame:
    """
    Executes Stage 1 ETL pipeline.

    Args:
        spark: SparkSession
        input_path: str, path to input Delta table
        ref_weld_path: str, path to reference Delta table
        output_path: str, path to write processed Delta table

    Returns:
        Spark DataFrame after Stage 1 transformations
    """

    logger.info("📥 Reading input Delta table from %s", input_path)
    df = read_delta(spark, input_path)

    return df

   
