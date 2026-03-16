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
logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# if not logger.hasHandlers():
#     ch = logging.StreamHandler()
#     formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#     ch.setFormatter(formatter)
#     logger.addHandler(ch)

# logger.propagate = False  # ⚡ key fix to avoid double logging


# -----------------------
# Pipeline function
# -----------------------
def run_stage1_pipeline(
    spark: SparkSession,
    input_path: str,
    ref_weld_path: str,
    output_path: str
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

    logger.info("--------------------------------------------------")
    logger.info("Starting Stage 1 – Data Quality")
    logger.info("--------------------------------------------------")

    logger.info(f"My logger name is: {logger.name}")

    root = logging.getLogger()
    print("ROOT HANDLERS:", root.handlers)
    print("PIPELINE LOGGER HANDLERS:", logger.handlers)
    print("PROPAGATE:", logger.propagate)

    logger.info("📥 Reading input Delta table from %s", input_path)
    df = read_delta(spark, input_path)

    logger.info("🧹 Cleaning column names")
    df = clean_columns(df)

    logger.info("📥 Reading reference Delta table from %s", ref_weld_path)
    ref_weld_df = read_delta(spark, ref_weld_path)

    logger.info("🔎 Filtering columns based on reference")
    df = filter_columns_by_reference(df, ref_weld_df)

    logger.info("✨ Applying regex-based column cleaning")
    df = regex_clean_columns(df)

    logger.info("🔤 Normalizing string values")
    df = normalize_strings(df)

    logger.info("💡 Applying comma-to-dot conversion where required")
    df = apply_comma_to_dot(df, ref_weld_df)

    logger.info("⚠ Dropping rows with invalid integer values")
    df, removed = drop_rows_with_invalid_int_values(df, ref_weld_df, log=False)
    logger.info("🧹 Rows removed due to invalid integers: %d", removed)
    logger.info("✅ Remaining rows: %d", df.count())

    logger.info("🔧 Applying datatypes as per reference")
    df = apply_datatype(df, ref_weld_df)

    logger.info("🔢 Rounding numeric columns as per reference")
    df = apply_roundoff_to_numeric_columns(df, ref_weld_df, log=False)

    logger.info("⏱ Cleaning and parsing timestamp column 'time'")
    df = fix_and_parse_timestamp(df, colname="time", use_try_to_timestamp=False)

    logger.info("⏱ filtering out of range rows")
    df = filter_out_of_range_rows(df, ref_weld_df)

    logger.info("📤 Writing processed Delta table to %s", output_path)
    write_delta(df, output_path)

    logger.info("🎉 Stage 1 pipeline completed successfully")
    return df
