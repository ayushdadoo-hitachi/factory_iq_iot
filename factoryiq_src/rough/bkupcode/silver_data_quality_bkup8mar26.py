# silver_data_quality_pipeline.py
"""
Stage 1 ETL Pipeline – Production-ready
- Reads input Delta table
- Cleans and normalizes columns
- Applies reference schema and data types
- Writes output Delta table
"""
import importlib
from pyspark.sql import SparkSession, DataFrame

from core.io_utils import read_delta, write_delta
from core.schema_utils import filter_columns_by_reference

import core.cleaning_utils as cu
importlib.reload(cu)
from core.cleaning_utils import (
    # clean_columns,
    regex_clean_columns,
    normalize_strings,
    apply_comma_to_dot,
    drop_rows_with_invalid_int_values,
    apply_datatype,
    apply_roundoff_to_numeric_columns,
    fix_and_parse_timestamp,
    filter_out_of_range_rows,
)

from core.logging_config import get_module_logger
logger = get_module_logger(__name__)  # produces 'factoryiq.pipelines.gold_data_enrichment'


# --------------------------------------------------
# Pipeline function
# --------------------------------------------------
def run_stage(
    spark: SparkSession,
    input_path: str,
    ref_weld_path: str,
    output_path: str
) -> DataFrame:
    """
    Executes Stage 1 ETL pipeline.
    """

    logger.info("--------------------------------------------------")
    logger.info("Starting Silver - Data Quality")
    logger.info("--------------------------------------------------")

    logger.debug("Reading input Delta table from %s", input_path)
    df = read_delta(spark, input_path)

    # logger.debug("Cleaning column names")
    # df = clean_columns(df)

    logger.debug("Reading reference Delta table from %s", ref_weld_path)
    ref_weld_df = read_delta(spark, ref_weld_path)

    logger.debug("Filtering columns based on reference")
    df = filter_columns_by_reference(df, ref_weld_df)

    logger.debug("Applying regex-based column cleaning")
    df = regex_clean_columns(df)

    logger.debug("Normalizing string values")
    df = normalize_strings(df)

    logger.debug("Applying comma-to-dot conversion where required")
    df = apply_comma_to_dot(df, ref_weld_df)

    logger.debug("Dropping rows with invalid integer values")
    df, removed = drop_rows_with_invalid_int_values(df, ref_weld_df, log=False)
    logger.debug("Rows removed due to invalid integers: %d", removed)
    logger.debug("Remaining rows: %d", df.count())

    logger.debug("Applying datatypes as per reference")
    df = apply_datatype(df, ref_weld_df)

    logger.debug("Rounding numeric columns as per reference")
    df = apply_roundoff_to_numeric_columns(df, ref_weld_df, log=False)

    logger.debug("Cleaning and parsing timestamp column 'time'")
    df = fix_and_parse_timestamp(df, colname="time", use_try_to_timestamp=False)

    logger.debug("Filtering out-of-range rows")
    df = filter_out_of_range_rows(df, ref_weld_df)

    logger.debug("Writing processed Delta table to %s", output_path)
    write_delta(df, output_path)

    logger.info("Silver - Data Quality Completed")

    return df