# pipeline1_data_quality_pipeline.py
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

# --------------------------------------------------
# Logger (Databricks-safe)
# --------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

    logger.info("pipeline1_data_quality | --------------------------------------------------")
    logger.info("pipeline1_data_quality | Starting Data Quality")
    logger.info("pipeline1_data_quality | --------------------------------------------------")

    logger.info("pipeline1_data_quality | Reading input Delta table from %s", input_path)
    df = read_delta(spark, input_path)

    # logger.info("pipeline1_data_quality | Cleaning column names")
    # df = clean_columns(df)

    logger.info("pipeline1_data_quality | Reading reference Delta table from %s", ref_weld_path)
    ref_weld_df = read_delta(spark, ref_weld_path)

    logger.info("pipeline1_data_quality | Filtering columns based on reference")
    df = filter_columns_by_reference(df, ref_weld_df)

    logger.info("pipeline1_data_quality | Applying regex-based column cleaning")
    df = regex_clean_columns(df)

    logger.info("pipeline1_data_quality | Normalizing string values")
    df = normalize_strings(df)

    logger.info("pipeline1_data_quality | Applying comma-to-dot conversion where required")
    df = apply_comma_to_dot(df, ref_weld_df)

    logger.info("pipeline1_data_quality | Dropping rows with invalid integer values")
    df, removed = drop_rows_with_invalid_int_values(df, ref_weld_df, log=False)
    logger.info("pipeline1_data_quality | Rows removed due to invalid integers: %d", removed)
    logger.info("pipeline1_data_quality | Remaining rows: %d", df.count())

    logger.info("pipeline1_data_quality | Applying datatypes as per reference")
    df = apply_datatype(df, ref_weld_df)

    logger.info("pipeline1_data_quality | Rounding numeric columns as per reference")
    df = apply_roundoff_to_numeric_columns(df, ref_weld_df, log=False)

    logger.info("pipeline1_data_quality | Cleaning and parsing timestamp column 'time'")
    df = fix_and_parse_timestamp(df, colname="time", use_try_to_timestamp=False)

    logger.info("pipeline1_data_quality | Filtering out-of-range rows")
    df = filter_out_of_range_rows(df, ref_weld_df)

    logger.info("pipeline1_data_quality | Writing processed Delta table to %s", output_path)
    write_delta(df, output_path)

    logger.info("pipeline1_data_quality | Pipeline completed successfully")

    return df