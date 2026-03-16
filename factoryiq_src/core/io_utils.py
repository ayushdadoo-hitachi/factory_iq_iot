# core/io_utils.py
"""I/O helpers for Delta tables, tables, CSV, and Excel-from-ABFSS."""

from typing import Optional, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import io
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


# -----------------------
# Delta I/O
# -----------------------
def read_delta(spark, path: str):
    """Read a Delta dataset from a path."""
    logger.debug("read_delta: %s", path)
    return spark.read.format("delta").load(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    options: Optional[Dict[str, str]] = None,
    register_table: Optional[str] = None
):
    """Write a DataFrame as Delta to a path; optionally register a table."""
    logger.debug(
        "write_delta: path=%s, mode=%s, options=%s, register_table=%s",
        path, mode, (list(options.keys()) if options else None), register_table
    )

    writer = df.write.format("delta").mode(mode)
    if options:
        for k, v in options.items():
            writer = writer.option(k, v)
    writer.save(path)

    if register_table:
        spark = SparkSession.builder.getOrCreate()
        spark.sql(f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{path}'")
        logger.debug("write_delta: registered table '%s' at '%s'", register_table, path)

    logger.debug("write_delta: completed")


# -----------------------
# Table I/O
# -----------------------
def read_table(spark, table_name: str):
    """Read a table by name."""
    logger.debug("read_table: %s", table_name)
    return spark.read.table(table_name)


def write_table(df: DataFrame, table_name: str, mode: str = "overwrite"):
    """Write a DataFrame to a managed table."""
    logger.debug("write_table: table=%s, mode=%s", table_name, mode)
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    logger.debug("write_table: completed")


# -----------------------
# CSV
# -----------------------
def read_csv(spark, path: str):
    """Read a CSV with header=true."""
    logger.debug("read_csv: %s", path)
    return spark.read.option("header", "true").csv(path)


# -----------------------
# Excel from ABFSS
# -----------------------
def read_excel_from_abfss(spark, full_path: str) -> pd.DataFrame:
    """Load Excel file from ABFSS into a pandas DataFrame."""
    logger.debug("read_excel_from_abfss: %s", full_path)
    bin_df = spark.read.format("binaryFile").load(full_path)
    content_bytes = bin_df.select("content").head()[0]
    pdf = pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")
    logger.debug("read_excel_from_abfss: columns(sample)=%s", list(pdf.columns)[:5])
    return pdf