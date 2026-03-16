"""
Generic utility to read any dataset (Delta / Parquet / CSV / JSON / Table)
and export it as a **single CSV file** to ADLS Gen2 (ABFSS).
Supports filters, column selection, row limits, and works in notebooks or jobs.
Ensures a reliable one-file output via coalesce + atomic rename.
"""

from typing import Optional, Sequence, Dict, Any
from datetime import datetime
import os
import sys
import re
import logging

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col
except Exception as e:  # pragma: no cover
    raise RuntimeError("This module must run on a Spark cluster (Databricks or spark-submit).") from e

# ----------------------------------------------------------------------------
# Logging setup (uses core.logging_config if available; else basicConfig)
# ----------------------------------------------------------------------------
def _get_logger(name: str = "single_csv_exporter") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    try:
        # If user has a central logging config module in workspace
        from core.logging_config import configure_logging  # type: ignore
        configure_logging()
    except Exception:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        )
    return logging.getLogger(name)

log = _get_logger()

# ----------------------------------------------------------------------------
# Utilities for Databricks / Hadoop FS interop
# ----------------------------------------------------------------------------
def _get_dbutils():
    """Return dbutils if available (Databricks), else None."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(SparkSession.getActiveSession() or SparkSession.builder.getOrCreate())
        return dbutils
    except Exception:
        return globals().get("dbutils", None)

def _safe_ls(path: str):
    dbutils = _get_dbutils()
    if dbutils is not None:
        return dbutils.fs.ls(path)
    sc = SparkSession.getActiveSession().sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    uri = sc._jvm.java.net.URI.create(path)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    p = sc._jvm.org.apache.hadoop.fs.Path(path)
    it = fs.listStatus(p)
    class FileInfo:
        def __init__(self, status):
            self.path = status.getPath().toString()
            self.name = status.getPath().getName()
            self.size = status.getLen()
    return [FileInfo(s) for s in it]

def _safe_mv(src: str, dst: str):
    dbutils = _get_dbutils()
    if dbutils is not None:
        return dbutils.fs.mv(src, dst)
    sc = SparkSession.getActiveSession().sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    src_uri = sc._jvm.java.net.URI.create(src)
    dst_uri = sc._jvm.java.net.URI.create(dst)
    fs_src = sc._jvm.org.apache.hadoop.fs.FileSystem.get(src_uri, hadoop_conf)
    fs_dst = sc._jvm.org.apache.hadoop.fs.FileSystem.get(dst_uri, hadoop_conf)
    src_path = sc._jvm.org.apache.hadoop.fs.Path(src)
    dst_path = sc._jvm.org.apache.hadoop.fs.Path(dst)
    parent = dst_path.getParent()
    if not fs_dst.exists(parent):
        fs_dst.mkdirs(parent)
    ok = fs_src.rename(src_path, dst_path)
    if not ok:
        raise IOError(f"Failed to move {src} to {dst}")

def _safe_rm(path: str, recurse: bool = True):
    dbutils = _get_dbutils()
    if dbutils is not None:
        return dbutils.fs.rm(path, recurse=recurse)
    sc = SparkSession.getActiveSession().sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    uri = sc._jvm.java.net.URI.create(path)
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    p = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs.delete(p, recurse)

# ----------------------------------------------------------------------------
# Core export API
# ----------------------------------------------------------------------------
_CATALOG_TABLE_PATTERN = re.compile(r"^[\w-]+\.[\w-]+(?:\.[\w-]+)?$")

def _read_input(
    spark: SparkSession,
    input_ref: str,
    read_as: str = "delta",
    read_options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Read input as DataFrame.

    input_ref: path (abfss:/, dbfs:/, s3:/, etc.) OR fully qualified table like 'catalog.db.table'.
    read_as: 'delta' | 'parquet' | 'csv' | 'json' | 'orc' | 'table'
    """
    read_as = (read_as or "delta").lower().strip()
    read_options = read_options or {}

    if read_as == "table" or _CATALOG_TABLE_PATTERN.match(input_ref or ""):
        log.info("Reading catalog table: %s", input_ref)
        return spark.table(input_ref)

    log.info("Reading path: %s as %s", input_ref, read_as)
    reader = spark.read.format(read_as)
    for k, v in read_options.items():
        reader = reader.option(k, v)
    return reader.load(input_ref)

def _select_columns(
    df: DataFrame,
    include_columns: Optional[Sequence[str]],
    exclude_columns: Optional[Sequence[str]],
) -> DataFrame:
    if include_columns:
        cols = [c for c in include_columns if c in df.columns]
        df = df.select([col(c) for c in cols])
    if exclude_columns:
        for c in exclude_columns:
            if c in df.columns:
                df = df.drop(c)
    return df

def export_single_csv(
    input_ref: str,
    output_dir_abfss: str,
    filename_prefix: str = "export",
    # reading
    read_as: str = "delta",
    read_options: Optional[Dict[str, str]] = None,
    where_clause: Optional[str] = None,
    include_columns: Optional[Sequence[str]] = None,
    exclude_columns: Optional[Sequence[str]] = None,
    row_limit: Optional[int] = None,
    # writing
    header: bool = True,
    delimiter: str = ",",
    quote: str = '"',
    escape: str = "\\",
    null_value: str = "",
    empty_value: str = "",
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    date_format: str = "yyyy-MM-dd",
    repartition_to_one: bool = True,
    compute_count: bool = False,
) -> Dict[str, Any]:
    """Read a dataset and export a **single CSV** file to ADLS Gen2.

    Returns a dict: {"path": str, "rows": int | None, "columns": [str]}
    """
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    if not output_dir_abfss.endswith("/"):
        output_dir_abfss += "/"

    # Read
    df = _read_input(spark, input_ref=input_ref, read_as=read_as, read_options=read_options)

    # Optional filter
    if where_clause:
        log.info("Applying filter: %s", where_clause)
        df = df.where(where_clause)

    # Column selection
    df = _select_columns(df, include_columns, exclude_columns)

    # Optional limit
    if row_limit is not None:
        log.info("Applying row limit: %s", row_limit)
        df = df.limit(int(row_limit))

    # Write temp one-part CSV
    ts_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    tmp_dir = f"{output_dir_abfss}{filename_prefix}_tmp_{ts_utc}/"
    if repartition_to_one:
        df = df.coalesce(1)

    writer = (
        df.write
          .mode("overwrite")
          .option("header", str(header).lower())
          .option("delimiter", delimiter)
          .option("quote", quote)
          .option("escape", escape)
          .option("nullValue", null_value)
          .option("emptyValue", empty_value)
          .option("timestampFormat", timestamp_format)
          .option("dateFormat", date_format)
    )
    writer.csv(tmp_dir)

    # Locate part file
    part_path = None
    for f in _safe_ls(tmp_dir):
        name = getattr(f, "name", None) or os.path.basename(getattr(f, "path", ""))
        if name.startswith("part-") and name.endswith(".csv"):
            part_path = getattr(f, "path", None) or f.path
            break
    if not part_path:
        raise FileNotFoundError("No CSV part file found after write. Check permissions and path.")

    final_name = f"{filename_prefix}_{ts_utc}Z.csv"
    final_path = f"{output_dir_abfss}{final_name}"

    # Move and cleanup
    _safe_mv(part_path, final_path)
    _safe_rm(tmp_dir, recurse=True)

    # Prepare result
    result = {
        "path": final_path,
        "rows": None,
        "columns": df.columns,
    }
    if compute_count:
        try:
            result["rows"] = df.count()
        except Exception as e:  # count is optional
            log.warning("Row count failed: %s", e)
            result["rows"] = None

    log.info("Single CSV created: %s", final_path)
    return result
