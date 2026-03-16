# core/io_utils.py

from typing import Optional, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import io

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 

# -----------------------
# Delta I/O
# -----------------------
def read_delta(spark, path: str):
    return spark.read.format("delta").load(path)

def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",                 # ⬅️ default append (important for Bronze)
    options: Optional[Dict[str, str]] = None,
    register_table: Optional[str] = None  # ⬅️ optional: register a table name
):
    writer = df.write.format("delta").mode(mode)
    if options:
        for k, v in options.items():
            writer = writer.option(k, v)
    writer.save(path)

    # Optional: register a metastore table pointing to this path (no data movement)
    if register_table:
        spark = SparkSession.builder.getOrCreate()
        spark.sql(f"CREATE TABLE IF NOT EXISTS {register_table} USING DELTA LOCATION '{path}'")

# -----------------------
# Table I/O (Unity Catalog safe)
# -----------------------
def read_table(spark, table_name: str):
    return spark.read.table(table_name)

def write_table(df, table_name: str, mode: str = "overwrite"):
    df.write.format("delta").mode(mode).saveAsTable(table_name)

# -----------------------
# Standard CSV
# -----------------------
def read_csv(spark, path: str):
    return spark.read.option("header", "true").csv(path)

# ------------------------------------------
# 4️⃣ Read Excel from ABFSS
# ------------------------------------------
def read_excel_from_abfss(spark, full_path: str) -> pd.DataFrame:
    bin_df = spark.read.format("binaryFile").load(full_path)
    content_bytes = bin_df.select("content").head()[0]
    return pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")