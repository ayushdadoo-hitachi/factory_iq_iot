# core/io_utils.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import io


# -----------------------
# Delta I/O
# -----------------------
def read_delta(spark, path: str):
    return spark.read.format("delta").load(path)


def write_delta(df, path: str, mode: str = "overwrite"):
    df.write.format("delta").mode(mode).save(path)


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
    """
    Reads Excel file from ABFSS using Spark binaryFile
    and returns Pandas DataFrame.
    """
    bin_df = spark.read.format("binaryFile").load(full_path)
    content_bytes = bin_df.select("content").head()[0]
    return pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")