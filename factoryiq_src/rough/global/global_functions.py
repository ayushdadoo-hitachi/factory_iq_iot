# Databricks notebook source
import yaml
import os

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
import re

# COMMAND ----------

def normalize_columns(df: DataFrame) -> DataFrame:
    def normalize(col_name: str) -> str:
        # Lowercase
        col = col_name.lower()
        # Replace space, hyphen, dot, slash with underscore
        col = re.sub(r"[ \-\.\/]+", "_", col)
        # Remove multiple underscores
        col = re.sub(r"_+", "_", col)
        # Strip leading/trailing underscores
        col = col.strip("_")
        return col

    # Rename all columns in one go
    new_cols = [normalize(c) for c in df.columns]
    return df.toDF(*new_cols)

# COMMAND ----------

from pyspark.sql import functions as F

def extract_active_task_num(df):
    """
    1. Extract numeric part from 'TaskX'
    2. Convert empty string to NULL
    3. Cast to Integer safely
    4. Replace NULLs with 0
    """

    df = df.withColumn(
        "der_activetask_num",
        F.regexp_extract(F.col("activetask"), r"(\d+)", 1)
    )

    # Handle empty string BEFORE casting
    df = df.withColumn(
        "der_activetask_num",
        F.when(F.col("der_activetask_num") == "", None)
         .otherwise(F.col("der_activetask_num"))
    )

    # Safe cast
    df = df.withColumn(
        "der_activetask_num",
        F.col("der_activetask_num").cast("int")
    )

    # # Final null handling
    # df = df.withColumn(
    #     "der_activetask_num",
    #     F.coalesce(F.col("der_activetask_num"), F.lit(0))
    # )

    return df


# COMMAND ----------

def date_to_string():
    start_dt = datetime.strptime(str(DATA_START_TIME), "%Y-%m-%d %H:%M:%S")
    date_str = start_dt.strftime("%Y%m%d")
    return date_str

# COMMAND ----------

import io
import pandas as pd

def read_excel_from_abfss(spark, full_path: str):
    """
    Reads an Excel file from ABFSS using Spark binaryFile and returns a pandas DataFrame.
    """
    # full_path = container_path.rstrip("/") + "/" + filename

    bin_df = spark.read.format("binaryFile").load(full_path)
    content_bytes = bin_df.select("content").head()[0]

    return pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")

# COMMAND ----------

import io
import pandas as pd

def read_csv_from_abfss(spark, full_path: str):
    df = spark.read.option("header", "true").csv(full_path)
    return df

# COMMAND ----------

def write_df_to_abfss(df, full_path: str):
    df.write.format("delta").mode("overwrite").save(full_path)

# COMMAND ----------

def read_data_from_tables_inabfss(spark, full_path: str):
    df = spark.read.format("delta").load(full_path)
    return df


# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install pyyaml