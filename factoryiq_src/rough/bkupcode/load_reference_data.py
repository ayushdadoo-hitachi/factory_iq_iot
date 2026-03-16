# Databricks notebook source
spark.sql("USE CATALOG `factoryiq_catalog`")
spark.sql("USE SCHEMA `factoryiq-csv-excel-schema`")

# COMMAND ----------

# MAGIC %run ./common/common_functions

# COMMAND ----------

# MAGIC %run ./global/global_functions

# COMMAND ----------

# MAGIC %run ./common/data_utils

# COMMAND ----------

joint_colors_path = f"{REF_BASE_PATH}{REF_JOINT_COLORS}"
ref_path_stage0_required_columns = f"{REF_BASE_PATH}{REF_STAGE0_REQUIRED_COLUMNS}"
wps_path_a = f"{REF_BASE_PATH}{REF_WPS_ACTUAL}"
reference_path = f"{REF_BASE_PATH}{REF_REQUIRED_WELD_FEATURES}"

# COMMAND ----------

import pandas as pd
import numpy as np

def sanitize_excel_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize Excel-imported Pandas DF for Spark + Arrow compatibility.
    """
    clean = pdf.copy()

    for col in clean.columns:
        dtype = clean[col].dtype

        # Numeric columns → keep as-is
        if dtype in ["int64", "float64"]:
            continue

        # Everything else → normalize
        clean[col] = (
            clean[col]
            .astype(str)
            .replace(
                {
                    "nan": None,
                    "NaN": None,
                    "None": None,
                    "": None
                }
            )
        )

    return clean


# COMMAND ----------

from pyspark.sql.types import *

def pandas_to_spark_schema(pdf: pd.DataFrame) -> StructType:
    fields = []

    for col, dtype in pdf.dtypes.items():
        if dtype == "int64":
            fields.append(StructField(col, LongType(), True))
        elif dtype == "float64":
            fields.append(StructField(col, DoubleType(), True))
        else:
            fields.append(StructField(col, StringType(), True))

    return StructType(fields)


# COMMAND ----------

def excel_path_to_spark_df(spark, path: str):
    print(path)
    pdf = read_excel_from_abfss(spark, path)
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    sdf = spark.createDataFrame(pdf, schema=schema)
    return normalize_columns(sdf)

# COMMAND ----------

required_stage0_columns_df = excel_path_to_spark_df(spark, ref_path_stage0_required_columns)
joint_colors_df           = excel_path_to_spark_df(spark, joint_colors_path)
wps_df_a                  = excel_path_to_spark_df(spark, wps_path_a)

# COMMAND ----------

ref_df = read_csv_from_abfss(spark, reference_path)

# COMMAND ----------

# Map each df to its destination path
writes = [
    (joint_colors_df, JOINT_COLORS_TABLE_PATH),
    (required_stage0_columns_df, REQUIRED_STAGE0_COLUMNS_TABLE_PATH),
    (wps_df_a, WPS_TABLE_ACTUAL_PATH),
    (ref_df, REQUIRED_WELD_FEATURES_TABLE_PATH),
]

for df, out_path in writes:
    print(f"Writing Delta (overwrite): {out_path}")
    write_df_to_abfss(df, out_path)

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS `factoryiq-catalog`.`factoryiq-referencetables-schema`.joint_colors_table;
# MAGIC DROP TABLE IF EXISTS `factoryiq-catalog`.`factoryiq-referencetables-schema`.required_stage0_columns_table;
# MAGIC DROP TABLE IF EXISTS `factoryiq-catalog`.`factoryiq-referencetables-schema`.welddata_reference_table;
# MAGIC DROP TABLE IF EXISTS `factoryiq-catalog`.`factoryiq-referencetables-schema`.wps_reference_table;
# MAGIC

# COMMAND ----------

spark.sql("DESCRIBE JOINT_COLORS_PATH")

# COMMAND ----------

dbutils.fs.ls("abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/ref_tables/joint_colors_table")
# dbutils.fs.ls("abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/ref_tables")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL "abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/joint_colors_table";
# MAGIC

# COMMAND ----------

dbutils.fs.rm("abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/joint_colors_table", True)

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC %pip install pyyaml
# MAGIC %pip install fsspec adlfs openpyxl
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()