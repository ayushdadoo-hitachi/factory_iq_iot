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

# wps_path_a = "/Volumes/workspace/sourcedata/sourcedatavolume/reference_data/wps_order_v5_actual.xlsx"
print(wps_path_a)
wps_pdf_raw_a = read_excel_from_abfss(spark,wps_path_a) # Step 1: Read Excel
wps_pdf_a = sanitize_excel_pdf(wps_pdf_raw_a) # Step 2: Sanitize
schema_a = pandas_to_spark_schema(wps_pdf_a) # Step 3: Build schema
wps_df_a = spark.createDataFrame(wps_pdf_a, schema=schema_a) # Step 4: Create Spark DF (Arrow-safe)
wps_df_a = normalize_columns(wps_df_a)
# display(wps_df)

# COMMAND ----------

joint_colors_pdf = read_excel_from_abfss(spark,joint_colors_path) # Step 1: Read Excel
joint_colors_pdf = sanitize_excel_pdf(joint_colors_pdf) # Step 2: Sanitize
joint_colors_schema = pandas_to_spark_schema(joint_colors_pdf) # Step 3: Build schema
joint_colors_df = spark.createDataFrame(joint_colors_pdf, schema=joint_colors_schema) # Step 4: Create Spark DF (Arrow-safe)
joint_colors_df = normalize_columns(joint_colors_df)

# COMMAND ----------

ref_df = read_csv_from_abfss(spark, reference_path)

# COMMAND ----------

print(ref_path_stage0_required_columns)
required_stage0_columns_pdf = read_excel_from_abfss(spark,ref_path_stage0_required_columns) # Step 1: Read Excel
required_stage0_columns_pdf = sanitize_excel_pdf(required_stage0_columns_pdf) # Step 2: Sanitize
required_stage0_columns_schema = pandas_to_spark_schema(required_stage0_columns_pdf) # Step 3: Build schema
required_stage0_columns_df = spark.createDataFrame(required_stage0_columns_pdf, schema=required_stage0_columns_schema) # Step 4: Create Spark DF (Arrow-safe)
required_stage0_columns_df = normalize_columns(required_stage0_columns_df)
# display(joint_colors_df)

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