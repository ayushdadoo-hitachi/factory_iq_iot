# Databricks notebook source
# ==========================================
# FactoryIQ – Weld Data Pipeline Runner
# ==========================================

import sys
import logging
import re
from pyspark.sql import DataFrame
import io
import pandas as pd
from pyspark.sql.types import *
import numpy as np

# COMMAND ----------

# ------------------------------------------
# 1️⃣ Project Setup
# ------------------------------------------
PROJECT_PATH = "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"

if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

# COMMAND ----------

# ------------------------------------------
# 2️⃣ Imports
# ------------------------------------------
from config.config_loader import load_config
from config.config_builder import (
    build_paths,
    build_weldcoldnames,
    ref_data
)

import pipelines.pipeline0_csv_ingestion as pipeline0
import pipelines.pipeline1_data_quality as pipeline1
import pipelines.pipeline2_data_enrichment as pipeline2
import pipelines.pipeline3_generate_plots as pipeline3
import pipelines.pipeline4_transfer_images as pipeline4

# COMMAND ----------

# ------------------------------------------
# 3️⃣ Logging
# ------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)

logger = logging.getLogger("factoryiq.pipeline")

# COMMAND ----------

# ------------------------------------------
# 4️⃣ Config
# ------------------------------------------
CONFIG_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/config/welddata_dev.yml"
ROBOT_ID = "igm6_29fjan_4feb"
REF_BASE_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/ref_csv_excel/"

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 5️⃣ Utility Functions
# MAGIC # ------------------------------------------
# MAGIC def normalize_columns(df: DataFrame) -> DataFrame:
# MAGIC     def normalize(col_name: str) -> str:
# MAGIC         col = col_name.lower()
# MAGIC         col = re.sub(r"[ \-\.\/]+", "_", col)
# MAGIC         col = re.sub(r"_+", "_", col)
# MAGIC         return col.strip("_")
# MAGIC
# MAGIC     return df.toDF(*[normalize(c) for c in df.columns])
# MAGIC
# MAGIC
# MAGIC def load_csv_reference(path: str) -> DataFrame:
# MAGIC     logger.info(f"Loading reference CSV: {path}")
# MAGIC     df = (
# MAGIC         spark.read
# MAGIC         .option("header", "true")
# MAGIC         .option("inferSchema", "true")
# MAGIC         .csv(path)
# MAGIC     )
# MAGIC     return normalize_columns(df)
# MAGIC
# MAGIC
# MAGIC def write_delta_overwrite(df: DataFrame, path: str):
# MAGIC     logger.info(f"Writing Delta (overwrite): {path}")
# MAGIC     (
# MAGIC         df.write
# MAGIC         .format("delta")
# MAGIC         .mode("overwrite")
# MAGIC         .option("overwriteSchema", "true")
# MAGIC         .save(path)
# MAGIC     )
# MAGIC

# COMMAND ----------

# ------------------------------------------
# 6️⃣ Load Config & Build Paths
# ------------------------------------------
logger.info("Loading configuration...")
cfg = load_config(spark, CONFIG_PATH)

paths = build_paths(cfg)
ref_paths = ref_data(cfg)
weldcol_names = build_weldcoldnames(cfg)

robot_data_stage0_path = f"{paths['STAGE0_TABLE']}_{ROBOT_ID}"
robot_data_stage1_path = f"{paths['STAGE1_TABLE']}_{ROBOT_ID}"
robot_data_stage2_path = f"{paths['STAGE2_TABLE']}_{ROBOT_ID}"

weld_ref_path = paths["REF_REQUIRED_WELD_FEATURES"]
wps_ref_path = paths["REF_WPS_FEATURES"]
joint_colors_path = paths["JOINT_COLORS"]
image_output_basepath = paths["IMAGE_BASEPATH"]

techdevisrunning = weldcol_names["TECHDEV_ISRUNNING_COL"]
tps500current = weldcol_names["TPS500_CURRENT_COL"]

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

def read_excel_from_abfss(spark, full_path: str):
    """
    Reads an Excel file from ABFSS using Spark binaryFile and returns a pandas DataFrame.
    """
    # full_path = container_path.rstrip("/") + "/" + filename
    bin_df = spark.read.format("binaryFile").load(full_path)
    content_bytes = bin_df.select("content").head()[0]
    return pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")

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

# ------------------------------------------
# 7️⃣ Load Reference Data
# ------------------------------------------
joint_colors_df = load_csv_reference(
    f"{REF_BASE_PATH}{ref_paths['joint_colors_path']}"
)

required_stage0_df = load_csv_reference(
    f"{REF_BASE_PATH}{ref_paths['ref_path_stage0_required_columns']}"
)

wps_df = load_csv_reference(
    f"{REF_BASE_PATH}{ref_paths['wps_path_a']}"
)

required_weld_features_df = load_csv_reference(
    f"{REF_BASE_PATH}{ref_paths['reference_path']}"
)


# ------------------------------------------
# 8️⃣ Persist Reference Tables (Delta)
# ------------------------------------------
write_delta_overwrite(joint_colors_df, joint_colors_path)
write_delta_overwrite(wps_df, wps_ref_path)
write_delta_overwrite(required_weld_features_df, weld_ref_path)

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 9️⃣ Run Stage 0
# MAGIC # ------------------------------------------
# MAGIC pipeline0.run(
# MAGIC     spark=spark,
# MAGIC     input_path="abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/igm6_29fjan_4feb.csv",
# MAGIC     output_path=robot_data_stage0_path
# MAGIC )

# COMMAND ----------

# ------------------------------------------
# 🔟 Run Stage 1
# ------------------------------------------
# df_stage1 = pipeline1.run_stage1_pipeline(
#     spark=spark,
#     input_path=robot_data_stage0_path,
#     ref_weld_path=weld_ref_path,
#     output_path=robot_data_stage1_path
# )

# COMMAND ----------

# ------------------------------------------
# 1️⃣1️⃣ Run Stage 2
# ------------------------------------------
# df_stage2 = pipeline2.run_pipeline2(
#     spark=spark,
#     input_path=robot_data_stage1_path,
#     wps_ref_path=wps_ref_path,
#     tps500current=tps500current,
#     techdevisrunning=techdevisrunning,
#     output_path=robot_data_stage2_path
# )

# COMMAND ----------

# ------------------------------------------
# 1️⃣2️⃣ Run Stage 3 – Plot Generation
# ------------------------------------------
# pipeline3.run_pipeline3_generate_plots(
#     spark=spark,
#     input_path=robot_data_stage2_path,
#     image_output_basepath=image_output_basepath,
#     joint_colors_path=joint_colors_path,
#     robot_name=ROBOT_ID,
#     welding_active_col="arcseam_isactive",
#     activetask_col="d_activetask_f"
# )

# COMMAND ----------

# ------------------------------------------
# 1️⃣3️⃣ Stage 4 – Image Transfer
# ------------------------------------------
dbutils.widgets.text("source", "/Volumes/factoryiq_iot/weld_images/images")
dbutils.widgets.text("dest", "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/weld_path_images")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])

pipeline4.run_pipeline4_transfer_images(
    source=dbutils.widgets.get("source"),
    dest=dbutils.widgets.get("dest"),
    dry_run=dbutils.widgets.get("dry_run") == "true"
)

logger.info("Pipeline execution completed successfully.")