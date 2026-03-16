# Databricks notebook source
# ====================
# Imports 
# ====================

# -------------------------
# Standard Library
# -------------------------
import sys
import re
import io
import logging
import importlib

# -------------------------
# Third-Party
# -------------------------
import pandas as pd
import numpy as np

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# -------------------------
# Project Config Modules
# -------------------------
import config.config_loader as config_loader
import config.config_builder as config_builder
importlib.reload(config_builder)

from config.config_loader import load_config
from config.config_builder import build_paths, build_weldcoldnames, ref_data

# -------------------------
# Pipelines
# -------------------------
# import pipelines.pipeline_reference_data_load as pipeline_ref
import pipelines.pipeline0_csv_ingestion as pipeline0
import pipelines.pipeline1_data_quality as pipeline1
import pipelines.pipeline2_data_enrichment as pipeline2
import pipelines.pipeline3_generate_plots as pipeline3
import pipelines.pipeline4_transfer_images as pipeline4

# Reload only the ones you actively modify during dev
importlib.reload(pipeline0)
importlib.reload(pipeline1)
importlib.reload(pipeline2)
importlib.reload(pipeline3)
importlib.reload(pipeline4)

# COMMAND ----------

from core.logging_config import setup_logging
setup_logging()
logger = logging.getLogger(__name__)

# COMMAND ----------

# ------------------------------------------
# 1️⃣ Setup Project Path
# ------------------------------------------

PROJECT_PATH = "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"

if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

print("Project path configured.")

# COMMAND ----------

# ------------------------------------------
# 4️⃣ Configuration
# ------------------------------------------
CONFIG_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/config/welddata_dev.yml"
ref_base_path = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/ref_csv_excel/"
ROBOT_ID = "igm6_29fjan_4feb"

# COMMAND ----------

# ------------------------------------------
# 5️⃣ Load Config + Build All Runtime Paths
# ------------------------------------------

# Load config
cfg = load_config(spark, CONFIG_PATH)

# Build base paths
paths = build_paths(cfg)

robot_data_stage0_path = f"{paths['STAGE0_TABLE']}_{ROBOT_ID}"
robot_data_stage1_path = f"{paths['STAGE1_TABLE']}_{ROBOT_ID}"
robot_data_stage2_path = f"{paths['STAGE2_TABLE']}_{ROBOT_ID}"

weld_ref_path = paths["REF_REQUIRED_WELD_FEATURES"]
wps_ref_path = paths["REF_WPS_FEATURES"]
joint_colors_path = paths["JOINT_COLORS"]
image_output_basepath = paths["IMAGE_BASEPATH"]

# ------------------------------------------
# Weld Column Names
# ------------------------------------------

weldcol_names = build_weldcoldnames(cfg)

techdevisrunning = weldcol_names["TECHDEV_ISRUNNING_COL"]
tps500current   = weldcol_names["TPS500_CURRENT_COL"]

# ------------------------------------------
# Reference File Paths
# ------------------------------------------

ref_paths = ref_data(cfg)

joint_colors_excel_path = f"{ref_base_path}{ref_paths['joint_colors_path']}"
ref_path_stage0_excel_path = f"{ref_base_path}{ref_paths['ref_path_stage0_required_columns']}"
required_weld_features_csv_path = f"{ref_base_path}{ref_paths['reference_path']}"
wps_excel_path = f"{ref_base_path}{ref_paths['wps_path_a']}"

# ------------------------------------------
# Logging Summary
# ------------------------------------------

logger.info("Configuration and paths initialized successfully.")
logger.info(f"Stage0 Path: {robot_data_stage0_path}")
logger.info(f"Stage1 Path: {robot_data_stage1_path}")
logger.info(f"Stage2 Path: {robot_data_stage2_path}")
logger.info(f"WPS Ref Path: {wps_ref_path}")
logger.info(f"Joint Colors Excel: {joint_colors_excel_path}")

# COMMAND ----------

# ==========================================
# Utility Functions – Consolidated
# ==========================================

# ------------------------------------------
# 1️⃣ Column Normalization
# ------------------------------------------

def normalize_columns(df: DataFrame) -> DataFrame:
    def normalize(col_name: str) -> str:
        col = col_name.lower()
        col = re.sub(r"[ \-\.\/]+", "_", col)
        col = re.sub(r"_+", "_", col)
        return col.strip("_")

    return df.toDF(*[normalize(c) for c in df.columns])


# ------------------------------------------
# 2️⃣ Pandas → Spark Schema Mapper
# ------------------------------------------

def pandas_to_spark_schema(pdf: pd.DataFrame) -> StructType:
    fields = []

    for col, dtype in pdf.dtypes.items():
        if dtype == "int64":
            spark_type = LongType()
        elif dtype == "float64":
            spark_type = DoubleType()
        else:
            spark_type = StringType()

        fields.append(StructField(col, spark_type, True))

    return StructType(fields)


# ------------------------------------------
# 3️⃣ Excel PDF Sanitizer
# ------------------------------------------

def sanitize_excel_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize Excel-imported Pandas DF for Spark + Arrow compatibility.
    """
    clean = pdf.copy()

    for col in clean.columns:
        dtype = clean[col].dtype

        if dtype in ["int64", "float64"]:
            continue

        clean[col] = (
            clean[col]
            .astype(str)
            .replace({
                "nan": None,
                "NaN": None,
                "None": None,
                "": None
            })
        )

    return clean


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


# ------------------------------------------
# 5️⃣ Excel Path → Spark DataFrame
# ------------------------------------------

def excel_path_to_spark_df(spark, path: str) -> DataFrame:
    pdf = read_excel_from_abfss(spark, path)
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    sdf = spark.createDataFrame(pdf, schema=schema)
    return normalize_columns(sdf)

# COMMAND ----------

# ------------------------------------------
# Load Reference Data + Persist as Delta
# ------------------------------------------

# Load Excel-based reference data
excel_refs = {
    "joint_colors": (joint_colors_excel_path, joint_colors_path),
    "required_stage0": (ref_path_stage0_excel_path, None),  # kept in memory only
    "wps": (wps_excel_path, wps_ref_path),
}

loaded_excels = {}

for name, (src_path, dest_path) in excel_refs.items():
    logger.info(f"Loading Excel: {src_path}")
    df = excel_path_to_spark_df(spark, src_path)
    loaded_excels[name] = df

    if dest_path:
        logger.info(f"Writing Delta (overwrite): {dest_path}")
        df.write.format("delta").mode("overwrite").save(dest_path)

# Load CSV-based reference data
logger.info(f"Loading CSV: {required_weld_features_csv_path}")
ref_df = (
    spark.read
    .option("header", "true")
    .csv(required_weld_features_csv_path)
)

logger.info(f"Writing Delta (overwrite): {weld_ref_path}")
ref_df.write.format("delta").mode("overwrite").save(weld_ref_path)

# COMMAND ----------

# ==========================================
# Stage 0 – CSV Ingestion
# ==========================================
pipeline0.run(
    spark=spark,
    input_path="abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/igm6_29fjan_4feb.csv",
    output_path=robot_data_stage0_path
)

# COMMAND ----------

# ==========================================
# Stage 1 – Data Quality
# ==========================================
df_stage1 = pipeline1.run_stage1_pipeline(
    spark=spark,
    input_path=robot_data_stage0_path,
    ref_weld_path=weld_ref_path,
    output_path=robot_data_stage1_path
)
# display(df_stage1.limit(10))

# COMMAND ----------

# ==========================================
# Stage 2 – Data Enrichment
# ==========================================
df_stage2 = pipeline2.run_pipeline2(
    spark=spark,
    input_path=robot_data_stage1_path,
    wps_ref_path=wps_ref_path,
    tps500current=tps500current,
    techdevisrunning=techdevisrunning,
    output_path=robot_data_stage2_path
)
# display(df_stage2.limit(10))

# COMMAND ----------

# ==========================================
# Stage 3 – Plot Generation
# ==========================================
pipeline3.run_pipeline3_generate_plots(
    spark=spark,
    input_path=robot_data_stage2_path,
    image_output_basepath=image_output_basepath,
    joint_colors_path=joint_colors_path,
    robot_name=ROBOT_ID,
    welding_active_col="arcseam_isactive",
    activetask_col="d_activetask_f"
)

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 9️⃣ Execute Stage 4 – Image Transfer
# MAGIC # ------------------------------------------
# MAGIC
# MAGIC # Widgets live here only
# MAGIC dbutils.widgets.text("source", "/Volumes/factoryiq_iot/weld_images/images", "Source Folder")
# MAGIC dbutils.widgets.text("dest", "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/weld_path_images", "Destination Folder")
# MAGIC dbutils.widgets.dropdown("dry_run", "false", ["true","false"], "Dry Run")
# MAGIC
# MAGIC source = dbutils.widgets.get("source")
# MAGIC dest   = dbutils.widgets.get("dest")
# MAGIC dry_run = dbutils.widgets.get("dry_run") == "true"
# MAGIC
# MAGIC pipeline4.run_pipeline4_transfer_images(
# MAGIC     source=source,
# MAGIC     dest=dest,
# MAGIC     dry_run=dry_run
# MAGIC )
# MAGIC

# COMMAND ----------

# ------------------------------------------
# 9️⃣ Execute Stage 4 – Image Transfer
# ------------------------------------------

dbutils.widgets.text("source", "/Volumes/factoryiq_iot/weld_images/images")
dbutils.widgets.text("dest", "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/weld_path_images")

source = dbutils.widgets.get("source")
dest   = dbutils.widgets.get("dest")

# Default to False for production safety
dry_run = False

pipeline4.run_pipeline4_transfer_images(
    source=source,
    dest=dest,
    dry_run=dry_run
)

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install fsspec adlfs openpyxl

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.library.restartPython()