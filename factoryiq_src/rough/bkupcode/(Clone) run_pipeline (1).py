# Databricks notebook source
# ==========================================
# Stage 1 – Data Quality Pipeline Runner
# ==========================================

# COMMAND ----------

import sys
from pyspark.sql import DataFrame
import re

# COMMAND ----------



# ------------------------------------------
# 1️⃣ Setup Project Path
# ------------------------------------------


PROJECT_PATH = "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"
ref_base_path = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/ref_csv_excel/"

if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

print("Project path configured.")

# COMMAND ----------

# MAGIC %skip
# MAGIC import enrichments.task_features as tf
# MAGIC
# MAGIC print("Loaded from:", tf.__file__)
# MAGIC print("Functions available:", dir(tf))

# COMMAND ----------



# COMMAND ----------

# ------------------------------------------
# 2️⃣ Imports
# ------------------------------------------
import logging
import importlib
import pandas as pd

import config.config_loader as config_loader
import config.config_builder as config_builder
importlib.reload(config_builder)

# import core.excel_utils as ceu
# importlib.reload(ceu)

from config.config_loader import load_config
from config.config_builder import build_paths, build_weldcoldnames, ref_data

from pipelines.pipeline_reference_data_load import run

import pipelines.pipeline0_csv_ingestion as pipeline0
importlib.reload(pipeline0)

from pipelines.pipeline0_csv_ingestion import run

import pipelines.pipeline1_data_quality as pipeline1

import pipelines.pipeline2_data_enrichment as pipeline2
importlib.reload(pipeline2)

import pipelines.pipeline3_generate_plots as pipeline3
importlib.reload(pipeline3)

import pipelines.pipeline4_transfer_images as pipeline4
importlib.reload(pipeline4)

# COMMAND ----------

# ------------------------------------------
# 3️⃣ Logging Configuration
# ------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


# COMMAND ----------

# ------------------------------------------
# 4️⃣ Configuration
# ------------------------------------------
CONFIG_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/config/welddata_dev.yml"
ROBOT_ID = "igm6_29fjan_4feb"

# COMMAND ----------

# ------------------------------------------
# 5️⃣ Load Config & Build Paths
# ------------------------------------------
cfg = load_config(spark, CONFIG_PATH)

paths = build_paths(cfg)

robot_data_stage0_path = f"{paths['STAGE0_TABLE']}_{ROBOT_ID}"
robot_data_stage1_path = f"{paths['STAGE1_TABLE']}_{ROBOT_ID}"
robot_data_stage2_path = f"{paths['STAGE2_TABLE']}_{ROBOT_ID}"
weld_ref_path = paths["REF_REQUIRED_WELD_FEATURES"]
wps_ref_path = paths["REF_WPS_FEATURES"]
joint_colors_path = paths["JOINT_COLORS"]
image_output_basepath = paths["IMAGE_BASEPATH"]

# logger.info(f"Reference table path: {wps_ref_path}")
# logger.info(f"joint_colors_path: {joint_colors_path}")

# COMMAND ----------

weldcol_names = build_weldcoldnames(cfg)

techdevisrunning = f"{weldcol_names['TECHDEV_ISRUNNING_COL']}"
tps500current = f"{weldcol_names['TPS500_CURRENT_COL']}"

# logger.info(f"techdevisrunning: {techdevisrunning}")
# logger.info(f"tps500current: {tps500current}")

# COMMAND ----------

ref_paths = ref_data(cfg)

joint_colors_excel = ref_paths["joint_colors_path"]
joint_colors_excel_path = f"{ref_base_path}{joint_colors_excel}"
logger.info(f"joint_colors_path: {joint_colors_excel_path}")

ref_path_stage0_excel = ref_paths["ref_path_stage0_required_columns"]
ref_path_stage0_excel_path = f"{ref_base_path}{ref_path_stage0_excel}"
logger.info(f"ref_path_stage0_required_columns: {ref_path_stage0_excel_path}")

required_weld_features_csv = ref_paths["reference_path"]
required_weld_features_csv_path = f"{ref_base_path}{required_weld_features_csv}"
logger.info(f"required_weld_features_path: {required_weld_features_csv_path}")

wps_excel = ref_paths["wps_path_a"]
wps_excel_path = f"{ref_base_path}{wps_excel}"
logger.info(f"wps_path_a: {wps_excel_path}")


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

def excel_path_to_spark_df(spark, path: str):
    print(path)
    pdf = read_excel_from_abfss(spark, path)
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    sdf = spark.createDataFrame(pdf, schema=schema)
    return normalize_columns(sdf)

# COMMAND ----------

# MAGIC %skip
# MAGIC bin_df = spark.read.format("binaryFile").load(joint_colors_excel_path)
# MAGIC
# MAGIC display(bin_df.select("path", "length"))

# COMMAND ----------

# from core.excel_utils import excel_path_to_spark_df

joint_colors_df = excel_path_to_spark_df(spark, joint_colors_excel_path)
required_stage0_df = excel_path_to_spark_df(spark, ref_path_stage0_excel_path)
wps_df = excel_path_to_spark_df(spark, wps_excel_path)

# COMMAND ----------

ref_df = spark.read.option("header", "true").csv(required_weld_features_csv_path)

# COMMAND ----------

# Map each df to its destination path
writes = [
    (joint_colors_df, joint_colors_path),
    (wps_df, wps_ref_path),
    (ref_df, weld_ref_path)
]

for df, out_path in writes:
    print(f"Writing Delta (overwrite): {out_path}")
    df.write.format("delta").mode("overwrite").save(out_path)

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 6️⃣ Execute load reference data Pipeline
# MAGIC # ------------------------------------------
# MAGIC
# MAGIC # ----------------------------
# MAGIC # 3️⃣ Run Pipeline
# MAGIC # ----------------------------
# MAGIC run(
# MAGIC     spark=spark,
# MAGIC     joint_colors_pdf=joint_colors_pdf,
# MAGIC     required_stage0_pdf=required_stage0_pdf,
# MAGIC     wps_pdf=wps_pdf,
# MAGIC     reference_csv_path=reference_path,
# MAGIC     output_paths=output_paths
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 6️⃣ Execute Stage 0 Pipeline
# MAGIC # ------------------------------------------
# MAGIC run(
# MAGIC     spark=spark,
# MAGIC     input_path="abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/igm6_29fjan_4feb.csv",
# MAGIC     output_path=robot_data_stage0_path
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 6️⃣ Execute Stage 1 Pipeline
# MAGIC # ------------------------------------------
# MAGIC df = pipeline1.run_stage1_pipeline(
# MAGIC     spark=spark,
# MAGIC     input_path=robot_data_stage0_path,
# MAGIC     ref_weld_path=weld_ref_path,
# MAGIC     output_path=robot_data_stage1_path
# MAGIC )
# MAGIC
# MAGIC # display(df.limit(10))

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 6️⃣ Execute Stage 2 Pipeline
# MAGIC # ------------------------------------------
# MAGIC df = pipeline2.run_pipeline2(
# MAGIC     spark=spark,
# MAGIC     input_path=robot_data_stage1_path,
# MAGIC     wps_ref_path=wps_ref_path,
# MAGIC     tps500current=tps500current,
# MAGIC     techdevisrunning=techdevisrunning,
# MAGIC     output_path=robot_data_stage2_path
# MAGIC )
# MAGIC
# MAGIC # print(df)
# MAGIC display(df.limit(10))

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # 8️⃣ Execute Stage 3 – Plot Generation
# MAGIC # ------------------------------------------
# MAGIC pipeline3.run_pipeline3_generate_plots(
# MAGIC     spark=spark,
# MAGIC     input_path=robot_data_stage2_path,  # Stage2 is enriched
# MAGIC     image_output_basepath=image_output_basepath,
# MAGIC     joint_colors_path=joint_colors_path,
# MAGIC     robot_name=ROBOT_ID,
# MAGIC     welding_active_col="arcseam_isactive",
# MAGIC     activetask_col="d_activetask_f"
# MAGIC )
# MAGIC
# MAGIC logger.info("Pipeline 3 completed successfully")

# COMMAND ----------

# ------------------------------------------
# 9️⃣ Execute Stage 4 – Image Transfer
# ------------------------------------------

# Widgets live here only
dbutils.widgets.text("source", "/Volumes/factoryiq_iot/weld_images/images", "Source Folder")
dbutils.widgets.text("dest", "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/weld_path_images", "Destination Folder")
dbutils.widgets.dropdown("dry_run", "false", ["true","false"], "Dry Run")

source = dbutils.widgets.get("source")
dest   = dbutils.widgets.get("dest")
dry_run = dbutils.widgets.get("dry_run") == "true"

pipeline4.run_pipeline4_transfer_images(
    source=source,
    dest=dest,
    dry_run=dry_run
)

logger.info("Pipeline 4 completed successfully")

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install fsspec adlfs openpyxl

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.library.restartPython()