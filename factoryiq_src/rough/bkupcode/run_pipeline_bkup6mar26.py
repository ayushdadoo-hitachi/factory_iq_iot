# Databricks notebook source
# ---- Project path (run before imports) ----
import sys

PROJECT_PATH = "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

print("Project path configured:", PROJECT_PATH)

# COMMAND ----------

# ====================
# Imports 
# ====================

# --- Standard library ---
import logging
import importlib

# --- Project modules ---
# Config
import config.config_loader as config_loader
import config.config_builder as config_builder
from config.config_loader import load_config
from config.config_builder import build_paths, build_weldcoldnames, ref_data

from core.io_utils import read_delta, write_delta

# Utilities
import core.cleaning_utils as ccu
from core.cleaning_utils import excel_path_to_spark_df


# Logging (your app/logger bootstrap)
import core.logging_config as clc
from core.logging_config import setup_logging

# Pipelines
# import pipelines.pipeline0_csv_ingestion as pipeline0
# import pipelines.bronze_ingest as bronze_stage
# import pipelines.pipeline1_data_quality as pipeline1
# import pipelines.pipeline2_data_enrichment as pipeline2
import pipelines.pipeline3_generate_plots as pipeline3
import pipelines.pipeline4_transfer_images as pipeline4

# After Stage 2 has been written (robot_data_stage2_path)
# from importlib import reload
# import pipelines.pipeline3_wire_feed_consumption as pipeline3wire
# reload(pipeline3wire)

importlib.reload(config_builder)
importlib.reload(ccu)
importlib.reload(clc)
# importlib.reload(bronze_stage)
# importlib.reload(pipeline1)
# importlib.reload(pipeline2)
importlib.reload(pipeline3)
importlib.reload(pipeline4)

from pipelines.bronze_ingest import run

# --- Initialize logging ---
setup_logging()
logger = logging.getLogger(__name__)

# COMMAND ----------

# ------------------------------------------
# 4️⃣ Configuration
# ------------------------------------------
CONFIG_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/config/welddata_dev.yml"
ref_base_path = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/ref_csv_excel/"
# output_csv_path = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/output_files/"

# COMMAND ----------

from config import config_paths as config_paths
importlib.reload(config_paths)

# COMMAND ----------

# MAGIC %skip
# MAGIC ACCOUNT     = "hrlfactoryiqsa"
# MAGIC CONTAINER   = "weld-data"
# MAGIC TABLE_ROOT  = "lake"   
# MAGIC
# MAGIC ROBOT_ID    = "igm6_29fjan_4feb"
# MAGIC # ROBOT_ID = "april_igm2"
# MAGIC
# MAGIC base = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/{ROBOT_ID}/robot_data"
# MAGIC
# MAGIC inflow_dir    = f"{base}/inflow/"              # kept intact by this purge
# MAGIC archive_dir   = f"{base}/archive/"             # will be deleted entirely
# MAGIC outputs_dir   = f"{base}/output_files/"        # will be deleted entirely
# MAGIC tables_root   = f"{base}/{TABLE_ROOT}"        # parent of bronze/silver/manifest etc.
# MAGIC
# MAGIC bronze_path = f"{tables_root}/bronze" # processed data path - bronze layer
# MAGIC silver_path = f"{tables_root}/silver" # processed data path - silver layer
# MAGIC gold_path = f"{tables_root}/gold" # processed data path - gold layer
# MAGIC
# MAGIC # Choose output locations for the enriched rows and episodes
# MAGIC gold_wirefeed_path     = f"{tables_root}/gold_wire_consumption" # processed gold data with wire consumption fields
# MAGIC gold_wirefeed_episodes_path = f"{tables_root}/gold_wire_consumption_episodes" # processed wire episodes data
# MAGIC

# COMMAND ----------

# ------------------------------------------
# 5️⃣ Load Config + Build All Runtime Paths
# ------------------------------------------

# Load config
cfg = load_config(spark, CONFIG_PATH)

# Build base paths
paths = build_paths(cfg)

# robot_data_stage0_path = f"{paths['STAGE0_TABLE']}_{ROBOT_ID}"
# robot_data_stage1_path = f"{paths['STAGE1_TABLE']}_{ROBOT_ID}"
# robot_data_stage2_path = f"{paths['STAGE2_TABLE']}_{ROBOT_ID}"

# weld_ref_path = paths["REF_REQUIRED_WELD_FEATURES"]
# wps_ref_path = paths["REF_WPS_FEATURES"]
# joint_colors_path = paths["JOINT_COLORS"]
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

# ref_paths = ref_data(cfg)

# joint_colors_excel_path = f"{ref_base_path}{ref_paths['joint_colors_path']}"
# ref_path_stage0_excel_path = f"{ref_base_path}{ref_paths['ref_path_stage0_required_columns']}"
# required_weld_features_csv_path = f"{ref_base_path}{ref_paths['reference_path']}"
# wps_excel_path = f"{ref_base_path}{ref_paths['wps_path_a']}"

# ------------------------------------------
# Logging Summary
# ------------------------------------------

logger.info("Configuration and paths initialized successfully.")
# logger.info(f"Stage0 Path: {robot_data_stage0_path}")
# logger.info(f"Stage1 Path: {robot_data_stage1_path}")
# logger.info(f"Stage2 Path: {robot_data_stage2_path}")
# logger.info(f"WPS Ref Path: {wps_ref_path}")
# logger.info(f"Joint Colors Excel: {joint_colors_excel_path}")

# COMMAND ----------

df = excel_path_to_spark_df(spark, config_paths.joint_colors_excel_path)
logger.info(f"Writing Delta (overwrite): {config_paths.joint_colors_path}")
write_delta(df, config_paths.joint_colors_path)

df = excel_path_to_spark_df(spark, config_paths.wps_excel_path)
logger.info(f"Writing Delta (overwrite): {config_paths.wps_path}")
write_delta(df, config_paths.wps_path)

df = (spark.read.option("header", "true").csv(config_paths.required_weld_features_csv_path))
logger.info(f"Writing Delta (overwrite): {config_paths.weld_features_path}")
df.write.format("delta").mode("overwrite").save(config_paths.weld_features_path)

# COMMAND ----------

# MAGIC %skip
# MAGIC # ------------------------------------------
# MAGIC # Load Reference Data + Persist as Delta
# MAGIC # ------------------------------------------
# MAGIC
# MAGIC # Load Excel-based reference data
# MAGIC excel_refs = {
# MAGIC     "joint_colors": (joint_colors_excel_path, joint_colors_path),
# MAGIC     "required_stage0": (ref_path_stage0_excel_path, None),  # kept in memory only
# MAGIC     "wps": (wps_excel_path, wps_ref_path),
# MAGIC }
# MAGIC
# MAGIC loaded_excels = {}
# MAGIC
# MAGIC for name, (src_path, dest_path) in excel_refs.items():
# MAGIC     # logger.info(f"Loading Excel: {src_path}")
# MAGIC     df = excel_path_to_spark_df(spark, src_path)
# MAGIC     loaded_excels[name] = df
# MAGIC
# MAGIC     if dest_path:
# MAGIC         logger.info(f"Writing Delta (overwrite): {dest_path}")
# MAGIC         df.write.format("delta").mode("overwrite").save(dest_path)
# MAGIC
# MAGIC # Load CSV-based reference data
# MAGIC # logger.info(f"Loading CSV: {required_weld_features_csv_path}")
# MAGIC ref_df = (
# MAGIC     spark.read
# MAGIC     .option("header", "true")
# MAGIC     .csv(required_weld_features_csv_path)
# MAGIC )
# MAGIC
# MAGIC logger.info(f"Writing Delta (overwrite): {weld_ref_path}")
# MAGIC ref_df.write.format("delta").mode("overwrite").save(weld_ref_path)

# COMMAND ----------

from pipelines import bronze_ingest as bronze
importlib.reload(bronze)

processed = bronze.run_stage(
    spark,
    inflow_dir=inflow_dir,
    bronze_path=bronze_path,
    archive_dir=archive_dir,
    # limit_files=5,                     # dev: only first 5
    add_batch_id=True,                 # traceability
    use_manifest=True,                 # avoid duplicates
    manifest_path=f"{bronze_path}_manifest",
    move_to_date_partition=True        # organize archive under dt=YYYY-MM-DD
)

print("Processed files:\n", "\n".join(processed))

# COMMAND ----------

# MAGIC %skip
# MAGIC # ==========================================
# MAGIC # Stage 0 – CSV Ingestion
# MAGIC # ==========================================
# MAGIC pipeline0.run(
# MAGIC     spark=spark,
# MAGIC     # input_path="abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/april_igm2.csv",
# MAGIC     input_path="abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/igm6_29fjan_4feb.csv",
# MAGIC     output_path=robot_data_stage0_path
# MAGIC )

# COMMAND ----------

from pipelines import silver_data_quality as silver
importlib.reload(silver)

silver_df = silver.run_stage(
    spark=spark,
    input_path=bronze_path,
    ref_weld_path=weld_ref_path,
    output_path=silver_path
)

print("Silver Stage row count =", silver_df.count())

# display(silver_df.limit(10))

# COMMAND ----------

from pipelines import gold_data_enrichment as gold
importlib.reload(gold)

gold_df = gold.run_stage(
    spark=spark,
    input_path=silver_path,
    wps_ref_path=wps_ref_path,
    tps500current=tps500current,
    techdevisrunning=techdevisrunning,
    output_path=gold_path
)

print("gold Stage row count =", gold_df.count())

# display(gold_df.limit(10))

# COMMAND ----------

from pipelines import gold_wire_feed_consumption as goldwirefeed
importlib.reload(goldwirefeed)

goldwirefeed_df = goldwirefeed.run_stage(
    spark=spark,
    input_path=gold_path,
    output_path=gold_wirefeed_path,
    output_episodes_path=gold_wirefeed_episodes_path,
    time_col="time",
    wfs_col="tps500i_wire_speed",
    wfs_unit="auto",     # or 'm_per_min' if you know it explicitly
    write_mode="overwrite"
)

# COMMAND ----------

# ==========================================
# Stage 3 – Plot Generation
# ==========================================
pipeline3.run_pipeline3_generate_plots(
    spark=spark,
    # input_path=robot_data_stage2_path,
    input_path=gold_wirefeed_path,
    image_output_basepath=image_output_basepath,
    joint_colors_path=joint_colors_path,
    robot_name=ROBOT_ID,
    welding_active_col="arcseam_isactive",
    activetask_col="d_activetask_f"
)

# COMMAND ----------

# ------------------------------------------
# 9️⃣ Execute Stage 4 – Image Transfer
# ------------------------------------------

# Default to False for production safety
dry_run = False

pipeline4.run_pipeline4_transfer_images(
    source=config_paths.temp_path_images,
    dest=config_paths.dest_images,
    dry_run=dry_run
)

# COMMAND ----------

# MAGIC %skip
# MAGIC # COMMAND ----------
# MAGIC # ==========================================
# MAGIC # Stage 3 – Joint Detection + Auto Object Categorization (NO WPS)
# MAGIC # ==========================================
# MAGIC from importlib import reload
# MAGIC import pipelines.pipeline3_joint_detection as pipeline3_joints
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC reload(pipeline3_joints)
# MAGIC
# MAGIC # Choose Stage 3 output location (same pattern as Stage 2)
# MAGIC robot_data_stage3_path = f"{paths['STAGE3_TABLE']}_{ROBOT_ID}"
# MAGIC
# MAGIC # Config: set the number of categories you want (A,B,... up to N)
# MAGIC cfg3 = pipeline3_joints.JointDetectConfig(
# MAGIC     # segmentation/fingerprinting knobs keep defaults unless you want to tweak
# MAGIC     obj_k=4,                  # <<--- choose A..D (set to any N: 2,3,4,...)
# MAGIC     obj_min_rows_per_task=50  # ignore tiny tasks when learning categories
# MAGIC )
# MAGIC
# MAGIC # Run Pipeline 3
# MAGIC df_stage3 = pipeline3_joints.run_pipeline3(
# MAGIC     spark=spark,
# MAGIC     input_path=robot_data_stage2_path,
# MAGIC     output_path=robot_data_stage3_path,
# MAGIC     cfg=cfg3
# MAGIC )
# MAGIC
# MAGIC # Quick sanity: categories learned (no joint counts used)
# MAGIC display(
# MAGIC     spark.read.format("delta").load(robot_data_stage3_path)
# MAGIC          .where("der_weld_on = 1")
# MAGIC          .groupBy("der_object_type", "d_activetask_f")
# MAGIC          .agg(F.countDistinct("der_joint_seg_id").alias("seg_count"),
# MAGIC               F.count("*").alias("arc_rows"))
# MAGIC          .orderBy("der_object_type","arc_rows", ascending=[True, False])
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC # COMMAND ----------
# MAGIC # ==========================================
# MAGIC # Stage 3 – Joint Detection (NO WPS)
# MAGIC # ==========================================
# MAGIC from importlib import reload
# MAGIC import pipelines.pipeline3_joint_detection as pipeline3_joints
# MAGIC
# MAGIC # If you're iterating on the file, hot-reload it
# MAGIC reload(pipeline3_joints)
# MAGIC
# MAGIC # Choose Stage 3 output location
# MAGIC robot_data_stage3_path = f"{paths['STAGE3_TABLE']}_{ROBOT_ID}"
# MAGIC
# MAGIC # Optional: tweak detection config (keep defaults if you like)
# MAGIC cfg3 = pipeline3_joints.JointDetectConfig(
# MAGIC     seam_jump_mult=6.0,     # MAD multiplier for seam jumps
# MAGIC     seam_roll_win=5,        # seconds window for local diff stats
# MAGIC     pose_round=0.5,         # rounding for fallback posture fingerprint
# MAGIC     seam_round=1.0,         # rounding for seam fingerprint
# MAGIC     bigmove_split=True,     # split segments on der_bigmove
# MAGIC     use_eucseg_split=True,  # split on der_euc_seg_id changes
# MAGIC     write_mode="overwrite"  # or "append"
# MAGIC )
# MAGIC
# MAGIC # Run Pipeline 3
# MAGIC df_stage3 = pipeline3_joints.run_pipeline3(
# MAGIC     spark=spark,
# MAGIC     input_path=robot_data_stage2_path,
# MAGIC     output_path=robot_data_stage3_path,
# MAGIC     cfg=cfg3
# MAGIC )

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

# -------------------------------------------------------------
# 9️⃣ Generate small csv file - only for development simulation
# -------------------------------------------------------------

# Databricks notebook cell: Split large robot CSV into 10k-row chunk CSVs
# Keeps the original file structure:
#   row 1: metadata
#   row 2: header
#   row 3: metadata
#   row 4+: data rows (semicolon-delimited)
#
# It writes one CSV per chunk: chunk_00000.csv, chunk_00001.csv, ...

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime, uuid

# =========================
# ---- PARAMETERS ----------
# =========================
# >>>> IMPORTANT: Use your REAL ABFSS paths; DO NOT use <container>/<account> placeholders.
ACCOUNT     = "hrlfactoryiqsa"
CONTAINER   = "weld-data"
ROBOT_ID    = "igm6_29fjan_4feb"

base = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/{ROBOT_ID}/robot_data"

input_csv_path  = f"{base}/{ROBOT_ID}.csv" # robot csv file path
inflow_dir    = f"{base}/inflow/"

rows_per_file   = 10_000

# Basic validation to catch placeholder typos
if "<" in input_csv_path or ">" in input_csv_path or "<" in inflow_dir or ">" in inflow_dir:
    raise ValueError("Replace placeholder paths with real ABFSS paths (no <...> allowed).")

# Make sure the output base exists
dbutils.fs.mkdirs(inflow_dir)

# =========================
# 1) Read raw file as TEXT and number rows
# =========================
raw_df = spark.read.text(input_csv_path)
w = Window.orderBy(F.monotonically_increasing_id())
df_rn = raw_df.withColumn("rn", F.row_number().over(w))

# Extract the three preamble lines
row1 = df_rn.filter(F.col("rn") == 1).select("value").first()
row2 = df_rn.filter(F.col("rn") == 2).select("value").first()
row3 = df_rn.filter(F.col("rn") == 3).select("value").first()

if row2 is None or not row2["value"]:
    raise ValueError("Could not find header on row 2. Check the input CSV format.")

meta1_line   = row1["value"] if row1 and row1["value"] is not None else ""
header_line  = row2["value"]
meta3_line   = row3["value"] if row3 and row3["value"] is not None else ""

# Data rows (row 4+), kept as TEXT lines
data_only = df_rn.filter(F.col("rn") >= 4).select("value")
total_rows = data_only.count()
print(f"Total data rows (excluding the first 3 lines): {total_rows}")

if total_rows == 0:
    raise ValueError("No data rows found after row 3. Verify the input file format.")

# =========================
# 2) Assign chunk_id per 10k rows, preserving original row order
#    We reuse the original 'rn' to keep line order stable.
# =========================
data_with_chunk = (
    df_rn
    .filter(F.col("rn") >= 4)
    .select("rn", "value")
    .withColumn("chunk_id", ((F.col("rn") - 4) / rows_per_file).cast("long"))
)

chunk_ids = [r["chunk_id"] for r in data_with_chunk.select("chunk_id").distinct().collect()]
chunk_ids.sort()
print(f"Total chunks to write: {len(chunk_ids)} (each ~{rows_per_file} rows, last may be smaller)")

# =========================
# 3) For each chunk, write a SINGLE CSV file that includes:
#    line1 = original row1 metadata
#    line2 = original header line
#    line3 = original row3 metadata (or blank if absent)
#    line4+ = this chunk's data lines
# =========================
for cid in chunk_ids:
    temp_dir = f"{inflow_dir}/_chunk_{cid:05d}"
    out_file = f"{inflow_dir}/chunk_{cid:05d}.csv"

    # Create a small 3-line DataFrame for the preamble
    preamble = spark.createDataFrame(
        [(meta1_line,), (header_line,), (meta3_line,)],
        ["value"]
    )

    # The chunk's data lines (as text)
    chunk_lines = (
        data_with_chunk
        .filter(F.col("chunk_id") == cid)
        .orderBy(F.col("rn"))           # preserve original order
        .select("value")
    )

    # Combine preamble + data and write as a single text file
    combined = preamble.unionByName(chunk_lines)

    # Write to a temp folder as a single part file
    (combined
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(temp_dir))

    # Find the single part file and rename it to chunk_<id>.csv
    part_files = [f.path for f in dbutils.fs.ls(temp_dir) if f.name.startswith("part-")]
    if not part_files:
        raise RuntimeError(f"No part file found in {temp_dir}")
    dbutils.fs.mv(part_files[0], out_file)
    dbutils.fs.rm(temp_dir, recurse=True)

    print(f"✔ Wrote {out_file}")

print("✅ Split complete.")

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install fsspec adlfs openpyxl

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

print("test logging")

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{bronze_path}`").show(truncate=False)

# COMMAND ----------

manifest_path = bronze_path + "_manifest"
df_manifest = spark.read.format("delta").load(manifest_path)
display(df_manifest.orderBy(F.desc("processed_ts")))

# COMMAND ----------

for f in dbutils.fs.ls(bronze_path):
    if f.name.startswith("part-") and f.name.endswith(".parquet"):
        print(f.name, f.size)

# COMMAND ----------

df_manifest = spark.read.format("delta").load(bronze_path + "_manifest")
display(df_manifest.orderBy(F.desc("processed_ts")))