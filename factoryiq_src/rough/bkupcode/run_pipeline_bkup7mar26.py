# Databricks notebook source
# ---- Project path (run before imports) ----
import sys

PROJECT_PATH = "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

print("Project path configured:", PROJECT_PATH)

# COMMAND ----------

# =============
# Imports 
# =============
import importlib   
import logging               

from pyspark.sql import functions as F

from core import cleaning_utils as cleaningutils; importlib.reload(cleaningutils) # --- cleaning utils
from core import logging_config as loggingsetup; importlib.reload(loggingsetup) # --- logging setup 
from config import config_paths as config_paths; importlib.reload(config_paths) # --- Config 

# --- Pipelines imports ---
from pipelines import bronze_ingest as bronze; importlib.reload(bronze)
from pipelines import silver_data_quality as silver; importlib.reload(silver)
from pipelines import gold_data_enrichment as gold; importlib.reload(gold)
from pipelines import generate_plots as plots; importlib.reload(plots)
from pipelines import transfer_images_to_adls as images_to_adls; importlib.reload(images_to_adls)
from pipelines import pipeline3_joint_detection as jointdetection; importlib.reload(jointdetection)


# COMMAND ----------

loggingsetup.setup_logging()
logger = logging.getLogger(__name__)

# COMMAND ----------

# ==========================================
# load reference data
# ==========================================

df = cleaningutils.excel_path_to_spark_df(spark, config_paths.joint_colors_excel_path)
logger.info(f"Writing Delta (overwrite): {config_paths.joint_colors_path}")
df.write.format("delta").mode("overwrite").save(config_paths.joint_colors_path)

df = cleaningutils.excel_path_to_spark_df(spark, config_paths.wps_excel_path)
logger.info(f"Writing Delta (overwrite): {config_paths.wps_path}")
df.write.format("delta").mode("overwrite").save(config_paths.wps_path)

df = (spark.read.option("header", "true").csv(config_paths.required_weld_features_csv_path))
logger.info(f"Writing Delta (overwrite): {config_paths.weld_features_path}")
df.write.format("delta").mode("overwrite").save(config_paths.weld_features_path)

# COMMAND ----------

# ==========================================
# bronze - data ingestion
# ==========================================
processed = bronze.run_stage(
    spark,
    inflow_dir=config_paths.inflow_dir,
    bronze_path=config_paths.bronze_path,
    archive_dir=config_paths.archive_dir,
    # limit_files=5,                     # dev: only first 5
    add_batch_id=True,                 # traceability
    use_manifest=True,                 # avoid duplicates
    manifest_path=f"{config_paths.bronze_path}_manifest",
    move_to_date_partition=True        # organize archive under dt=YYYY-MM-DD
)

print("Processed files:\n", "\n".join(processed))

# COMMAND ----------

# ==========================================
# silver - data quality
# ==========================================

silver_df = silver.run_stage(
    spark=spark,
    input_path=config_paths.bronze_path,
    ref_weld_path=config_paths.weld_features_path,
    output_path=config_paths.silver_path
)

print("Silver Stage row count =", silver_df.count())

# display(silver_df.limit(10))

# COMMAND ----------

# ==========================================
# gold - data enrichment
# ==========================================

gold_df = gold.run_stage(
    spark=spark,
    input_path=config_paths.silver_path,
    wps_ref_path=config_paths.wps_path,
    tps500current=config_paths.tps500current,
    techdevisrunning=config_paths.techdevisrunning,
    output_path=config_paths.gold_path,

    enable_wire_consumption=True,  # default True; keep explicit
    wire_episodes_output_path=config_paths.gold_wirefeed_episodes_path,

    time_col=config_paths.time_col,
    wfs_col=config_paths.wfs_col,
    wfs_unit="auto",
    write_mode="overwrite",
    round_wire_outputs=True,
    round_digits=2,
    log_counts=False,
)

# COMMAND ----------

# ==========================================
# Plot Generation
# ==========================================
plots.run_generate_plots(
    spark=spark,
    # input_path=robot_data_stage2_path,
    input_path=config_paths.gold_path,
    image_output_basepath=config_paths.temp_path_images,
    joint_colors_path=config_paths.joint_colors_path,
    robot_name=config_paths.ROBOT_ID,
    welding_active_col=config_paths.techdevisrunning,
    activetask_col=config_paths.activetask_col
)

# COMMAND ----------

# ------------------------------------------
# images transfer to adls
# ------------------------------------------
dry_run = False

images_to_adls.run_transfer_images(
    source=config_paths.temp_path_images,
    dest=config_paths.dest_images,
    dry_run=dry_run
)

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

# -----------------------------------------------------------
# Deriving fields for joint detection (using a different way)
# -----------------------------------------------------------

cfg3 = jointdetection.JointDetectConfig(
    obj_k=4,                 # A..D
    obj_min_rows_per_task=50,
    write_mode="overwrite"   # or "append" if you prefer accumulating
)

df_stage3 = jointdetection.run_pipeline3(
    spark=spark,
    input_path=config_paths.silver_path,          # your Stage-2 path
    output_path=config_paths.joint_detection_path,         # e.g., f"{tables_root}/gold"
    cfg=cfg3
)
print("Stage-3 rows:", df_stage3.count())

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