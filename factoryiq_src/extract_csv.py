# Databricks notebook source
import importlib
from config import config_paths as config_paths
importlib.reload(config_paths)

from core import logging_config as logcfg; importlib.reload(logcfg)

# Choose verbosity per run (1 | 2 | 3)
app_logger = logcfg.setup_logging(verbosity=1)  # 2 is DEBUG + INFO
logger = logcfg.get_module_logger(__name__)

# from core.single_csv_exporter import export_single_csv
from core import single_csv_exporter as singlecsvexporter; importlib.reload(singlecsvexporter)

# COMMAND ----------

# MAGIC %skip
# MAGIC # -----------------------------------------------
# MAGIC # 9️⃣ wps extract - only for development
# MAGIC # -----------------------------------------------
# MAGIC
# MAGIC res = export_single_csv(
# MAGIC     input_ref=config_paths.wps_path,  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=config_paths.outputs_dir,
# MAGIC     filename_prefix="wps_extract",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=25_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)

# COMMAND ----------

# -----------------------------------------------
# 9️⃣ Bronze Stage Extract - only for development
# -----------------------------------------------

res = singlecsvexporter.export_single_csv(
    input_ref=config_paths.bronze_path,  # or any ABFSS path / catalog table
    output_dir_abfss=config_paths.outputs_dir,
    filename_prefix="bronze_stage_extract",
    read_as="delta",
    row_limit=1000,          # try a small slice first
    compute_count=True,
)
# print(res)


# COMMAND ----------

# MAGIC %skip
# MAGIC # -----------------------------------------------
# MAGIC # 9️⃣ Silver Stage Extract - only for development
# MAGIC # -----------------------------------------------
# MAGIC
# MAGIC res = export_single_csv(
# MAGIC     input_ref=config_paths.silver_path,  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=config_paths.outputs_dir,
# MAGIC     filename_prefix="silver_stage_extract",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=25_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC # -----------------------------------------------
# MAGIC # 9️⃣ gold Stage Extract - only for development
# MAGIC # -----------------------------------------------
# MAGIC
# MAGIC res = singlecsvexporter.export_single_csv(
# MAGIC     input_ref=config_paths.gold_path,  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=config_paths.outputs_dir,
# MAGIC     filename_prefix="gold_stage_extract",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=10_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC # -----------------------------------------------
# MAGIC # 9️⃣ gold Stage Manifest Extract - only for development
# MAGIC # -----------------------------------------------
# MAGIC
# MAGIC res = singlecsvexporter.export_single_csv(
# MAGIC     input_ref=f"{config_paths.gold_path}_manifest",  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=config_paths.outputs_dir,
# MAGIC     filename_prefix="gold_stage_manifest_extract",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=10_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC # --------------------------------------------------
# MAGIC # 9️⃣ gold wire feed Extract - only for development
# MAGIC # --------------------------------------------------
# MAGIC
# MAGIC res = export_single_csv(
# MAGIC     input_ref=config_paths.gold_wirefeed_path,  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=config_paths.outputs_dir,
# MAGIC     filename_prefix="gold_wirefeed_extract",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=10_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC res = export_single_csv(
# MAGIC     input_ref=config_paths.joint_detection_path,  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=config_paths.outputs_dir,
# MAGIC     filename_prefix="joint_detection",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=10_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)
# MAGIC

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

# MAGIC %skip
# MAGIC # (Optional) quick peek
# MAGIC # display(df_stage3.limit(20))
# MAGIC
# MAGIC # (Optional) export a small CSV extract for validation
# MAGIC _ = export_single_csv(
# MAGIC     input_ref=robot_data_stage3_path,
# MAGIC     output_dir_abfss=output_csv_path,
# MAGIC     filename_prefix="stage3_joint_extract",
# MAGIC     read_as="delta",
# MAGIC     row_limit=25_000,      # adjust if needed
# MAGIC     compute_count=True,
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC res = export_single_csv(
# MAGIC     input_ref=wire_episodes_path,  # or any ABFSS path / catalog table
# MAGIC     output_dir_abfss=output_csv_path,
# MAGIC     filename_prefix="wire_episode_extract",
# MAGIC     read_as="delta",
# MAGIC     # row_limit=10_000,          # try a small slice first
# MAGIC     compute_count=True,
# MAGIC )
# MAGIC # print(res)