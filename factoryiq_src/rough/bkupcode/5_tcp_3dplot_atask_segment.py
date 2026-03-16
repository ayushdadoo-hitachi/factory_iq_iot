# Databricks notebook source
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
from mpl_toolkits.mplot3d import proj3d
from matplotlib.patches import FancyArrowPatch

# COMMAND ----------

# MAGIC %run ./common/data_utils

# COMMAND ----------

# MAGIC %run ./global/global_functions

# COMMAND ----------

# =========================
# Joint label configuration
# =========================

JOINT_LABEL_XY_OFFSET   = 40    # mm (sideways offset)
JOINT_LABEL_Z_OFFSET    = 15    # mm (vertical lift)

JOINT_LABEL_FONT_SIZE   = 8
JOINT_LABEL_COLOR       = "black"

ARROW_COLOR             = "black"
ARROW_LINEWIDTH         = 1.6
ARROW_HEAD_SIZE         = 12    # mutation_scale
ARROW_ALPHA             = 0.9


# COMMAND ----------

# MAGIC %run ./common/common_functions

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

# print(IMAGE_BASEPATH)

# COMMAND ----------

df = read_data_from_tables_inabfss(spark,STAGE4_TABLE)

# COMMAND ----------

df = compute_der_euc_distance_spark(df)
df = compute_der_bigmove_spark(df, dist_threshold=150)
df = add_der_euc_seg_id(df)


# display(df.limit(5))

# COMMAND ----------

# df.write.format("delta").mode("overwrite").saveAsTable("workspace.welddata.stage5")
# df.write.format("delta").mode("overwrite").saveAsTable(STAGE5_TABLE)
write_df_to_abfss(df,STAGE5_TABLE) 

# COMMAND ----------

active_task_list = [
    int(row["d_activetask_f"])
    for row in (
        df
        .filter(F.col("d_activetask_f") != 0)
        .select("d_activetask_f")
        .distinct()
        .orderBy("d_activetask_f")
        .collect()
    )
]

print(f"[INFO] Found {len(active_task_list)} active task segments")


# COMMAND ----------

# output_suffix = IMAGE_WPS_SUFFIX
joint_type = WPS_JOINT
joint_col_name = f"{joint_type}_id"

# COMMAND ----------

jc_pdf = read_data_from_tables_inabfss(spark,JOINT_COLORS_TABLE_PATH)

# COMMAND ----------

# Load joint color reference table
joint_color_pdf = (jc_pdf.select("joint_id", "color").dropna().toPandas())

# Build lookup dict: {joint_id: "#hex"}
JOINT_COLOR_MAP = {
    int(row["joint_id"]): f"#{row['color']}"
    for _, row in joint_color_pdf.iterrows()
}


# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.clusterAccessMode")

# COMMAND ----------

# Replace with one of your files:
adls_file = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/weld_path_images/tcp_plot_igm6_29fjan_4feb_15354.png"

# Copy back from ADLS to a temp local (DBFS) file
dbutils.fs.cp(adls_file, "file:/dbfs/tmp/_probe.png", recurse=False)

# Check PNG magic bytes: should start with 89 50 4E 47 (\x89PNG)
with open("/dbfs/tmp/_probe.png", "rb") as f:
    head = f.read(8)
print(head)  # expect: b'\x89PNG\r\n\x1a\n'

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3_clean_joint;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3;