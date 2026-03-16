# Databricks notebook source
import importlib
import pipelines.stage1_data_quality_pipeline as stage1
import config.config_builder as ccp
import config.config_loader as ccl
importlib.reload(ccl)
# from config.config_loader import load_config
importlib.reload(ccp)
from config.config_builder import build_paths, build_weldcoldnames
import logging

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,  # Set to INFO to show info-level messages
    format='%(asctime)s [%(levelname)s] %(message)s'
)


# COMMAND ----------

PROJECT_PATH = "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"
CONFIG_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/config/welddata_dev.yml"
igm_robot = "igm6_29fjan_4feb"

# COMMAND ----------

# MAGIC %skip
# MAGIC %run ./common/data_utils

# COMMAND ----------

import sys

if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

print("Path added")


# COMMAND ----------

importlib.reload(stage1)

# COMMAND ----------



# COMMAND ----------


cfg = load_config(spark, CONFIG_PATH)
# print(cfg)
table_val = build_paths(cfg)
robot_data_input_path = table_val["STAGE0_TABLE"]+"_"+igm_robot
robot_data_output_path = table_val["STAGE1_TABLE"]+"_"+igm_robot
# print("hello1")
weld_ref_path = table_val["REF_REQUIRED_WELD_FEATURES"]
print(weld_ref_path)

weldcol_val = build_weldcoldnames(cfg)
# print(weldcol_val.get("TECHDEV_ISRUNNING_COL", "Key not found"))


# COMMAND ----------

# MAGIC %skip
# MAGIC ref_weld_df = stage1.run_stage1_pipeline(spark, weld_ref_path)
# MAGIC display(ref_weld_df.limit(10))

# COMMAND ----------

# MAGIC %skip
# MAGIC df = stage1.run_stage1_pipeline(spark, weld_ref_path, weld_ref_path)
# MAGIC display(df.limit(10))

# COMMAND ----------

df = stage1.run_stage1_pipeline(spark, robot_data_input_path, weld_ref_path, robot_data_output_path)
display(df.limit(10))

# COMMAND ----------

# MAGIC %skip
# MAGIC from pipelines.data_quality_pipeline import just_checking
# MAGIC
# MAGIC just_checking(spark=spark, input_path='/path/to/input')
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC df = dqp.feature_pruning(spark, REQUIRED_WELD_FEATURES_TABLE_PATH)
# MAGIC display(df)
# MAGIC