# Databricks notebook source
import yaml
import os

# COMMAND ----------

# MAGIC %skip
# MAGIC import yaml
# MAGIC
# MAGIC CONFIG_PATH = "/Workspace/config/welddata_dev.yml"
# MAGIC # CONFIG_PATH = "../config/welddata_dev.yml"
# MAGIC # ../common/data_utils
# MAGIC
# MAGIC with open(CONFIG_PATH, "r") as f:
# MAGIC     cfg = yaml.safe_load(f)
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC CATALOG = cfg["catalog"]
# MAGIC SCHEMA = cfg["schema"]
# MAGIC
# MAGIC WPS_TABLE_ACTUAL = f"`{CATALOG}`.`{SCHEMA}`.{cfg['wps_tables']['reference_actual']}"
# MAGIC
# MAGIC # print(WPS_TABLE_ACTUAL)

# COMMAND ----------

# MAGIC %skip
# MAGIC /Volumes/factoryiq_catalog/factoryiq-csv-excel-schema/config-volume/welddata_dev.yml

# COMMAND ----------

# # CONFIG_PATH = "/Volumes/factoryiq_catalog/factoryiq-csv-excel-schema/config-volume/welddata_dev.yml"
# CONFIG_PATH = "abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/welddata_dev.yml"

# if not os.path.exists(CONFIG_PATH):
#     raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH}")

# with open(CONFIG_PATH, "r") as f:
#     cfg = yaml.safe_load(f)


# ADLS_PATH = "abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/"
ADLS_PATH = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/config/"
CONFIG_PATH = ADLS_PATH + "welddata_dev.yml"
# CONFIG_PATH = REF_BASE_PATH + "welddata_dev.yml"


df = spark.read.text(CONFIG_PATH)
cfg_text = "\n".join(r.value for r in df.collect())
cfg = yaml.safe_load(cfg_text)



CATALOG = cfg["catalog"]
SCHEMA = cfg["schema"]
REF_SCHEMA = cfg["ref_schema"]
# OUTPUT = cfg["output"]

### reference data path
REF_BASE_PATH = f"{cfg['reference_data']['base_path']}"

REF_WPS_ACTUAL = f"/ref_csv_excel/{cfg['reference_data']['wps_actual_excel']}"
REF_REQUIRED_WELD_FEATURES = f"/ref_csv_excel/{cfg['reference_data']['required_weld_features']}"
REF_JOINT_COLORS = f"/ref_csv_excel/{cfg['reference_data']['joint_colors']}"
REF_STAGE0_REQUIRED_COLUMNS = f"/ref_csv_excel/{cfg['reference_data']['stage0_required_columns']}"

### robot data path
ROBOT_BASE_PATH = f"{cfg['robot_data']['base_path']}"
# ROBOT_DATA = f"{cfg['robot_data']['csv_file']}"
# ROBOT_DATA = f"{cfg['robot_data']['csv_month']}{'_igm2'}"
ROBOT_DATA = "igm6_29fjan_4feb"
# ROBOT_DATA = "igm2_29jan_4feb"
# ROBOT_DATA = f"{cfg['robot_data']['csv_month']}{'_igm6'}"
ROBOT_DATA_CSV = f"{ROBOT_DATA}{'.csv'}"
# ROBOT_DATA_CSV = "igm6_20day_january.csv"


### reference delta tables
WPS_TABLE_ACTUAL_PATH = (REF_BASE_PATH.rstrip("/") + f"/ref_tables/{cfg['ref_tables']['wps']}")
REQUIRED_WELD_FEATURES_TABLE_PATH = (REF_BASE_PATH.rstrip("/") + f"/ref_tables/{cfg['ref_tables']['required_weld_features']}")
JOINT_COLORS_TABLE_PATH = (REF_BASE_PATH.rstrip("/") + f"/ref_tables/{cfg['ref_tables']['joint_colors']}")
REQUIRED_STAGE0_COLUMNS_TABLE_PATH = (REF_BASE_PATH.rstrip("/") + f"/ref_tables/{cfg['ref_tables']['required_stage0_columns']}")
# WPS_TABLE_MODIFIED = f"`{CATALOG}`.`{SCHEMA}`.{cfg['wps_tables']['reference_modified']}"

### raw data columns 
TIME_COL = f"{cfg['welddata_columns']['time_col']}"
TCP_X_COL = f"{cfg['welddata_columns']['tcp_x_col']}"
TCP_Y_COL = f"{cfg['welddata_columns']['tcp_y_col']}"
TCP_Z_COL = f"{cfg['welddata_columns']['tcp_z_col']}"
TECHDEV_ISRUNNING_COL = f"{cfg['welddata_columns']['techdev_isrunning_col']}"
ACTIVETASK_COL = f"{cfg['welddata_columns']['activetask_col']}"
TPS500_CURRENT_COL = f"{cfg['welddata_columns']['tps500i_current_col']}"
JOINT_COL  = f"{cfg['welddata_columns']['tcp_z_col']}"    # check this later

### derived data columns 
WELDING_ACTIVE_COL = f"{cfg['derived_welddata_columns']['der_is_welding_active']}"
STEP_MAJOR_COL = f"{cfg['derived_welddata_columns']['der_step_major']}"
DER_STEP_MAJOR_FILLED_COL = f"{cfg['derived_welddata_columns']['der_step_major_filled']}"
DER_MACHINE_STATE_COL = f"{cfg['derived_welddata_columns']['der_machine_state']}"
DER_ACTIVETASKNUM_COL = f"{cfg['derived_welddata_columns']['der_activetask_num_col']}"
DER_ACTIVETASKNUM_FCOL = f"{cfg['derived_welddata_columns']['der_activetask_num_filled_col']}"


# STAGE0_TABLE = f"`{CATALOG}`.`{SCHEMA}`.{cfg['tables']['stage0']}{'_'}{ROBOT_DATA}"
STAGE0_TABLE = (REF_BASE_PATH.rstrip("/") + f"/robot_data/robot_data_tables/{cfg['tables']['stage0']}{'_'}{ROBOT_DATA}")
STAGE1_TABLE = (REF_BASE_PATH.rstrip("/") + f"/robot_data/robot_data_tables/{cfg['tables']['stage1']}{'_'}{ROBOT_DATA}")
STAGE2_TABLE = (REF_BASE_PATH.rstrip("/") + f"/robot_data/robot_data_tables/{cfg['tables']['stage2']}{'_'}{ROBOT_DATA}")
STAGE3_TABLE = (REF_BASE_PATH.rstrip("/") + f"/robot_data/robot_data_tables/{cfg['tables']['stage3']}{'_'}{ROBOT_DATA}")
STAGE4_TABLE = (REF_BASE_PATH.rstrip("/") + f"/robot_data/robot_data_tables/{cfg['tables']['stage4']}{'_'}{ROBOT_DATA}")
STAGE5_TABLE = (REF_BASE_PATH.rstrip("/") + f"/robot_data/robot_data_tables/{cfg['tables']['stage5']}{'_'}{ROBOT_DATA}")
STAGE3_WINDOW_TABLE = f"`{CATALOG}`.`{SCHEMA}`.{cfg['tables']['stage3_window']}{'_'}{ROBOT_DATA}"
STAGE3_WINDOW_CLEANJOINT_TABLE = f"`{CATALOG}`.`{SCHEMA}`.{cfg['tables']['stage3_filtered_cleanjoint']}{'_'}{ROBOT_DATA}"
STAGE3_WINDOW_DUMMYJOINTS_TABLE = f"`{CATALOG}`.`{SCHEMA}`.{cfg['tables']['stage3_window_dummyjoints']}"


# COMMAND ----------

IMAGE_BASEPATH = f"{cfg['output']['image_path']}"

# IMAGE_WPS_SUFFIX = f"{cfg['output']['image_suffix']['wps']}"
WPS_JOINT = f"{cfg['columns']['joint']['wps']}"

# IMAGE_DUMMY_SUFFIX = f"{cfg['output']['image_suffix']['dummy']}"
DUMMY_JOINT = f"{cfg['columns']['joint']['dummy']}"

# COMMAND ----------

# MAGIC %skip
# MAGIC CONFIG_PATH = (
# MAGIC     "/Workspace/Users/ayush26071974@gmail.com/"
# MAGIC     "hds_iot/weld_data_process_2jan26/config/welddata_filtered_dev.yml"
# MAGIC )
# MAGIC
# MAGIC if not os.path.exists(CONFIG_PATH):
# MAGIC     raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH}")
# MAGIC
# MAGIC with open(CONFIG_PATH, "r") as f:
# MAGIC     cfg = yaml.safe_load(f)
# MAGIC
# MAGIC DATA_START_TIME = f"{cfg['filtered_datasets']['time_window']['start_ts']}"
# MAGIC DATA_END_TIME = f"{cfg['filtered_datasets']['time_window']['end_ts']}"
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC PLOT_CONFIG_PATH = (
# MAGIC     "/Workspace/Users/ayush26071974@gmail.com/"
# MAGIC     "hds_iot/weld_data_process_2jan26/config/weldplot_dev/plot.yml"
# MAGIC )
# MAGIC
# MAGIC if not os.path.exists(PLOT_CONFIG_PATH):
# MAGIC     raise FileNotFoundError(f"Config file not found at: {PLOT_CONFIG_PATH}")
# MAGIC
# MAGIC with open(PLOT_CONFIG_PATH, "r") as f:
# MAGIC     plot_cfg = yaml.safe_load(f)
# MAGIC
# MAGIC PLOT_NAME = f"{plot_cfg['plot']['name']}"
# MAGIC PLOT_ENV = f"{plot_cfg['plot']['environment']}"
# MAGIC PLOT_DESC = f"{plot_cfg['plot']['description']}"
# MAGIC
# MAGIC JOINT_LABEL_ENABLE = f"{plot_cfg['joint_labels']['enabled']}"
# MAGIC JOINT_LABEL_FONT_SIZE = f"{plot_cfg['joint_labels']['font_size']}"
# MAGIC JOINT_LABEL_Z_OFFSET = f"{plot_cfg['joint_labels']['z_offset']}"
# MAGIC JOINT_LABEL_COLOR = f"{plot_cfg['joint_labels']['color']}"
# MAGIC JOINT_LABEL_HORIZONTAL = f"{plot_cfg['joint_labels']['horizontal_position']}"
# MAGIC JOINT_LABEL_VERTICAL = f"{plot_cfg['joint_labels']['vertical_position']}"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install pyyaml