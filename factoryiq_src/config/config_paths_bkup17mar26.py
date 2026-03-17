


CSV_ACCOUNT     = "sarawrobotcsvfiles"
CSV_CONTAINER   = "csv-files-from-welding-robot"

# ACCOUNT     = "stfactoryiqdevadls"
ACCOUNT       = dbutils.secrets.get("factoryiq-secrets", "ACCOUNT")

CONTAINER   = "welding-data"

TABLE_ROOT  = "lake"   

ROBOT_ID    = "igm6_29fjan_4feb"
# ROBOT_ID = "april_igm2"

# ======================
# Global / run controls
# ======================
VERBOSITY = 1  # 1=INFO, 2=DEBUG, 3=TRACE (if you support it)

# base = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/{ROBOT_ID}/robot_data"
csvbase = f"abfss://{CSV_CONTAINER}@{CSV_ACCOUNT}.dfs.core.windows.net"

base = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net"

input_csv_path  = f"{csvbase}/{ROBOT_ID}.csv" # robot csv file path

inflow_dir    = f"{base}/inflow/"              # kept intact by this purge
# splitfiles_dir    = f"{base}/splitfiles/"              # kept intact by this purge
splitfiles_dir    = f"{base}/inflow/"              # kept intact by this purge
archive_dir   = f"{base}/archive/"             # will be deleted entirely
outputs_dir   = f"{csvbase}/output_files/"        # will be deleted entirely
tables_root   = f"{base}/{TABLE_ROOT}"        # parent of bronze/silver/manifest etc.

bronze_path = f"{tables_root}/bronze" # processed data path - bronze layer
silver_path = f"{tables_root}/silver" # processed data path - silver layer
gold_path = f"{tables_root}/gold" # processed data path - gold layer

# Choose output locations for the enriched rows and episodes
gold_wirefeed_path     = f"{tables_root}/gold_wire_consumption" # processed gold data with wire consumption fields
gold_wirefeed_episodes_path = f"{tables_root}/gold_wire_consumption_episodes" # processed wire episodes data

joint_detection_path     = f"{tables_root}/joint_detection" # processed gold data with wire consumption fields

# image_output_basepath = f"{tables_root}/weld_images" # path for weld images
temp_path_images = "/Volumes/factoryiq_iot/weld_images/images"
dest_images = f"{tables_root}/weld_images" # processed data path - gold layer

ref_path_csv_excel = f"abfss://{CSV_CONTAINER}@{CSV_ACCOUNT}.dfs.core.windows.net"
ref_path = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net"

joint_colors_excel_path = f"{ref_path_csv_excel}/ref_csv_excel/joint_colors.xlsx"
wps_excel_path = f"{ref_path_csv_excel}/ref_csv_excel/wps_order_v5_actual.xlsx"
required_weld_features_csv_path = f"{ref_path_csv_excel}/ref_csv_excel/weld_features_raw.csv"

joint_colors_path = f"{ref_path}/ref_tables/joint_colors_table"
wps_path = f"{ref_path}/ref_tables/wps_reference_table"
weld_features_path = f"{ref_path}/ref_tables/welddata_reference_table"

techdevisrunning = "arcseam_isactive"
tps500current   = "tps500i_current"
time_col = "time"
wfs_col = "tps500i_wire_speed"
activetask_col = "d_activetask_f"