"""
config_paths.py
----------------
Loads static config.json and constructs all ABFSS paths.

Usage everywhere:
    from config_paths import config_path
    df = spark.read.csv(config_path.input_csv_path)
"""

import json
import os
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class ConfigPaths:
    """Loads static JSON config and constructs all required ABFSS paths."""

    def __init__(self, config_file="config.json"):
        spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(spark)

        # -----------------------
        # Load JSON configuration
        # -----------------------
        config_path = self._resolve_config_path(config_file)

        with open(config_path, "r") as f:
            self.cfg = json.load(f)

        # --------------------------------------
        # Resolve ACCOUNT from Databricks secrets
        # --------------------------------------
        self.ACCOUNT = self.dbutils.secrets.get(
            self.cfg["ACCOUNT_SECRET_SCOPE"],
            self.cfg["ACCOUNT_SECRET_KEY"]
        )

        # ------------------------
        # Basic identity constants
        # ------------------------
        self.CSV_ACCOUNT = self.cfg["CSV_ACCOUNT"]
        self.CSV_CONTAINER = self.cfg["CSV_CONTAINER"]
        self.CONTAINER = self.cfg["CONTAINER"]
        self.TABLE_ROOT = self.cfg["TABLE_ROOT"]
        self.ROBOT_ID = self.cfg["ROBOT_ID"]

        # ------------------------
        # Base ABFSS URLs
        # ------------------------
        self.csv_base = (
            f"abfss://{self.CSV_CONTAINER}@{self.CSV_ACCOUNT}.dfs.core.windows.net"
        )

        self.base = (
            f"abfss://{self.CONTAINER}@{self.ACCOUNT}.dfs.core.windows.net"
        )

        # ------------------------
        # Final resolved pipeline paths
        # ------------------------
        self.input_csv_path = f"{self.csv_base}/{self.ROBOT_ID}.csv"

        self.inflow_dir = f"{self.base}/inflow/"
        self.splitfiles_dir = f"{self.base}/inflow/"
        self.archive_dir = f"{self.base}/archive/"
        self.outputs_dir = f"{self.csv_base}/output_files/"

        self.tables_root = f"{self.base}/{self.TABLE_ROOT}"

        # Bronze / Silver / Gold
        self.bronze_path = f"{self.tables_root}/bronze"
        self.silver_path = f"{self.tables_root}/silver"
        self.gold_path = f"{self.tables_root}/gold"

        # Gold-level outputs
        self.gold_wirefeed_path = f"{self.tables_root}/gold_wire_consumption"
        self.gold_wirefeed_episodes_path = (
            f"{self.tables_root}/gold_wire_consumption_episodes"
        )
        self.joint_detection_path = f"{self.tables_root}/joint_detection"

        # ------------------------
        # Image paths
        # ------------------------
        self.temp_path_images = self.cfg["TEMP_PATH_IMAGES"]
        self.dest_images = f"{self.tables_root}/weld_images"

        # ------------------------
        # Reference paths
        # ------------------------
        ref_base_csv = (
            f"abfss://{self.CSV_CONTAINER}@{self.CSV_ACCOUNT}.dfs.core.windows.net"
        )
        ref_base = (
            f"abfss://{self.CONTAINER}@{self.ACCOUNT}.dfs.core.windows.net"
        )

        self.joint_colors_excel_path = f"{ref_base_csv}/{self.cfg['JOINT_COLORS_EXCEL']}"
        self.wps_excel_path = f"{ref_base_csv}/{self.cfg['WPS_EXCEL']}"
        self.weld_features_excel_path = f"{ref_base_csv}/{self.cfg['WELD_FEATURES_EXCEL']}"

        self.joint_colors_path = f"{ref_base}/{self.cfg['JOINT_COLORS_TABLE']}"
        self.wps_path = f"{ref_base}/{self.cfg['WPS_TABLE']}"
        self.weld_features_path = f"{ref_base}/{self.cfg['WELD_FEATURES_TABLE']}"

        # ------------------------
        # Feature columns
        # ------------------------
        self.techdevisrunning = self.cfg["TECHDEV_IS_RUNNING"]
        self.tps500current = self.cfg["TPS500_CURRENT"]
        self.time_col = self.cfg["TIME_COL"]
        self.wfs_col = self.cfg["WFS_COL"]
        self.activetask_col = self.cfg["ACTIVETASK_COL"]

        # ------------------------
        # Logging control
        # ------------------------
        self.VERBOSITY = self.cfg["VERBOSITY"]

    # --------------------------------------------------------
    # Supports running locally and in Databricks workspace
    # --------------------------------------------------------
    @staticmethod
    def _resolve_config_path(config_file):
        """
        Search for config.json in:
            - current directory
            - /Workspace/... (Databricks)
        """
        if os.path.exists(config_file):
            return config_file

        dbx_path = f"/Workspace/{config_file}"
        if os.path.exists(dbx_path):
            return dbx_path

        raise FileNotFoundError(f"Config JSON file not found: {config_file}")


# ------------------------------
# Single global instance
# ------------------------------
config_path = ConfigPaths()