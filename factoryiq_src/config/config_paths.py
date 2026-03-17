import json
import os
import inspect
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

print("DEBUG: config_paths.py LOADED FROM:", __file__)

class ConfigPaths:
    """Loads static JSON config from ADLS and constructs required ABFSS paths."""

    def __init__(self, config_file_adls=None):
        print("\n========== CONFIG DEBUG START ==========")
        print("DEBUG: Creating new ConfigPaths() instance")
        print("DEBUG: Module file:", __file__)
        print("DEBUG: Current working directory:", os.getcwd())

        spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(spark)

        # ------------------------------------------
        # ADLS path to config.json (user-specified)
        # ------------------------------------------
        self.config_file_adls = config_file_adls or \
            "abfss://datalake@sarawrobotcsvfiles.dfs.core.windows.net/config.json"

        print("DEBUG: Attempting to read config.json from:")
        print("       ", self.config_file_adls)

        # ------------------------------------------
        # Read JSON from ADLS
        # ------------------------------------------
        try:
            json_str = spark.read.text(self.config_file_adls).collect()[0][0]
            print("DEBUG: Successfully read config.json content:")
            print(json_str)
        except Exception as e:
            print("ERROR: FAILED TO READ CONFIG.JSON:", e)
            raise

        self.cfg = json.loads(json_str)

        print("DEBUG: Parsed JSON keys:", list(self.cfg.keys()))

        # ------------------------------------------
        # Resolve Account
        # ------------------------------------------
        self.ACCOUNT = self.dbutils.secrets.get(scope="kv-scope", key="ACCOUNT")
        print("DEBUG: ACCOUNT from secret:", self.ACCOUNT)

        # Basic identity constants
        self.CSV_ACCOUNT = self.cfg["CSV_ACCOUNT"]
        self.CSV_CONTAINER = self.cfg["CSV_CONTAINER"]
        print("DEBUG: CSV_ACCOUNT:", self.CSV_ACCOUNT)
        print("DEBUG: CSV_CONTAINER:", self.CSV_CONTAINER)

        self.CONTAINER = self.cfg["CONTAINER"]
        self.TABLE_ROOT = self.cfg["TABLE_ROOT"]
        self.ROBOT_ID = self.cfg["ROBOT_ID"]

        # Build bases
        self.csv_base = f"abfss://{self.CSV_CONTAINER}@{self.CSV_ACCOUNT}.dfs.core.windows.net"
        self.base     = f"abfss://{self.CONTAINER}@{self.ACCOUNT}.dfs.core.windows.net"

        print("DEBUG: Constructed csv_base:", self.csv_base)
        print("DEBUG: Constructed base:", self.base)

        # Reference files
        self.joint_colors_excel_path = f"{self.csv_base}/{self.cfg['JOINT_COLORS_EXCEL']}"
        print("DEBUG: joint_colors_excel_path:", self.joint_colors_excel_path)

        print("========== CONFIG DEBUG END ==========\n")


# ------------------------------
# Global instance: config_path
# ------------------------------
config_path = ConfigPaths()
print("DEBUG: Global config_path initialized. CSV_CONTAINER=", config_path.CSV_CONTAINER)