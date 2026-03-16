# Databricks notebook source
logger.info("333333")

# COMMAND ----------

# MAGIC %skip
# MAGIC # --- Databricks → Azure Log Analytics: One-cell setup (ROOT handlers) ---
# MAGIC
# MAGIC import os, sys, json, time, random, base64, hmac, hashlib, urllib.request
# MAGIC import logging, datetime
# MAGIC from datetime import timezone
# MAGIC
# MAGIC # ---------------------------
# MAGIC # 1) CONFIG: LA credentials
# MAGIC # ---------------------------
# MAGIC LA_WORKSPACE_ID = "<your-law-workspace-id>"   # e.g., c8e7....
# MAGIC LA_SHARED_KEY   = "<your-law-primary-key>"    # base64 string
# MAGIC LA_LOG_TYPE     = "DatabricksAppLogs"         # table -> DatabricksAppLogs_CL
# MAGIC LA_LEVEL        = "INFO"                      # INFO and above
# MAGIC
# MAGIC # Optional context stamped on each row
# MAGIC os.environ["ROBOT_ID"] = "igm6_29fjan_4feb"
# MAGIC # Databricks will set these automatically; we fall back to env if present:
# MAGIC # DATABRICKS_NOTEBOOK_PATH, DATABRICKS_WORKSPACE_URL
# MAGIC
# MAGIC # ---------------------------
# MAGIC # 2) Quiet root & reattach
# MAGIC # ---------------------------
# MAGIC root = logging.getLogger()
# MAGIC for h in list(root.handlers):
# MAGIC     root.removeHandler(h)
# MAGIC root.setLevel(LA_LEVEL)
# MAGIC
# MAGIC # Console handler (prints to notebook once)
# MAGIC ch = logging.StreamHandler(stream=sys.stdout)
# MAGIC ch.setLevel(LA_LEVEL)
# MAGIC ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
# MAGIC root.addHandler(ch)
# MAGIC
# MAGIC # ---------------------------
# MAGIC # 3) LA handler definition
# MAGIC # ---------------------------
# MAGIC class _LAHandler(logging.Handler):
# MAGIC     """Azure Log Analytics handler — batches by time/size with retries."""
# MAGIC     def __init__(self, workspace_id, shared_key, log_type="DatabricksAppLogs",
# MAGIC                  timeout_sec=5, batch_size=50, flush_interval=2.0, max_retries=5):
# MAGIC         super().__init__()
# MAGIC         self.workspace_id = workspace_id
# MAGIC         self.shared_key = shared_key
# MAGIC         self.log_type = log_type
# MAGIC         self.timeout_sec = timeout_sec
# MAGIC         self.batch_size = batch_size
# MAGIC         self.flush_interval = flush_interval
# MAGIC         self.max_retries = max_retries
# MAGIC         self._buf = []
# MAGIC         self._last = time.time()
# MAGIC
# MAGIC     def _build_signature(self, date, content_length, method, content_type, resource):
# MAGIC         x_headers = "x-ms-date:" + date
# MAGIC         string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
# MAGIC         bytes_to_hash = string_to_hash.encode("utf-8")
# MAGIC         decoded_key = base64.b64decode(self.shared_key)
# MAGIC         encoded_hash = base64.b64encode(
# MAGIC             hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
# MAGIC         ).decode()
# MAGIC         return f"SharedKey {self.workspace_id}:{encoded_hash}"
# MAGIC
# MAGIC     def _post_batch(self, records):
# MAGIC         if not records:
# MAGIC             return
# MAGIC         body = json.dumps(records, ensure_ascii=False).encode("utf-8")
# MAGIC         method, resource, content_type = "POST", "/api/logs", "application/json"
# MAGIC         date = datetime.datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
# MAGIC         sig = self._build_signature(date, len(body), method, content_type, resource)
# MAGIC         url = f"https://{self.workspace_id}.ods.opinsights.azure.com{resource}?api-version=2016-04-01"
# MAGIC         headers = {
# MAGIC             "content-type": content_type,
# MAGIC             "Authorization": sig,
# MAGIC             "Log-Type": self.log_type,
# MAGIC             "x-ms-date": date,
# MAGIC             "time-generated-field": "timestamp",  # use our timestamp for TimeGenerated
# MAGIC         }
# MAGIC         attempt = 0
# MAGIC         while True:
# MAGIC             try:
# MAGIC                 req = urllib.request.Request(url, data=body, headers=headers)
# MAGIC                 with urllib.request.urlopen(req, timeout=self.timeout_sec):
# MAGIC                     return
# MAGIC             except Exception as e:
# MAGIC                 attempt += 1
# MAGIC                 if attempt > self.max_retries:
# MAGIC                     logging.getLogger(__name__).warning(f"LA post failed after {attempt} attempts: {e}")
# MAGIC                     return
# MAGIC                 time.sleep(min(2 ** attempt + random.uniform(0, 0.5), 30.0))
# MAGIC
# MAGIC     def _flush_if_needed(self, force=False):
# MAGIC         if self._buf and (force or (time.time() - self._last >= self.flush_interval) or (len(self._buf) >= self.batch_size)):
# MAGIC             self._post_batch(self._buf)
# MAGIC             self._buf = []
# MAGIC             self._last = time.time()
# MAGIC
# MAGIC     def emit(self, record: logging.LogRecord):
# MAGIC         try:
# MAGIC             ts = datetime.datetime.fromtimestamp(record.created, timezone.utc).isoformat()
# MAGIC             # Databricks context (best-effort)
# MAGIC             nb = os.getenv("DATABRICKS_NOTEBOOK_PATH")
# MAGIC             ws = os.getenv("DATABRICKS_WORKSPACE_URL")
# MAGIC             try:
# MAGIC                 # If available, pull from Spark conf
# MAGIC                 if not ws and spark.conf.contains("spark.databricks.workspaceUrl"):
# MAGIC                     ws = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
# MAGIC             except Exception:
# MAGIC                 pass
# MAGIC
# MAGIC             doc = {
# MAGIC                 "timestamp": ts,                 # -> timestamp_t
# MAGIC                 "Level": record.levelname,       # -> Level
# MAGIC                 "Message": record.getMessage(),  # -> Message
# MAGIC                 "logger": record.name,           # -> logger_s
# MAGIC                 "module": record.module,         # -> module_s
# MAGIC                 "funcName": record.funcName,     # -> funcName_s
# MAGIC                 "lineno": record.lineno,         # -> lineno_d
# MAGIC                 "process": record.process,       # -> process_d
# MAGIC                 "thread": record.thread,         # -> thread_d
# MAGIC                 # Context
# MAGIC                 "robotId": os.getenv("ROBOT_ID"),
# MAGIC                 "notebook": nb,
# MAGIC                 "workspaceUrl": ws,
# MAGIC             }
# MAGIC             if record.exc_info:
# MAGIC                 import traceback
# MAGIC                 doc["exception"] = "".join(traceback.format_exception(*record.exc_info))
# MAGIC
# MAGIC             self._buf.append(doc)
# MAGIC             self._flush_if_needed()
# MAGIC         except Exception:
# MAGIC             self.handleError(record)
# MAGIC
# MAGIC     def flush(self):
# MAGIC         try:
# MAGIC             self._flush_if_needed(force=True)
# MAGIC         except Exception:
# MAGIC             pass
# MAGIC
# MAGIC     def close(self):
# MAGIC         try:
# MAGIC             self.flush()
# MAGIC         finally:
# MAGIC             super().close()
# MAGIC
# MAGIC # Attach LA handler to ROOT
# MAGIC la = _LAHandler(LA_WORKSPACE_ID, LA_SHARED_KEY, log_type=LA_LOG_TYPE)
# MAGIC la.setLevel(LA_LEVEL)
# MAGIC root.addHandler(la)
# MAGIC
# MAGIC # Optional: reduce noisy libs
# MAGIC logging.getLogger("py4j").setLevel(logging.WARN)
# MAGIC logging.getLogger("py4j.clientserver").setLevel(logging.WARN)
# MAGIC
# MAGIC # Convenience app logger
# MAGIC logger = logging.getLogger("factoryiq.pipeline")
# MAGIC logger.setLevel(LA_LEVEL)
# MAGIC logger.propagate = True  # let it flow to ROOT
# MAGIC
# MAGIC logger.info("Logging initialized: console + LA (DatabricksAppLogs_CL)")

# COMMAND ----------

# ===========================================================
# Databricks → Azure Log Analytics (no duplicates, module logger)
# ===========================================================

import logging, datetime, hashlib, hmac, base64, json, os, sys, urllib.request

# ---------------------------
# 1) CONFIG: LA credentials
# ---------------------------
LA_WORKSPACE_ID = "c8e73781-d27f-49ed-86e4-4df8f61b7010"   # your customerId
LA_SHARED_KEY   = "HCQSSAgJ+Hzab7+Xm+cV1uTfOCNmZ2zLQ7clZ54CoxBI1waMoRNlXcRdP+VmFad6LyrIzlOgD6gkYcg6G06GwQ=="  # your primarySharedKey
LA_LOG_TYPE     = "DatabricksAppLogs"     # LA table = DatabricksAppLogs_CL
LA_LEVEL        = "INFO"                   # INFO and above

# Optional context (appears in LA rows)
ROBOT_ID = "igm6_29fjan_4feb"
os.environ["ROBOT_ID"] = ROBOT_ID


# -----------------------------------------
# 2) CLEAN ALL EXISTING (noisy) HANDLERS
# -----------------------------------------
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
_root.propagate = False
_root.setLevel(logging.NOTSET)  # root stays quiet


# ------------------------------------------------
# 3) Create a MODULE logger (only this will log)
# ------------------------------------------------
logger = logging.getLogger(__name__)
# Remove any prior handlers on this module logger too
for h in list(logger.handlers):
    logger.removeHandler(h)
logger.propagate = False
logger.setLevel(LA_LEVEL)


# ------------------------------------------------
# 4) LA handler (TZ-aware) attached to MODULE logger
# ------------------------------------------------
class _LAHandler(logging.Handler):
    """Azure Log Analytics handler — timezone-aware; single-record send."""
    def __init__(self, workspace_id, shared_key, log_type="DatabricksAppLogs", timeout_sec=5):
        super().__init__()
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = log_type
        self.timeout_sec = timeout_sec

    def _build_signature(self, date, content_length, method, content_type, resource):
        x_headers = "x-ms-date:" + date
        string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
        bytes_to_hash = string_to_hash.encode("utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
        return f"SharedKey {self.workspace_id}:{encoded_hash}"

    def _post(self, records):
        body = json.dumps(records).encode("utf-8")
        method, resource, content_type = "POST", "/api/logs", "application/json"
        date = datetime.datetime.now(datetime.UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
        sig = self._build_signature(date, len(body), method, content_type, resource)
        url = f"https://{self.workspace_id}.ods.opinsights.azure.com{resource}?api-version=2016-04-01"
        headers = {
            "content-type": content_type,
            "Authorization": sig,
            "Log-Type": self.log_type,
            "x-ms-date": date,
        }
        req = urllib.request.Request(url, data=body, headers=headers)
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as _:
            pass

    def emit(self, record):
        try:
            ts = datetime.datetime.fromtimestamp(record.created, datetime.UTC).isoformat()
            doc = {
                "timestamp": ts,                          # becomes timestamp_t
                "level": record.levelname,                # becomes Level
                "logger": record.name,                    # logger_s
                "message": record.getMessage(),           # Message
                "module": record.module,                  # module_s
                "funcName": record.funcName,              # funcName_s
                "lineno": record.lineno,                  # lineno_d
                "process": record.process,                # process_d
                "thread": record.thread,                  # thread_d
                # context
                "robotId": os.getenv("ROBOT_ID"),         # robotId_s
                "notebook": os.getenv("DATABRICKS_NOTEBOOK_PATH"),
                "workspaceUrl": os.getenv("DATABRICKS_WORKSPACE_URL"),
            }
            if record.exc_info:
                import traceback
                doc["exception"] = "".join(traceback.format_exception(*record.exc_info))
            self._post([doc])
        except Exception:
            self.handleError(record)

# Attach LA handler ONCE to the MODULE logger
if not any(isinstance(h, _LAHandler) for h in logger.handlers):
    la = _LAHandler(LA_WORKSPACE_ID, LA_SHARED_KEY, log_type=LA_LOG_TYPE)
    la.setLevel(LA_LEVEL)
    logger.addHandler(la)


# --------------------------------------------------------
# 5) OPTIONAL: Add one clean console handler to MODULE
#    (comment these 4 lines if you don't want cell prints)
# --------------------------------------------------------
console = logging.StreamHandler(stream=sys.stdout)
console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
console.setLevel(LA_LEVEL)
logger.addHandler(console)


# -------------------------
# 6) Test (should print once
#    in cell and ingest once)
# -------------------------
logger.info("5555555")

# COMMAND ----------

from fiq_logging import init_logging, set_context

# Put your LAW creds here (hardcoded for now)
WS_ID  = "c8e73781-d27f-49ed-86e4-4df8f61b7010"
WS_KEY = "HCQSSAgJ+Hzab7+Xm+cV1uTfOCNmZ2zLQ7clZ54CoxBI1waMoRNlXcRdP+VmFad6LyrIzlOgD6gkYcg6G06GwQ=="

# Optional: add context fields to include in each log record
set_context(robot_id="igm6_29fjan_4feb", stage="stage0")

# Initialize the logger (MUST pass workspace_id + shared_key)
logger = init_logging(
    logger_name="factoryiq.pipeline",
    workspace_id=WS_ID,   
    shared_key=WS_KEY,
    add_console=True       # True = see logs printed once in notebook
)

# Use the logger (one log in notebook, one in LAW)
logger.info("TEST — should appear in notebook and LAW")

# COMMAND ----------

logger.info("ddddddddddddd")

# COMMAND ----------

print('hello')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE CATALOG `factoryiq-catalog`;
# MAGIC

# COMMAND ----------

pdf = df.toPandas()

pdf.to_excel("/dbfs/FileStore/my_export.xlsx", index=False)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

projectteam = dbutils.secrets.get(scope="factoryiq-kv-scope", key="projectteam")
print(projectteam)


# COMMAND ----------

# MAGIC %sql
# MAGIC LIST 'abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/';
# MAGIC

# COMMAND ----------

# List files
display(dbutils.fs.ls("abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/"))


# COMMAND ----------

# Example: reading CSV
df = spark.read.option("header", "true").csv(
    "abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/weld_features_raw.csv"
)
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1) Make sure catalog & schema exist (adjust names if you already have them)
# MAGIC CREATE CATALOG IF NOT EXISTS factoryiq_catalog;
# MAGIC CREATE SCHEMA  IF NOT EXISTS factoryiq_catalog.refdata;
# MAGIC
# MAGIC -- 2) Create an EXTERNAL volume that maps to your ADLS container/folder
# MAGIC CREATE EXTERNAL VOLUME factoryiq_catalog.refdata.excel_vol
# MAGIC LOCATION 'abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/';
# MAGIC ``

# COMMAND ----------

# DBTITLE 1,fetch csv from the sa
# Example: reading CSV
df = spark.read.option("header", "true").csv(
    "abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/weld_features_raw.csv"
)
df.show()


# COMMAND ----------


# >>>>  this code is failing >>>>>>>>>>>

# Read the Excel from ADLS Gen2 via Spark
import pandas as pd
# Read Excel directly into Pandas
pdf = pd.read_excel("abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/joint_colors.xlsx")


spark_df = (spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dataAddress", "'Sheet1'!A1")  # adjust if needed
            .load(xlsx_path))

# Convert to Pandas
pdf = spark_df.toPandas()


# COMMAND ----------

spark.sql("SELECT current_user()").show()

# COMMAND ----------

# A. Confirm you’re on the expected metastore & what catalog/schema are active
spark.sql("SELECT current_catalog() AS catalog, current_schema() AS schema").show(truncate=False)

# B. Do the catalog + schema exist?
spark.sql("SHOW CATALOGS").show(truncate=False)
spark.sql("SHOW SCHEMAS IN factoryiq_catalog").show(truncate=False)

# C. List volumes in the schema (note the backticks: your schema name has hyphens)
spark.sql("SHOW VOLUMES IN factoryiq_catalog.`factoryiq-csv-excel-schema`").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Does this volume point to managed or external storage?
# MAGIC DESCRIBE VOLUME factoryiq_catalog.`factoryiq-csv-excel-schema`.`config-volume`;
# MAGIC
# MAGIC -- If it's external, what external location/credential does it use?
# MAGIC -- (If you know the external location name, describe it directly)
# MAGIC -- Example:
# MAGIC -- DESCRIBE EXTERNAL LOCATION factoryiq_storage_ext;
# MAGIC
# MAGIC -- Show privileges on the volume (to rule out UC-level grants)
# MAGIC SHOW GRANTS ON VOLUME factoryiq_catalog.`factoryiq-csv-excel-schema`.`config-volume`;

# COMMAND ----------

# MAGIC %pip install adlfs fsspec openpyxl pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import io, pandas as pd

xlsx = "abfss://factoryiq-referencedata@hrlfactoryiqsa.dfs.core.windows.net/joint_colors.xlsx"

bin_df = spark.read.format("binaryFile").load(xlsx)
content_bytes = bin_df.select("content").head()[0]   # take the first (only) file

pdf = pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")
print(pdf.head())

# COMMAND ----------

spark.sql("SELECT current_catalog()").show()
spark.sql("SELECT current_schema()").show()


# COMMAND ----------

# MAGIC %skip
# MAGIC # reference_path = "/Volumes/workspace/sourcedata/sourcedatavolume/reference_data/weld_features_raw.csv"
# MAGIC
# MAGIC reference_path = f"{REF_BASE_PATH}{REF_REQUIRED_WELD_FEATURES}"
# MAGIC
# MAGIC ref_df = spark.read.csv(reference_path, header=True)

# COMMAND ----------

# MAGIC %skip
# MAGIC import io, pandas as pd
# MAGIC
# MAGIC xlsx = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/ref_csv_excel/column_alignment_Sheet1.xlsx"
# MAGIC
# MAGIC bin_df = spark.read.format("binaryFile").load(xlsx)
# MAGIC content_bytes = bin_df.select("content").head()[0]   # take the first (only) file
# MAGIC
# MAGIC pdf = pd.read_excel(io.BytesIO(content_bytes), engine="openpyxl")
# MAGIC # print(pdf.head())
# MAGIC
# MAGIC pdf = sanitize_excel_pdf(pdf) # Step 2: Sanitize
# MAGIC rstage0_columns_schema = pandas_to_spark_schema(pdf) # Step 3: Build schema
# MAGIC sdf = spark.createDataFrame(pdf, schema=rstage0_columns_schema) # Step 4: Create Spark DF (Arrow-safe)
# MAGIC sdf = normalize_columns(sdf)
# MAGIC # display(sdf.limit(25))
# MAGIC
# MAGIC print(REQUIRED_STAGE0_COLUMNS_TABLE_PATH)
# MAGIC
# MAGIC sdf.write.format("delta").mode("overwrite").save(REQUIRED_STAGE0_COLUMNS_TABLE_PATH)
# MAGIC print("done")
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC # List files
# MAGIC display(dbutils.fs.ls("abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/"))
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC df = read_data_from_tables_inabfss(spark,REQUIRED_STAGE0_COLUMNS_TABLE_PATH)
# MAGIC df.show()

# COMMAND ----------

write_df_to_abfss(joint_colors_df,JOINT_COLORS_TABLE_PATH) 
write_df_to_abfss(required_stage0_columns_df,REQUIRED_STAGE0_COLUMNS_TABLE_PATH)
write_df_to_abfss(wps_df_a,WPS_TABLE_ACTUAL_PATH)
write_df_to_abfss(ref_df,REQUIRED_WELD_FEATURES_TABLE_PATH)

# COMMAND ----------

# MAGIC %skip
# MAGIC def get_distinct_stepnumber_names(df, colname):
# MAGIC     result_df = df.select(colname).distinct()
# MAGIC     display(result_df)
# MAGIC     count = result_df.count()
# MAGIC     if count == 0:
# MAGIC         print(f"No distinct {colname} values found.")
# MAGIC     else:
# MAGIC         print(f"Distinct {colname} count: {count}")
# MAGIC     return result_df

# COMMAND ----------

# df = spark.table(STAGE1_TABLE)
df = read_data_from_tables_inabfss(spark,STAGE1_TABLE)
# df = normalize_current_voltage_inplace(df)
display(df.limit(10))
print("before count     :", df.count())
df = filter_out_of_range_rows(df)
# print("mid count     :", df.count())
# df = run_isolation_forest_anomaly_detection(df)
print("after count     :", df.count())
display(df.limit(10))
# df.write.format("delta").mode("overwrite").saveAsTable(STAGE2_TABLE)
write_df_to_abfss(df,STAGE2_TABLE) 
# df.printSchema()
# df_stage1 = fix_and_parse_timestamp(df_stage1, use_try_to_timestamp=True)
# df_stage1 = filter_valid_rows(df_stage1)
# df_stage1.write.format("delta").mode("overwrite").saveAsTable("workspace.welddata.stage1_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell in Databricks
# MAGIC COPY INTO 'abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/robot_data_tables/'
# MAGIC FROM stage4_igm6_29fjan_4feb
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS('header'='true','delimiter'=',');
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC nslookup stfactoryiqdevadls.dfs.core.windows.net
# MAGIC

# COMMAND ----------

https://hrlfactoryiqsa.blob.core.windows.net/weld-data/robot_data/robot_data_tables/stage4_igm6_29fjan_4feb/