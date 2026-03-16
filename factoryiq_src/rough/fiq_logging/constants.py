# fiq_logging/constants.py

# This becomes the table name "<Log-Type>_CL" in Log Analytics
DEFAULT_LOG_TYPE = "DatabricksAppLogs"

# Default log level for your app
DEFAULT_LEVEL = "INFO"

# Optional env var names (used by set_context/get_context)
ENV_ROBOT = "ROBOT_ID"
ENV_STAGE = "PIPELINE_STAGE"
ENV_RUN_TS = "RUN_TS"
ENV_NOTEBOOK = "DATABRICKS_NOTEBOOK_PATH"
ENV_WORKSPACE_URL = "DATABRICKS_WORKSPACE_URL"
