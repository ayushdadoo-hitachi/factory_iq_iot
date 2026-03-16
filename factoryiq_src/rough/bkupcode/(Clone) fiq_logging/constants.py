# fiq_logging/constants.py
DEFAULT_LOG_TYPE = "DatabricksAppLogs"   # LA table becomes DatabricksAppLogs_CL
DEFAULT_LEVEL = "INFO"

# Environment variable names (optional context you can set in notebook)
ENV_ROBOT = "ROBOT_ID"
ENV_STAGE = "PIPELINE_STAGE"
ENV_RUN_TS = "RUN_TS"