# fiq_logging/context.py
import os

# Cache expensive Databricks context lookups
_DBCTX_CACHED = None

def _get_dbctx_safe():
    """
    Try to fetch Databricks runtime context once and cache it.
    Safe to use outside DBX (returns None).
    """
    global _DBCTX_CACHED
    if _DBCTX_CACHED is not None:
        return _DBCTX_CACHED
    try:
        # dbutils is available in Databricks notebooks/jobs
        dbctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore  # noqa: F821
        _DBCTX_CACHED = dbctx
        return dbctx
    except Exception:
        _DBCTX_CACHED = None
        return None

def get_context():
    """
    Build a dict of context fields to attach to each log event.
    Pulls from env (cheap) + Databricks runtime (once, cached).
    """
    ctx = {
        "robotId": os.getenv("ROBOT_ID"),
        "stage": os.getenv("PIPELINE_STAGE"),
        "runTs": os.getenv("RUN_TS"),
        "notebook": os.getenv("DATABRICKS_NOTEBOOK_PATH"),
        "workspaceUrl": os.getenv("DATABRICKS_WORKSPACE_URL"),
        "clusterId": None,
        "jobRunId": None,
    }

    dbctx = _get_dbctx_safe()
    try:
        if dbctx:
            if not ctx["notebook"] and dbctx.notebookPath().isDefined():
                ctx["notebook"] = dbctx.notebookPath().get()
            if dbctx.browserHostName().isDefined() and not ctx["workspaceUrl"]:
                # e.g., adb-1234567890123456.2.azuredatabricks.net
                ctx["workspaceUrl"] = f"https://{dbctx.browserHostName().get()}"
            if dbctx.clusterId().isDefined():
                ctx["clusterId"] = dbctx.clusterId().get()
            if dbctx.jobRunId().isDefined():
                ctx["jobRunId"] = dbctx.jobRunId().get()
    except Exception:
        # Never break the app on context fetch
        pass

    return ctx

def set_context(robot_id=None, stage=None, run_ts=None, **extras):
    """
    Convenience: set common env vars so they appear in logs.
    """
    if robot_id is not None:
        os.environ["ROBOT_ID"] = str(robot_id)
    if stage is not None:
        os.environ["PIPELINE_STAGE"] = str(stage)
    if run_ts is not None:
        os.environ["RUN_TS"] = str(run_ts)
    # Any additional key=value you want to expose as context
    for k, v in extras.items():
        if isinstance(k, str):
            os.environ[k] = str(v)
``