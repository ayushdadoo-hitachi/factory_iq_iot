# orchestrator.py
# Purpose: Bronze -> Silver -> Gold pipeline runner for Databricks Job.
# Trigger: Can be called by Azure Function (HTTP or Event Grid) passing blob_url as python_params[0].

import os
import sys
import importlib
import logging
from typing import Optional, List

from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# 1) Project path so imports work when this runs as a Python script task
# ------------------------------------------------------------------------------------
PROJECT_PATH = os.environ.get(
    "PROJECT_PATH",
    "/Workspace/Users/ayush.dadoo.ext@hitachirail.net/factoryiq"
)
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

# ------------------------------------------------------------------------------------
# 2) Imports from your project
# ------------------------------------------------------------------------------------
from config import config_paths as config_paths  # type: ignore
import core.logging_config as logging_conf       # type: ignore

from pipelines import ref_ingest                 # type: ignore
from pipelines import bronze_ingest as bronze    # type: ignore
from pipelines import silver_data_quality as silver  # type: ignore
from pipelines import gold_data_enrichment as gold   # type: ignore

# In case you hot-deploy changes:
importlib.reload(config_paths)
importlib.reload(logging_conf)
importlib.reload(ref_ingest)
importlib.reload(bronze)
importlib.reload(silver)
importlib.reload(gold)

# ------------------------------------------------------------------------------------
# 3) Utilities
# ------------------------------------------------------------------------------------
def get_dbutils(spark):
    """
    Safely obtain dbutils in a Python script task.
    """
    try:
        # Works in Databricks 12.x+ Python tasks
        from pyspark.dbutils import DBUtils  # type: ignore
        return DBUtils(spark)
    except Exception:
        try:
            # Fallback if running interactively in a notebook
            import IPython  # type: ignore
            return IPython.get_ipython().user_ns["dbutils"]
        except Exception:
            return None


def parse_blob_url(argv: List[str]) -> Optional[str]:
    """
    Accept blob URL as first positional argument (python_params[0]).
    Supports also --blob-url=... for local/manual runs.
    """
    if len(argv) >= 2 and not argv[1].startswith("--"):
        return argv[1]
    for a in argv[1:]:
        if a.startswith("--blob-url="):
            return a.split("=", 1)[1]
    return None


def to_abfss_from_https(https_url: str) -> Optional[str]:
    """
    Optional helper: translate a Blob HTTPS URL to ABFS path
      https://<acct>.blob.core.windows.net/<container>/<path>
    -> abfss://<container>@<acct>.dfs.core.windows.net/<path>
    """
    try:
        # crude but safe transform
        # Example:
        # https://hrlfactoryiqsa.blob.core.windows.net/weld-data/igm6_.../inflow/file.csv
        # -> abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/igm6_.../inflow/file.csv
        from urllib.parse import urlparse
        u = urlparse(https_url)
        host = u.netloc  # hrlfactoryiqsa.blob.core.windows.net
        acct = host.split(".")[0]
        parts = u.path.lstrip("/").split("/", 1)
        container = parts[0]
        path_in_container = parts[1] if len(parts) > 1 else ""
        return f"abfss://{container}@{acct}.dfs.core.windows.net/{path_in_container}"
    except Exception:
        return None


# ------------------------------------------------------------------------------------
# 4) Main Orchestration
# ------------------------------------------------------------------------------------
def run_pipeline(blob_url: Optional[str] = None) -> None:
    """
    Execute Reference -> Bronze -> Silver -> Gold.
    If blob_url is provided, we log and pass it through for traceability.
    The Bronze stage already uses a manifest and will skip duplicates, so
    it is safe to process the inflow directory on each trigger.
    """
    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)

    app_logger = logging_conf.setup_logging(verbosity=config_paths.VERBOSITY)
    logger = logging_conf.get_module_logger(__name__)

    logger.info("===== Orchestrator start =====")
    logger.info("PROJECT_PATH=%s", PROJECT_PATH)
    if blob_url:
        logger.info("Triggered by blob_url=%s", blob_url)
        abfss_hint = to_abfss_from_https(blob_url)
        if abfss_hint:
            logger.info("Derived ABFSS path for reference (if needed): %s", abfss_hint)

    # --------------------------------------
    # Reference Tables (idempotent overwrite)
    # --------------------------------------
    logger.info("Loading reference tables...")
    ref_ingest.load_reference_tables(
        spark=spark,
        joint_colors_excel_path=config_paths.joint_colors_excel_path,
        wps_excel_path=config_paths.wps_excel_path,
        weld_features_csv_path=config_paths.required_weld_features_csv_path,
        joint_colors_path=config_paths.joint_colors_path,
        wps_path=config_paths.wps_path,
        weld_features_path=config_paths.weld_features_path,
    )
    logger.info("Reference tables loaded.")

    # -----------------
    # Bronze Ingestion
    # -----------------
    logger.info("Starting Bronze ingestion.")
    processed = bronze.run_stage(
        spark=spark,
        inflow_dir=config_paths.inflow_dir,
        bronze_path=config_paths.bronze_path,
        archive_dir=config_paths.archive_dir,
        limit_files=None,
        delta_options={"mergeSchema": "true"},
        add_batch_id=True,
        use_manifest=True,
        manifest_path=f"{config_paths.bronze_path}_manifest",
        move_to_date_partition=True,      # this is ignored when archiving disabled
        dbutils_=dbutils,
        enable_archiving=False            # <--- turn archiving OFF
    )



    logger.info("Bronze processed %d files.", len(processed))
    if processed:
        logger.info("Bronze processed files:\n%s", "\n".join(processed))
    else:
        logger.info("No new files found in inflow_dir=%s", config_paths.inflow_dir)


    

    # ---------------
    # Silver (Batch)
    # ---------------
    logger.info("Starting Silver stage.")
    silver_df = silver.run_stage(
        spark=spark,
        input_path=config_paths.bronze_path,
        ref_weld_path=config_paths.weld_features_path,
        output_path=config_paths.silver_path,
        manifest_path=f"{config_paths.silver_path}_manifest",
        delta_options={"mergeSchema": "true"},
        partition_by_batch=True,
        overwrite_existing_batch=False,
        round_numeric=True,
        log_counts=False,
    )
    # Avoid expensive count() if not needed; keep it for visibility
    try:
        logger.info("Silver row sample count: %d", silver_df.count())
    except Exception:
        logger.info("Silver stage completed (count skipped).")

    # --------------
    # Gold (Enrich)
    # --------------
    logger.info("Starting Gold stage.")
    gold_df = gold.run_stage(
        spark=spark,
        input_path=config_paths.silver_path,
        wps_ref_path=config_paths.wps_path,
        tps500current=config_paths.tps500current,
        techdevisrunning=config_paths.techdevisrunning,
        output_path=config_paths.gold_path,
        enable_wire_consumption=True,
        wire_episodes_output_path=config_paths.gold_wirefeed_episodes_path,
        time_col=config_paths.time_col,
        wfs_col=config_paths.wfs_col,
        wfs_unit="auto",
        write_mode="append",
        round_wire_outputs=True,
        round_digits=2,
        log_counts=False,
        manifest_path=f"{config_paths.gold_path}_manifest",
        delta_options={"mergeSchema": "true"},
        partition_by_batch=True,
        overwrite_existing_batch=False,
    )
    try:
        _ = gold_df.limit(1).count()
        logger.info("Gold stage completed.")
    except Exception:
        logger.info("Gold stage completed (preview skipped).")

    logger.info("===== Orchestrator end =====")


# ------------------------------------------------------------------------------------
# 5) Entry point
# ------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Accept optional blob_url as positional param or --blob-url=<url>
    blob_url = parse_blob_url(sys.argv)
    try:
        run_pipeline(blob_url=blob_url)
    except Exception as e:
        # Ensure job shows failure for alerting
        logging.exception("Pipeline failed: %s", e)
        raise