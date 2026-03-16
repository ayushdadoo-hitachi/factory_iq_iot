"""
Stage 0 Bronze: ingest CSV files -> Delta (append, coalesce-one, archive, manifest).
"""

# --- Standard library ---
import importlib
from typing import Dict, Optional, List
from datetime import datetime

# --- Third-party ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# dbutils access
try:
    from databricks.sdk.runtime import dbutils as _dbutils_mod
except Exception:
    _dbutils_mod = None

# --- Project ---
import core.ingestion_utils as ingest_ut; importlib.reload(ingest_ut)
from core.io_utils import write_delta
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)

# new shared helpers
import core.batch_utils as batch_ut; importlib.reload(batch_ut)
import core.manifest_utils as mf_ut; importlib.reload(mf_ut)


# ---------------------------
# dbutils accessor
# ---------------------------
def _get_dbutils(dbutils_override=None):
    if dbutils_override is not None:
        return dbutils_override
    if _dbutils_mod is not None:
        return _dbutils_mod
    raise RuntimeError(
        "dbutils is not available. Import in your notebook/job:\n"
        "    from databricks.sdk.runtime import dbutils\n"
        "Then pass dbutils_=dbutils into run_stage(...)."
    )


# ---------------------------
# Local helpers (Bronze)
# ---------------------------
def _list_inflow_csvs(dbu, inflow_dir: str, limit_files: Optional[int]) -> List[str]:
    items = dbu.fs.ls(inflow_dir)
    candidates = sorted([it.path for it in items if it.path.lower().endswith(".csv")])
    if limit_files:
        candidates = candidates[:limit_files]
    return candidates


def _archive_file(dbu, src_path: str, archive_dir: str, by_date: bool) -> None:
    if by_date:
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        dest_dir = f"{archive_dir.rstrip('/')}/dt={dt}"
    else:
        dest_dir = archive_dir.rstrip("/")
    dbu.fs.mkdirs(dest_dir)

    file_name = src_path.split("/")[-1]
    dest_path = f"{dest_dir}/{file_name}"

    try:
        dbu.fs.rm(dest_path)
    except Exception:
        pass
    dbu.fs.mv(src_path, dest_path)


def _ingest_single_csv_to_bronze(
    spark: SparkSession,
    csv_path: str,
    bronze_path: str,
    *,
    options: Dict[str, str],
    add_batch_id: bool,
) -> None:
    """Read+prepare one CSV, attach batch_id (optional), coalesce(1), append to Bronze."""
    logger.debug("Bronze ingest: %s", csv_path)

    df = ingest_ut.read_and_prepare(spark, csv_path)

    if add_batch_id:
        bid = batch_ut.extract_batch_id_from_path(csv_path)
        df = batch_ut.add_batch_id_col(df, bid)

    df_single = df.coalesce(1)

    write_delta(
        df_single,
        bronze_path,
        mode="append",
        options=options,
        register_table=None
    )
    logger.debug("Bronze write: appended %s", csv_path)


# --------------------------------------------
# Folder ingestion: inflow -> bronze -> archive
# --------------------------------------------
def run_stage(
    spark: SparkSession,
    inflow_dir: str,
    bronze_path: str,
    archive_dir: str,
    *,
    limit_files: Optional[int] = None,
    delta_options: Optional[Dict[str, str]] = None,
    add_batch_id: bool = True,
    use_manifest: bool = True,
    manifest_path: Optional[str] = None,
    move_to_date_partition: bool = True,
    dbutils_: Optional[object] = None
) -> List[str]:
    """
    Process CSVs from inflow, append to Bronze Delta, archive source, update manifest.
    Returns list of processed file paths.
    """
    logger.info("Starting Bronze - Data Ingestion")

    dbu = _get_dbutils(dbutils_)
    dbu.fs.mkdirs(archive_dir)

    options = {"mergeSchema": "true"}
    if delta_options:
        options.update(delta_options)

    if use_manifest:
        if not manifest_path:
            manifest_path = bronze_path.rstrip("/") + "_manifest"
        mf_ut.init_manifest_if_needed(spark, manifest_path)

    candidates = _list_inflow_csvs(dbu, inflow_dir, limit_files)
    processed_files: List[str] = []

    logger.debug("Inflow=%s | Bronze=%s | Archive=%s | Manifest=%s (enabled=%s) | files=%d",
                 inflow_dir, bronze_path, archive_dir, manifest_path, use_manifest, len(candidates))

    for idx, csv_path in enumerate(candidates, start=1):
        if use_manifest and mf_ut.is_in_manifest(spark, manifest_path, csv_path):
            logger.info("Skipping already-processed: %s", csv_path)
            continue

        try:
            _ingest_single_csv_to_bronze(
                spark,
                csv_path,
                bronze_path,
                options=options,
                add_batch_id=add_batch_id,
            )
            processed_files.append(csv_path)

            if use_manifest:
                mf_ut.append_manifest(spark, manifest_path, csv_path, status="success")

            _archive_file(dbu, csv_path, archive_dir, move_to_date_partition)

        except Exception:
            logger.exception("Failed to process %s", csv_path)
            if use_manifest:
                mf_ut.append_manifest(spark, manifest_path, csv_path, status="failed")

    logger.debug("Completed Bronze. Files processed: %d", len(processed_files))
    return processed_files