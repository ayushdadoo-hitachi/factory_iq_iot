"""
Stage 0 (Bronze): Raw Robot CSV(s) ➜ Delta

- run(...): ingest a single CSV ➜ append to Bronze Delta
- run_stage(...): ingest all CSVs in an inflow folder:
    • parse each CSV via read_and_prepare (expects 3-line preamble)
    • coalesce(1) ➜ one Parquet per CSV in Bronze
    • move processed CSV to archive (keeps inflow clean)
    • optional Delta manifest to avoid reprocessing

No extra metadata columns are added that affect Stage-1 schema, except
an optional 'batch_id' derived from filename (can be disabled).
"""

# --- Standard library ---
import importlib
from typing import Dict, Optional, List, Tuple
from datetime import datetime

# --- Third-party ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Ensure dbutils is available when running from a Python module (notebook-external)
try:
    from databricks.sdk.runtime import dbutils as _dbutils_mod
except Exception:  # pragma: no cover
    _dbutils_mod = None

# --- Project ---
import core.ingestion_utils as ingest_ut; importlib.reload(ingest_ut)
from core.io_utils import write_delta
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


# ---------------------------
# Helpers to access dbutils.fs
# ---------------------------
def _get_dbutils(dbutils_override=None):
    """
    Returns a usable dbutils object (override > imported), or raises with guidance.
    """
    if dbutils_override is not None:
        return dbutils_override
    if _dbutils_mod is not None:
        return _dbutils_mod

    # Not available automatically if you execute as a plain python module
    raise RuntimeError(
        "dbutils is not available. In your notebook or job, import it before using this module:\n"
        "    from databricks.sdk.runtime import dbutils\n"
        "Then pass it into run_folder(..., dbutils_=dbutils)\n"
    )

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
    manifest_path: Optional[str] = None,   # e.g., abfss://.../robot_data/robot_data_tables/bronze_<robot>_manifest
    move_to_date_partition: bool = True,
    dbutils_: Optional[object] = None
) -> List[str]:
    """
    Process CSVs from an inflow directory, append each to Bronze Delta, and move to archive.

    - Scans inflow_dir for *.csv
    - For each file:
        * parse with read_and_prepare
        * coalesce(1) & append to bronze_path (=> one Parquet per CSV)
        * move original CSV to archive_dir[/dt=YYYY-MM-DD]
        * record filename in a Delta manifest (optional)

    Returns: list of processed file paths.
    """
    logger.info("--------------------------------------------------")
    logger.info("Starting Bronze - Data Ingestion")
    logger.info("--------------------------------------------------")

    dbu = _get_dbutils(dbutils_)
    dbu.fs.mkdirs(archive_dir)  # ensure archive root exists

    options = {"mergeSchema": "true"}
    if delta_options:
        options.update(delta_options)

    # Prepare manifest
    if use_manifest:
        if not manifest_path:
            manifest_path = bronze_path.rstrip("/") + "_manifest"
        _init_manifest_if_needed(spark, manifest_path)

    # List candidate files
    items = dbu.fs.ls(inflow_dir)
    candidates = sorted([it.path for it in items if it.path.lower().endswith(".csv")])

    if limit_files:
        candidates = candidates[:limit_files]

    processed_files: List[str] = []

    logger.debug("--------------------------------------------------")
    logger.debug("📂 Inflow dir : %s", inflow_dir)
    logger.debug("🥉 Bronze path: %s", bronze_path)
    logger.debug("📦 Archive dir: %s", archive_dir)
    logger.debug("🧾 Manifest   : %s (enabled=%s)", manifest_path, use_manifest)
    logger.debug("🗂️ Files to process: %d", len(candidates))

    for idx, csv_path in enumerate(candidates, start=1):
        # Skip if already in manifest
        if use_manifest and _is_in_manifest(spark, manifest_path, csv_path):
            logger.info("⏭️  Skipping already-processed: %s", csv_path)
            continue

        try:
            logger.debug("[%d] 📥 Reading: %s", idx, csv_path)
            df = ingest_ut.read_and_prepare(spark, csv_path)

            if add_batch_id:
                batch_id = csv_path.split("/")[-1].replace(".csv", "")
                df = df.withColumn("batch_id", F.lit(batch_id))

            # One Parquet per CSV
            df_single_file = df.coalesce(1)

            write_delta(
                df_single_file,
                bronze_path,
                mode="append",
                options=options,
                register_table=None
            )
            processed_files.append(csv_path)
            logger.debug("[%d] ✅ Appended to Bronze", idx)

            # Record in manifest
            if use_manifest:
                _append_manifest(spark, manifest_path, csv_path, status="success")

            # Move to archive
            _archive_file(dbu, csv_path, archive_dir, move_to_date_partition)

        except Exception as e:
            logger.exception("❌ Failed to process %s", csv_path)
            if use_manifest:
                _append_manifest(spark, manifest_path, csv_path, status=f"failed: {str(e)[:200]}")
            # Optionally move to quarantine:
            # _archive_file(dbu, csv_path, archive_dir + "_failed", move_to_date_partition)

    logger.debug("🟢 Completed. Files processed: %d", len(processed_files))
    return processed_files


# ---------------------------
# Manifest helpers (Delta)
# ---------------------------
def _init_manifest_if_needed(spark: SparkSession, manifest_path: str) -> None:
    if not _delta_exists(spark, manifest_path):
        schema = "file_path STRING, processed_ts TIMESTAMP, status STRING"
        (spark.createDataFrame([], schema)
              .write.format("delta")
              .mode("overwrite")
              .save(manifest_path))

def _delta_exists(spark: SparkSession, path: str) -> bool:
    try:
        _ = spark.read.format("delta").load(path).limit(1).count()
        return True
    except Exception:
        return False

def _is_in_manifest(spark: SparkSession, manifest_path: str, file_path: str) -> bool:
    try:
        mf = spark.read.format("delta").load(manifest_path)
        return mf.where(F.col("file_path") == file_path).limit(1).count() > 0
    except Exception:
        return False

def _append_manifest(spark: SparkSession, manifest_path: str, file_path: str, status: str) -> None:
    row = [(file_path, datetime.utcnow(), status)]
    df = spark.createDataFrame(row, ["file_path", "processed_ts", "status"])
    df.write.format("delta").mode("append").save(manifest_path)


# ---------------------------
# Archiving helpers (dbutils.fs)
# ---------------------------
def _archive_file(dbu, src_path: str, archive_dir: str, by_date: bool) -> None:
    if by_date:
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        dest_dir = f"{archive_dir.rstrip('/')}/dt={dt}"
    else:
        dest_dir = archive_dir.rstrip("/")
    dbu.fs.mkdirs(dest_dir)

    file_name = src_path.split("/")[-1]
    dest_path = f"{dest_dir}/{file_name}"

    # Overwrite if already exists
    try:
        dbu.fs.rm(dest_path)
    except Exception:
        pass

    dbu.fs.mv(src_path, dest_path)