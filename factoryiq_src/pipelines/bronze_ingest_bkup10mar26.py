"""
Stage 0 Bronze: ingest CSV files -> Delta (append, coalesce-one, archive, manifest).
"""
import importlib
from typing import Dict, Optional, List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from databricks.sdk.runtime import dbutils as _dbutils_mod
except Exception:
    _dbutils_mod = None

import core.ingestion_utils as ingest_ut; importlib.reload(ingest_ut)
from core.io_utils import write_delta
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)
import core.batch_utils as batch_ut; importlib.reload(batch_ut)
import core.fs_utils as fs_ut; importlib.reload(fs_ut)
from core.stage_runner import StageRunner

def _get_dbutils(dbutils_override=None):
    if dbutils_override is not None:
        return dbutils_override
    if _dbutils_mod is not None:
        return _dbutils_mod
    raise RuntimeError("dbutils is not available. Import in notebook/job and pass dbutils_=dbutils")

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
    logger.info("Starting Bronze - Data Ingestion")

    dbu = _get_dbutils(dbutils_)
    dbu.fs.mkdirs(archive_dir)
    options = {"mergeSchema": "true"}
    if delta_options:
        options.update(delta_options)

    candidates = fs_ut.list_csvs(dbu, inflow_dir, limit_files)
    if use_manifest:
        manifest_path = manifest_path or bronze_path.rstrip("/") + "_manifest"
    else:
        manifest_path = None

    runner = StageRunner(spark, manifest_path=manifest_path)

    def process_one(csv_path: str) -> None:
        df = ingest_ut.read_and_prepare(spark, csv_path)
        if add_batch_id:
            bid = batch_ut.extract_batch_id_from_path(csv_path)
            df = batch_ut.add_batch_id_col(df, bid)
        df_single = df.coalesce(1)
        write_delta(df_single, bronze_path, mode="append", options=options, register_table=None)
        logger.debug("Bronze write appended: %s", csv_path)
        logger.info("inside process one")

    finalize_one = lambda p: fs_ut.archive_one(dbu, p, archive_dir, move_to_date_partition)

    processed = runner.run(candidates, process_one=process_one, finalize_one=finalize_one)
    logger.debug("Completed Bronze. Files processed: %d", len(processed))
    return processed