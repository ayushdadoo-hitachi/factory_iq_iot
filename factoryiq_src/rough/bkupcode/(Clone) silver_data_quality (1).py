import importlib
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from core.io_utils import read_delta

import core.schema_utils as schema_ut; importlib.reload(schema_ut)
import core.cleaning_utils as cleaning_ut; importlib.reload(cleaning_ut)
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)

from core.stage_runner import StageRunner
from core.manifest_utils import delta_exists


def _list_batch_ids(spark: SparkSession, bronze_path: str) -> List[str]:
    df_bronze = spark.read.format("delta").load(bronze_path)
    if "batch_id" not in df_bronze.columns:
        raise ValueError("Bronze table is missing 'batch_id' column.")
    return [r["batch_id"] for r in df_bronze.select("batch_id").distinct().collect()]


def _delete_silver_batch(spark: SparkSession, silver_path: str, batch_id: str) -> None:
    if delta_exists(spark, silver_path):
        spark.sql(f"DELETE FROM delta.`{silver_path}` WHERE batch_id = '{batch_id}'")


def run_stage(
    spark: SparkSession,
    input_path: str,
    ref_weld_path: str,
    output_path: str,
    *,
    manifest_path: Optional[str] = None,
    delta_options: Optional[Dict[str, str]] = None,
    partition_by_batch: bool = True,
    overwrite_existing_batch: bool = False,
    round_numeric: bool = True,
    log_counts: bool = True
) -> DataFrame:
    logger.info("Starting Silver - Data Quality (Batch)")

    options = {"mergeSchema": "true"}
    if delta_options:
        options.update(delta_options)

    if not manifest_path:
        manifest_path = output_path.rstrip("/") + "_manifest"

    runner = StageRunner(
        spark,
        manifest_path=manifest_path,
        init_manifest_schema="file_path STRING, processed_ts TIMESTAMP, status STRING"
    )

    ref_weld_df = read_delta(spark, ref_weld_path)
    candidates = _list_batch_ids(spark, input_path)
    if not candidates:
        logger.info("No Bronze batches found.")
        return spark.createDataFrame([], ref_weld_df.schema)

    last_df: Optional[DataFrame] = None

    def process_one(batch_id: str) -> None:
        nonlocal last_df

        df_bronze_batch = spark.read.format("delta").load(input_path).where(F.col("batch_id") == batch_id)

        src_files: List[str] = []
        try:
            src_files = [
                r["file_path"]
                for r in (
                    df_bronze_batch
                    .select(F.col("_metadata.file_path").alias("file_path"))
                    .distinct()
                    .collect()
                )
            ]
        except Exception:
            src_files = []

        df = df_bronze_batch

        df = schema_ut.filter_columns_by_reference(df, ref_weld_df)
        df = cleaning_ut.regex_clean_columns(df)
        df = cleaning_ut.normalize_strings(df)
        df = cleaning_ut.apply_comma_to_dot(df, ref_weld_df)

        df, removed = cleaning_ut.drop_rows_with_invalid_int_values(df, ref_weld_df, log=False)
        if log_counts:
            logger.debug("invalid_int_removed=%d", removed)
            logger.debug("rows_after_invalid_int=%d", df.count())

        df = cleaning_ut.apply_datatype(df, ref_weld_df)

        if round_numeric:
            df = cleaning_ut.apply_roundoff_to_numeric_columns(df, ref_weld_df, log=False)

        df = cleaning_ut.fix_and_parse_timestamp(df, colname="time", use_try_to_timestamp=False)
        df = cleaning_ut.filter_out_of_range_rows(df, ref_weld_df)

        if overwrite_existing_batch:
            _delete_silver_batch(spark, output_path, batch_id)

        writer = df.write.format("delta").mode("append")
        for k, v in options.items():
            writer = writer.option(k, v)
        if partition_by_batch and "batch_id" in df.columns:
            writer = writer.partitionBy("batch_id")
        writer.save(output_path)

        if src_files:
            logger.info("Silver processed files:\n%s", "\n".join(src_files))

        last_df = df

    runner.run(
        items=candidates,
        is_already_done=None,
        process_one=process_one,
        finalize_one=None
    )

    if last_df is not None:
        return last_df

    try:
        return spark.read.format("delta").load(output_path).limit(0)
    except Exception:
        return spark.createDataFrame([], ref_weld_df.schema)