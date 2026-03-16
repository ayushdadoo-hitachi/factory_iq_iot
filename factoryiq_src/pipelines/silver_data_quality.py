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
    lst = [r["batch_id"] for r in df_bronze.select("batch_id").distinct().collect()]
    logger.debug("Discovered batch_ids (count=%d): %s", len(lst), lst)
    return lst


def _delete_silver_batch(spark: SparkSession, silver_path: str, batch_id: str) -> None:
    if delta_exists(spark, silver_path):
        logger.debug("Deleting existing Silver partition for batch_id=%s", batch_id)
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
    logger.debug("Write options: %s", options)
    logger.debug("partition_by_batch=%s overwrite_existing_batch=%s round_numeric=%s log_counts=%s",
                 partition_by_batch, overwrite_existing_batch, round_numeric, log_counts)

    if not manifest_path:
        manifest_path = output_path.rstrip("/") + "_manifest"
    logger.debug("Using manifest_path=%s", manifest_path)

    runner = StageRunner(
        spark,
        manifest_path=manifest_path,
        init_manifest_schema="file_path STRING, processed_ts TIMESTAMP, status STRING"
    )

    logger.debug("Reading reference table: %s", ref_weld_path)
    ref_weld_df = read_delta(spark, ref_weld_path)
    logger.debug("Reference first column=%s; ref schema cols=%d",
                 ref_weld_df.columns[0] if ref_weld_df.columns else "N/A",
                 len(ref_weld_df.columns))

    candidates = _list_batch_ids(spark, input_path)
    if not candidates:
        logger.info("No Bronze batches found.")
        return spark.createDataFrame([], ref_weld_df.schema)

    last_df: Optional[DataFrame] = None

    def process_one(batch_id: str) -> None:
        nonlocal last_df
        logger.debug("=== SILVER BATCH START: %s ===", batch_id)

        # Load batch
        df_in = spark.read.format("delta").load(input_path).where(F.col("batch_id") == batch_id)
        logger.debug("[load] columns(%d)=%s", len(df_in.columns), df_in.columns)
        if log_counts:
            try:
                cnt = df_in.count()
                logger.debug("[load] row_count=%d", cnt)
            except Exception as e:
                logger.debug("[load] count failed: %s", e)

        # Normalize batch_id type if present
        if "batch_id" in df_in.columns:
            df_in = df_in.withColumn("batch_id", F.col("batch_id").cast("string"))

        # Source parquet files
        try:
            src_files = [
                r["file_path"]
                for r in (
                    df_in.select(F.col("_metadata.file_path").alias("file_path"))
                        .distinct()
                        .collect()
                )
            ]
            logger.debug("[src] parquet_files(count=%d)=%s", len(src_files), src_files)
        except Exception as e:
            logger.debug("[src] unable to fetch parquet files via _metadata: %s", e)
            src_files = []

        # Filter columns by reference (keep batch_id)
        logger.debug("[filter] before: batch_id in df_in? %s", "batch_id" in df_in.columns)
        df = schema_ut.filter_columns_by_reference(df_in, ref_weld_df, keep_extra_cols=["batch_id"])
        logger.debug("[filter] after: columns(%d)=%s", len(df.columns), df.columns)
        logger.debug("[filter] after: has batch_id? %s", "batch_id" in df.columns)

        # Cleaning steps (log columns after each)
        df = cleaning_ut.regex_clean_columns(df)
        logger.debug("[regex_clean_columns] cols(%d) has_batch_id=%s", len(df.columns), "batch_id" in df.columns)

        df = cleaning_ut.normalize_strings(df)
        logger.debug("[normalize_strings] cols(%d) has_batch_id=%s", len(df.columns), "batch_id" in df.columns)

        df = cleaning_ut.apply_comma_to_dot(df, ref_weld_df)
        logger.debug("[apply_comma_to_dot] cols(%d) has_batch_id=%s", len(df.columns), "batch_id" in df.columns)

        df, removed = cleaning_ut.drop_rows_with_invalid_int_values(df, ref_weld_df, log=False)
        logger.debug("[drop_rows_with_invalid_int_values] removed=%s cols(%d) has_batch_id=%s",
                     removed, len(df.columns), "batch_id" in df.columns)
        if log_counts:
            try:
                cnt = df.count()
                logger.debug("[post_drop_invalid_ints] row_count=%d", cnt)
            except Exception as e:
                logger.debug("[post_drop_invalid_ints] count failed: %s", e)

        # df = cleaning_ut.apply_datatype(df, ref_weld_df)
        df = cleaning_ut.apply_datatype(df, ref_weld_df, keep_extra_cols=["batch_id"])
        logger.debug("[apply_datatype] cols(%d) has_batch_id=%s", len(df.columns), "batch_id" in df.columns)

        if round_numeric:
            df = cleaning_ut.apply_roundoff_to_numeric_columns(df, ref_weld_df, log=False)
            logger.debug("[apply_roundoff_to_numeric_columns] cols(%d) has_batch_id=%s",
                         len(df.columns), "batch_id" in df.columns)

        df = cleaning_ut.fix_and_parse_timestamp(df, colname="time", use_try_to_timestamp=False)
        logger.debug("[fix_and_parse_timestamp] cols(%d) has_batch_id=%s",
                     len(df.columns), "batch_id" in df.columns)

        df = cleaning_ut.filter_out_of_range_rows(df, ref_weld_df)
        logger.debug("[filter_out_of_range_rows] cols(%d) has_batch_id=%s",
                     len(df.columns), "batch_id" in df.columns)

        if overwrite_existing_batch:
            _delete_silver_batch(spark, output_path, batch_id)

        logger.debug("[write] final cols(%d)=%s", len(df.columns), df.columns)
        logger.debug("[write] has batch_id? %s", "batch_id" in df.columns)
        writer = df.write.format("delta").mode("append")
        for k, v in options.items():
            writer = writer.option(k, v)
        if partition_by_batch and "batch_id" in df.columns:
            writer = writer.partitionBy("batch_id")
            logger.debug("[write] partitionBy=batch_id")
        else:
            logger.debug("[write] NO partitioning (batch_id missing or disabled)")

        writer.save(output_path)
        logger.debug("[write] completed output_path=%s", output_path)

        if src_files:
            logger.info("Silver processed files:\n%s", "\n".join(src_files))

        last_df = df
        logger.debug("=== SILVER BATCH END: %s ===", batch_id)

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