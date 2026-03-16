import importlib
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from core.io_utils import read_delta
import enrichments.machine_state as machinestate; importlib.reload(machinestate)
import enrichments.task_features as taskfeatures; importlib.reload(taskfeatures)
import enrichments.segmentation as segment; importlib.reload(segment)
import enrichments.movement_features as movementfeatures; importlib.reload(movementfeatures)
import enrichments.cleanup as cleaningfeatures; importlib.reload(cleaningfeatures)
import enrichments.wps_mapping as wpsmapping; importlib.reload(wpsmapping)
import enrichments.wire_consumption as wireconsumption; importlib.reload(wireconsumption)
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)

from core.stage_runner import StageRunner
from core.manifest_utils import delta_exists


def _list_batch_ids(spark: SparkSession, silver_path: str) -> List[str]:
    df_silver = spark.read.format("delta").load(silver_path)
    if "batch_id" not in df_silver.columns:
        raise ValueError("Silver table is missing 'batch_id' column.")
    return [r["batch_id"] for r in df_silver.select("batch_id").distinct().collect()]


def _delete_partition(spark: SparkSession, path: str, batch_id: str) -> None:
    if delta_exists(spark, path):
        spark.sql(f"DELETE FROM delta.`{path}` WHERE batch_id = '{batch_id}'")


def run_stage(
    spark: SparkSession,
    *,
    input_path: str,
    wps_ref_path: str,
    tps500current: str,
    techdevisrunning: str,
    output_path: str,
    enable_wire_consumption: bool = True,
    wire_episodes_output_path: Optional[str] = None,
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",
    write_mode: str = "append",
    round_wire_outputs: bool = True,
    round_digits: int = 2,
    log_counts: bool = False,
    manifest_path: Optional[str] = None,
    delta_options: Optional[Dict[str, str]] = None,
    partition_by_batch: bool = True,
    overwrite_existing_batch: bool = False
) -> DataFrame:
    logger.info("Starting Gold - Data Enrichment (Batch)")

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

    wps_ref_df = read_delta(spark, wps_ref_path)
    candidates = _list_batch_ids(spark, input_path)
    if not candidates:
        logger.info("No Silver batches found.")
        return spark.createDataFrame([], wps_ref_df.limit(0).schema)

    last_df: Optional[DataFrame] = None

    def process_one(batch_id: str) -> None:
        nonlocal last_df

        df_in = spark.read.format("delta").load(input_path).where(F.col("batch_id") == batch_id)

        try:
            src_files = [
                r["file_path"]
                for r in (
                    df_in.select(F.col("_metadata.file_path").alias("file_path"))
                        .distinct()
                        .collect()
                )
            ]
        except Exception:
            src_files = []

        df = df_in
        df = taskfeatures.extract_active_task_num(df)
        df = taskfeatures.add_step_major(df)
        df = machinestate.add_der_machine_state(df, tps500current, techdevisrunning)
        df = machinestate.add_welding_active(df)
        df = segment.add_atask_segment_id(df)
        df = segment.add_mstepnumber_segment_id(df)
        df = segment.add_d_activetask_f(df)
        df = segment.add_d_step_major_f(df)
        df = wpsmapping.add_wps_joint_id(df, wps_ref_df)
        df = movementfeatures.compute_der_euc_distance_spark(df)
        df = movementfeatures.compute_der_bigmove_spark(df, threshold=150.0)
        df = movementfeatures.add_der_euc_seg_id(df)
        df = cleaningfeatures.drop_intermediate_columns(df)

        if enable_wire_consumption:
            if not wire_episodes_output_path:
                raise ValueError("wire_episodes_output_path is required when enable_wire_consumption=True")
            df_with_wire, df_episodes = wireconsumption._apply_wire_consumption_to_df(
                df,
                time_col=time_col,
                wfs_col=wfs_col,
                wfs_unit=wfs_unit,
                round_outputs=round_wire_outputs,
                round_digits=round_digits,
                log_counts=log_counts,
            )
            df = df_with_wire

            if overwrite_existing_batch:
                _delete_partition(spark, wire_episodes_output_path, batch_id)

            writer_ep = df_episodes.write.format("delta").mode("append")
            for k, v in options.items():
                writer_ep = writer_ep.option(k, v)
            if partition_by_batch and "batch_id" in df_episodes.columns:
                writer_ep = writer_ep.partitionBy("batch_id")
            writer_ep.save(wire_episodes_output_path)

        if overwrite_existing_batch:
            _delete_partition(spark, output_path, batch_id)

        writer = df.write.format("delta").mode("append")
        for k, v in options.items():
            writer = writer.option(k, v)
        if partition_by_batch and "batch_id" in df.columns:
            writer = writer.partitionBy("batch_id")
        writer.save(output_path)

        if src_files:
            logger.info("Gold processed files:\n%s", "\n".join(src_files))

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
        return spark.createDataFrame([], wps_ref_df.limit(0).schema)
