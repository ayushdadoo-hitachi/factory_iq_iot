"""
Gold Data Enrichment Pipeline (single sink + episodes)
- Reads Silver table
- Applies task features, machine state, segmentation, WPS mapping, movement features
- Drops intermediate columns
- Applies wire consumption (adds columns to rows)
- Writes Gold table to 'output_path' (single sink with wire columns)
- Writes wire episodes to 'wire_episodes_output_path'
"""

import importlib
# import logging
from typing import Optional, Tuple
from pyspark.sql import SparkSession, DataFrame

# IO
from core.io_utils import read_delta, write_delta

# Enrichment modules (module imports so reload works during development)
import enrichments.machine_state as machinestate; importlib.reload(machinestate)
import enrichments.task_features as taskfeatures; importlib.reload(taskfeatures)
import enrichments.segmentation as segment; importlib.reload(segment)
import enrichments.movement_features as movementfeatures; importlib.reload(movementfeatures)
import enrichments.cleanup as cleaningfeatures; importlib.reload(cleaningfeatures)
import enrichments.wps_mapping as wpsmapping; importlib.reload(wpsmapping)

# Wire consumption
import enrichments.wire_consumption as wireconsumption; importlib.reload(wireconsumption)

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)  

def run_stage(
    spark: SparkSession,
    *,
    # Inputs / outputs
    input_path: str,
    wps_ref_path: str,
    tps500current: str,
    techdevisrunning: str,
    output_path: str,

    # Wire consumption (end-of-pipeline; merged into Gold rows and episodes written)
    enable_wire_consumption: bool = True,
    wire_episodes_output_path: Optional[str] = None,   # required if enable_wire_consumption=True
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",
    write_mode: str = "overwrite",
    round_wire_outputs: bool = True,
    round_digits: int = 2,
    log_counts: bool = False,
) -> DataFrame:
    """
    Executes Gold stage:
      - Enrichment -> drop intermediate columns
      - (Optional) Wire consumption -> merges wire columns into DF + writes episodes
      - Write Gold (single output_path)
    Returns the final Gold DataFrame.
    """
    logger.info("--------------------------------------------------")
    logger.info("Starting Gold - Data Enrichment")
    logger.info("--------------------------------------------------")

    logger.debug("reading Silver input: %s", input_path)
    df = read_delta(spark, input_path)

    logger.debug("reading WPS reference: %s", wps_ref_path)
    wps_ref_df = read_delta(spark, wps_ref_path)

    logger.debug("deriving task features")
    df = taskfeatures.extract_active_task_num(df)
    df = taskfeatures.add_step_major(df)

    logger.debug("deriving machine state")
    df = machinestate.add_der_machine_state(df, tps500current, techdevisrunning)
    df = machinestate.add_welding_active(df)

    logger.debug("applying segmentation features")
    df = segment.add_atask_segment_id(df)
    df = segment.add_mstepnumber_segment_id(df)
    df = segment.add_d_activetask_f(df)
    df = segment.add_d_step_major_f(df)

    logger.debug("applying WPS mapping")
    df = wpsmapping.add_wps_joint_id(df, wps_ref_df)

    logger.debug("deriving movement features")
    df = movementfeatures.compute_der_euc_distance_spark(df)
    df = movementfeatures.compute_der_bigmove_spark(df, threshold=150.0)
    df = movementfeatures.add_der_euc_seg_id(df)

    logger.debug("dropping intermediate columns (pre-wire)")
    df = cleaningfeatures.drop_intermediate_columns(df)

    if enable_wire_consumption:
        if not wire_episodes_output_path:
            raise ValueError("wire_episodes_output_path is required when enable_wire_consumption=True")
        logger.debug("applying wire consumption")
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

        logger.debug("writing wire episodes to %s", wire_episodes_output_path)
        write_delta(df_episodes, wire_episodes_output_path, mode=write_mode)

    logger.debug("writing final table to %s", output_path)
    write_delta(df, output_path, mode=write_mode)
    logger.info("Gold - Data Enrichment Completed")
    return df