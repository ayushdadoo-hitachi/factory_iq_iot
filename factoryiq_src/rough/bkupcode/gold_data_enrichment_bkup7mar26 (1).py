"""
Gold – Data Enrichment Pipeline (with optional Wire Feed Consumption)
- Reads Silver table
- Applies segmentation, WPS mapping, movement features
- Drops intermediate columns
- Applies wire consumption (adds columns)
- Writes Gold table (now includes wire columns)
- Writes wire episodes (and optionally wire rows to a separate table)
"""

import importlib
import logging
from typing import Optional, Tuple
from pyspark.sql import SparkSession, DataFrame

# Core IO
from core.io_utils import read_delta, write_delta

# Enrichment modules (module imports so reload works during dev)
import enrichments.machine_state as machinestate; importlib.reload(machinestate)
import enrichments.task_features as taskfeatures; importlib.reload(taskfeatures)
import enrichments.segmentation as segment; importlib.reload(segment)
import enrichments.movement_features as movementfeatures; importlib.reload(movementfeatures)
import enrichments.cleanup as cleaningfeatures; importlib.reload(cleaningfeatures)
import enrichments.wps_mapping as wpsmapping; importlib.reload(wpsmapping)

# Wire consumption (optional step)
import enrichments.wire_consumption as wireconsumption; importlib.reload(wireconsumption)

logger = logging.getLogger(__name__)


def run_wire_consumption_stage(
    spark: SparkSession,
    *,
    input_df: Optional[DataFrame] = None,   # prefer DF to guarantee freshness
    input_path: Optional[str] = None,       # fallback if DF not provided
    output_path: Optional[str] = None,      # optional: write wire 'rows' table (if desired)
    output_episodes_path: str,              # required to persist episodes table
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",                 # 'auto' | 'm_per_min' | 'mm_per_s' | 'm_per_s'
    write_mode: str = "overwrite",
    round_outputs: bool = True,
    round_digits: int = 2,
    log_counts: bool = False,
) -> Tuple[DataFrame, DataFrame]:
    """
    Runs the wire feed consumption pipeline on the provided DF (or path):
      read/accept -> transform -> (optional round) -> write outputs (episodes always; rows optional)
    Returns:
      df_rows (the input rows augmented with wire columns),
      df_episodes (episode aggregates)
    """
    logger.info("--------------------------------------------------")
    logger.info("Starting Gold – Wire Feed Consumption (end of pipeline)")
    logger.info("--------------------------------------------------")

    if input_df is None:
        if not input_path:
            raise ValueError("Provide either input_df or input_path for wire consumption.")
        df_in = read_delta(spark, input_path)
    else:
        df_in = input_df

    logger.info("Wire input columns: %s", df_in.columns)

    if log_counts:
        try:
            logger.info("Wire input row count: %s", df_in.count())
        except Exception:
            logger.warning("Failed to count input rows (skipping).")

    # Transform (adds wire columns onto per-row dataset)
    df_rows, factor = wireconsumption.enrich_wire_consumption(
        df_in, time_col=time_col, wfs_col=wfs_col, wfs_unit=wfs_unit
    )
    logger.info("🧮 WFS factor used: %.6f", factor)

    # Build episodes
    df_episodes = wireconsumption.compute_episode_kpis(df_rows)

    if log_counts:
        try:
            logger.info("Rows (with wire columns): %s", df_rows.count())
            logger.info("Episodes (aggregates): %s", df_episodes.count())
        except Exception:
            logger.warning("Failed to count output rows (skipping).")

    # Optional rounding
    if round_outputs:
        logger.info("🔄 Rounding outputs to %d decimals", round_digits)
        df_rows = wireconsumption.round_all_numeric_columns(df_rows, ndigits=round_digits)
        df_episodes = wireconsumption.round_all_numeric_columns(df_episodes, ndigits=round_digits)

    # IO: write (episodes are typically needed by downstream consumers)
    if output_path:
        write_delta(df_rows, output_path, mode=write_mode)
        logger.info("Wire rows written to:     %s", output_path)
    write_delta(df_episodes, output_episodes_path, mode=write_mode)
    logger.info("Wire episodes written to: %s", output_episodes_path)

    logger.info("🎉 Wire Feed Consumption completed")
    return df_rows, df_episodes


def run_stage(
    spark: SparkSession,
    *,
    # Enrichment inputs/outputs
    input_path: str,
    wps_ref_path: str,
    tps500current: str,
    techdevisrunning: str,
    output_path: str,

    # Optional wire consumption step (end of pipeline)
    enable_wire_consumption: bool = True,          # default True since you want it applied at the end
    wire_output_path: Optional[str] = None,        # optional: persist wire rows as a dedicated table
    wire_output_episodes_path: Optional[str] = None,  # required if enable_wire_consumption=True
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",
    write_mode: str = "overwrite",
    round_wire_outputs: bool = True,
    round_digits: int = 2,
    log_counts: bool = False,
) -> DataFrame:
    """
    Orchestrates Gold stage:
      - Enrichment -> drop intermediate columns
      - Optional Wire Feed step (adds columns) -> write Gold with wire columns
      - Episodes table is also written if wire step is enabled.

    Returns:
      Final Gold DataFrame (which now includes wire columns when enabled).
    """
    logger.info("--------------------------------------------------")
    logger.info("Starting Gold – Data Enrichment")
    logger.info("--------------------------------------------------")

    # === Enrichment ===
    logger.info("📥 Reading Silver data")
    df = read_delta(spark, input_path)

    logger.info("📥 Reading WPS reference")
    wps_ref_df = read_delta(spark, wps_ref_path)

    logger.info("🧩 Extracting active task number")
    df = taskfeatures.extract_active_task_num(df)

    logger.info("🧩 Extracting step major number")
    df = taskfeatures.add_step_major(df)

    logger.info("⚙️ Adding machine state")
    df = machinestate.add_der_machine_state(df, tps500current, techdevisrunning)
    df = machinestate.add_welding_active(df)

    logger.info("✂️ Applying segmentation")
    df = segment.add_atask_segment_id(df)
    df = segment.add_mstepnumber_segment_id(df)
    df = segment.add_d_activetask_f(df)
    df = segment.add_d_step_major_f(df)

    logger.info("🔗 Applying WPS mapping")
    df = wpsmapping.add_wps_joint_id(df, wps_ref_df)

    logger.info("📐 Computing movement features")
    df = movementfeatures.compute_der_euc_distance_spark(df)
    df = movementfeatures.compute_der_bigmove_spark(df, threshold=150.0)
    df = movementfeatures.add_der_euc_seg_id(df)

    # 🧹 Drop intermediate columns BEFORE wire so wire columns are retained in final DF
    logger.info("🧹 Dropping intermediate columns (pre-wire)")
    df = cleaningfeatures.drop_intermediate_columns(df)

    # === Wire consumption (END) ===
    if enable_wire_consumption:
        if not wire_output_episodes_path:
            raise ValueError(
                "wire_output_episodes_path is required when enable_wire_consumption=True"
            )

        # Run wire on the cleaned DF so added wire columns become part of the final Gold DF
        df_rows_with_wire, _df_episodes = run_wire_consumption_stage(
            spark=spark,
            input_df=df,                                   # run on in-memory cleaned DF
            output_path=wire_output_path,                  # optional separate 'rows' table for wire
            output_episodes_path=wire_output_episodes_path,
            time_col=time_col,
            wfs_col=wfs_col,
            wfs_unit=wfs_unit,
            write_mode=write_mode,
            round_outputs=round_wire_outputs,
            round_digits=round_digits,
            log_counts=log_counts,
        )

        # Replace Gold DF with the augmented version that includes wire columns
        df = df_rows_with_wire

    # 📤 Write final Gold (includes wire columns if enabled)
    logger.info("📤 Writing Gold table")
    write_delta(df, output_path)
    logger.info("✅ Gold enrichment written (with wire columns: %s)", enable_wire_consumption)

    logger.info("🎉 Gold stage completed")
    return df