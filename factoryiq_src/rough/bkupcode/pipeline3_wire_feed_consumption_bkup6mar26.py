# pipelines/pipeline3_wire_feed_consumption.py
# -------------------------------------------------------------------
# Orchestrator for Wire Feed Consumption (uses enrichments/wire_consumption.py)
# -------------------------------------------------------------------

from typing import Optional, Tuple
import logging
import time
import importlib

from pyspark.sql import SparkSession, DataFrame

from core.logging_config import setup_logging
from core.io_utils import read_delta, write_delta

import enrichments.wire_consumption as ewc
importlib.reload(ewc)

from enrichments.wire_consumption import (
    enrich_wire_consumption,
    compute_episode_kpis,
    compute_daily_wire_from_rows,   # keep exported for external callers
    round_all_numeric_columns       # simple rounding helper
)

def run_pipeline3_wire_feed_consumption(
    spark: SparkSession,
    input_path: str,
    output_rows_path: str,
    output_episodes_path: str,
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",     # 'auto' | 'm_per_min' | 'mm_per_s' | 'm_per_s'
    write_mode: str = "overwrite",
    # Logging knobs
    log_counts: bool = False,
    log_level: int = logging.INFO,
    # Rounding knobs
    round_outputs: bool = True,
    round_digits: int = 2,
) -> Tuple[DataFrame, DataFrame]:
    """
    Orchestrates the wire consumption pipeline:
      read input -> transform -> (optional round) -> write outputs
    """
    # Configure logging per run so jobs can override level
    setup_logging()
    logging.getLogger().setLevel(log_level)
    logger = logging.getLogger(__name__)

    t0_all = time.perf_counter()
    logger.info("--------------------------------------------------")
    logger.info("Starting Stage 3 – Wire Feed Consumption")
    logger.info("--------------------------------------------------")
    logger.info(f"Params: input={input_path}, rows_out={output_rows_path}, episodes_out={output_episodes_path}, "
                f"time_col={time_col}, wfs_col={wfs_col}, wfs_unit={wfs_unit}, mode={write_mode}, "
                f"round_outputs={round_outputs}, round_digits={round_digits}")

    try:
        # IO: read
        df_in = read_delta(spark, input_path)
        logger.info(f"Input columns: {df_in.columns}")

        if log_counts:
            try:
                logger.info(f"Input row count: {df_in.count()}")
            except Exception:
                logger.warning("Failed to count input rows (skipping).")

        # Transform
        df_rows, factor = enrich_wire_consumption(
            df_in, time_col=time_col, wfs_col=wfs_col, wfs_unit=wfs_unit
        )
        logger.info(f"🧮 WFS factor used: {factor:.6f}")

        df_episodes = compute_episode_kpis(df_rows)

        if log_counts:
            try:
                logger.info(f"Rows (sample table): {df_rows.count()}")
                logger.info(f"Rows (episodes table): {df_episodes.count()}")
            except Exception:
                logger.warning("Failed to count output rows (skipping).")

        # Optional rounding (simple)
        if round_outputs:
            logger.info(f"🔄 Rounding outputs to {round_digits} decimals")
            df_rows = round_all_numeric_columns(df_rows, ndigits=round_digits)
            df_episodes = round_all_numeric_columns(df_episodes, ndigits=round_digits)

        # IO: write
        write_delta(df_rows, output_rows_path, mode=write_mode)
        write_delta(df_episodes, output_episodes_path, mode=write_mode)

        logger.info(f"Rows written to:     {output_rows_path}")
        logger.info(f"Episodes written to: {output_episodes_path}")
        logger.info(f"🎉 Stage 3 completed in {time.perf_counter() - t0_all:.2f}s")

        return df_rows, df_episodes

    except Exception:
        logger = logging.getLogger(__name__)
        logger.exception("❌ Stage 3 failed with an exception.")
        raise