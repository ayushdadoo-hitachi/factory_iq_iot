# enrichments/wire_consumption.py

import time
# import logging
import importlib
from typing import Tuple, Optional, List
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    BooleanType,
    TimestampType,
    DateType,
    FloatType,
)

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 

# ----------------------------
# Validation
# ----------------------------
def require_columns(df: DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Required column(s) missing: {missing}")

# ----------------------------
# WFS helpers
# ----------------------------
def infer_wfs_to_mps_factor(df: DataFrame, wfs_col: str) -> float:
    logger.debug("inferring WFS unit factor from '%s' (median heuristic)", wfs_col)
    med = df.selectExpr(f"percentile_approx({wfs_col}, 0.5, 1000) AS med").collect()[0]["med"]
    try:
        if med is None:
            return 1.0 / 60.0
        med = float(med)
    except Exception:
        return 1.0 / 60.0

    # Heuristic bands
    if 0.5 <= med <= 30.0:
        return 1.0 / 60.0      # m/min -> m/s
    if 50.0 <= med <= 500.0:
        return 1e-3            # mm/s  -> m/s
    return 1.0 / 60.0          # default

def resolve_wfs_factor(df: DataFrame, wfs_col: str, wfs_unit: str) -> float:
    u = (wfs_unit or "auto").strip().lower()
    if u == "auto":       return infer_wfs_to_mps_factor(df, wfs_col)
    if u == "m_per_min":  return 1.0 / 60.0
    if u == "mm_per_s":   return 1e-3
    if u == "m_per_s":    return 1.0
    raise ValueError("wfs_unit must be one of: 'auto','m_per_min','mm_per_s','m_per_s'")

# ----------------------------
# Core transforms
# ----------------------------
def enrich_wire_consumption(
    df: DataFrame,
    time_col: str,
    wfs_col: str,
    wfs_unit: str,
) -> Tuple[DataFrame, float]:
    """
    Adds the following columns to the input DataFrame:
      der_ts, der_ts_day, der_dt_s, der_wfs_mps,
      der_wire_len_m_sample, der_wire_len_m_cumsum, der_episode_id

    Notes:
    - Keeps the original `time_col` (no drop), but uses `der_ts` for computations.
    - Requires columns: [time_col, wfs_col, "arcseam_isactive"].
    """
    t0 = time.perf_counter()
    logger.debug("enrichment start (wire consumption features)")
    require_columns(df, [time_col, wfs_col, "arcseam_isactive"])

    # Ensure time is timestamp; keep original column
    df = df.withColumn(time_col, F.to_timestamp(F.col(time_col)))

    # Processing-friendly timestamps
    df = (
        df.withColumn("der_ts", F.col(time_col).cast(TimestampType()))
          .withColumn("der_ts_day", F.to_date("der_ts").cast(DateType()))
    )

    # Active arc condition
    arc_expr = (F.col("arcseam_isactive") != F.lit(0)).cast(BooleanType())

    # Per-day, time-ordered window
    w = Window.partitionBy("der_ts_day").orderBy("der_ts")

    # Delta seconds
    df = (
        df.withColumn("der_ts_prev", F.lag("der_ts").over(w))
          .withColumn(
              "der_dt_s",
              F.when(F.col("der_ts_prev").isNull(), F.lit(0.0))
               .otherwise(F.col("der_ts").cast("long") - F.col("der_ts_prev").cast("long"))
               .cast(DoubleType())
          )
    )

    # WFS conversion factor
    factor = resolve_wfs_factor(df, wfs_col, wfs_unit)
    logger.debug("conversion factor used (to m/s): %.6f", factor)

    # Wire length per sample and cumulative
    df = (
        df.withColumn("der_wfs_mps", F.col(wfs_col).cast(DoubleType()) * F.lit(factor))
          .withColumn(
              "der_wire_len_m_sample",
              F.when(arc_expr, F.col("der_wfs_mps") * F.col("der_dt_s"))
               .otherwise(F.lit(0.0))
               .cast(DoubleType())
          )
          .withColumn(
              "der_wire_len_m_cumsum",
              F.sum("der_wire_len_m_sample").over(w).cast(DoubleType())
          )
    )

    # Episode identification (within day)
    arc_prev = F.lag(arc_expr.cast("int"), 1, 0).over(w)
    start_flag = (arc_expr.cast("int") - arc_prev) == F.lit(1)
    df = (
        df.withColumn("der_episode_start_flag", start_flag.cast("int"))
          .withColumn("der_episode_id_day", F.sum("der_episode_start_flag").over(w))
          .withColumn(
              "der_episode_id",
              F.when(
                  arc_expr,
                  F.concat_ws(
                      "_",
                      F.col("der_ts_day").cast("string"),
                      F.col("der_episode_id_day").cast("string")
                  )
              ).otherwise(F.lit(None))
          )
          .drop("der_ts_prev", "der_episode_start_flag", "der_episode_id_day")
          .repartition(F.col("der_ts_day"))
          .sortWithinPartitions("der_ts")
    )

    logger.debug("enrichment completed in %.2fs", time.perf_counter() - t0)
    return df, factor


def compute_episode_kpis(df: DataFrame) -> DataFrame:
    """
    Aggregates episode-level KPIs from a row-level DataFrame produced by enrich_wire_consumption.
    Requires:
      - der_episode_id (nullable)
      - der_ts, der_ts_day, der_dt_s, der_wire_len_m_sample, der_wfs_mps
    """
    logger.debug("aggregating episode KPIs")
    d = df.where(F.col("der_episode_id").isNotNull())

    g = d.groupBy("der_episode_id")
    ep = (
        g.agg(
            F.min("der_ts").alias("der_start_ts"),
            F.max("der_ts").alias("der_end_ts"),
            F.first("der_ts_day").alias("der_ts_day"),
            F.sum("der_dt_s").alias("der_duration_s"),
            F.sum("der_wire_len_m_sample").alias("der_wire_len_m_total"),
            F.avg("der_wfs_mps").alias("der_avg_wfs_mps"),
        )
        .withColumn("der_duration_s", F.col("der_duration_s").cast(DoubleType()))
        .withColumn("der_wire_len_m_total", F.col("der_wire_len_m_total").cast(DoubleType()))
        .withColumn("der_avg_wfs_mps", F.col("der_avg_wfs_mps").cast(DoubleType()))
    )

    # Day-level and global cumulative totals
    w_day = Window.partitionBy("der_ts_day").orderBy("der_start_ts") \
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    w_all = Window.orderBy("der_start_ts") \
                  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    ep = (
        ep
        .withColumn("der_episode_seq_day", F.row_number().over(Window.partitionBy("der_ts_day").orderBy("der_start_ts")))
        .withColumn("der_wire_len_m_cum_day", F.sum("der_wire_len_m_total").over(w_day).cast(DoubleType()))
        .withColumn("der_wire_len_m_cum_global", F.sum("der_wire_len_m_total").over(w_all).cast(DoubleType()))
        .orderBy("der_ts_day", "der_start_ts")
    )

    logger.debug("episode KPIs computed")
    return ep


def compute_daily_wire_from_rows(df_rows: DataFrame) -> DataFrame:
    """
    Computes daily wire totals from row-level cumulative values.
    Requires:
      - der_ts_day
      - der_wire_len_m_cumsum
    """
    logger.debug("computing daily totals from row-level cumulative")
    return (
        df_rows.groupBy("der_ts_day")
               .agg(F.max("der_wire_len_m_cumsum").alias("der_wire_len_m_total_day"))
               .orderBy("der_ts_day")
    )

# ----------------------------
# (Optional) simple rounding for numeric columns
# ----------------------------
def round_all_numeric_columns(df: DataFrame, ndigits: int = 2) -> DataFrame:
    numeric = [f.name for f in df.schema.fields if isinstance(f.dataType, (DoubleType, FloatType))]
    out = df
    for c in numeric:
        out = out.withColumn(c, F.round(F.col(c), ndigits))
    logger.debug("rounded %d numeric columns to %d decimals", len(numeric), ndigits)
    return out

# ----------------------------
# Helper kept in this file (called by Gold)
# ----------------------------
def _apply_wire_consumption_to_df(
    df_in: DataFrame,
    *,
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",
    round_outputs: bool = True,
    round_digits: int = 2,
    log_counts: bool = False,
) -> Tuple[DataFrame, DataFrame]:
    """
    Applies wire consumption to the given DataFrame and returns:
      - df_rows_with_input rows augmented with wire columns
      - df_episodes: episodes derived from df_rows_with_wire

    This function performs no I/O (no reads/writes). It is intended
    to be called by the Gold stage which handles persistence.
    """
    logger.debug("apply helper start")
    if log_counts:
        try:
            logger.debug("input row count: %s", df_in.count())
        except Exception:
            logger.warning("input row count not available")

    df_rows_with_wire, factor = enrich_wire_consumption(
        df_in,
        time_col=time_col,
        wfs_col=wfs_col,
        wfs_unit=wfs_unit,
    )
    logger.debug("conversion factor used: %.6f", factor)

    df_episodes = compute_episode_kpis(df_rows_with_wire)

    if round_outputs:
        df_rows_with_wire = round_all_numeric_columns(df_rows_with_wire, ndigits=round_digits)
        df_episodes = round_all_numeric_columns(df_episodes, ndigits=round_digits)
        logger.debug("rounding applied to rows and episodes")

    if log_counts:
        try:
            logger.debug("output row count: %s", df_rows_with_wire.count())
            logger.debug("episodes count: %s", df_episodes.count())
        except Exception:
            logger.warning("output counts not available")

    logger.debug("apply helper completed")
    return df_rows_with_wire, df_episodes