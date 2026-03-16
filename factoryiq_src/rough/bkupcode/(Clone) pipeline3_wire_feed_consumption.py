# Databricks: pipelines/pipeline3_wire_feed_consumption.py
# -------------------------------------------------------------------
# Wire consumption from existing data:
#   - Arc ON: auto-detected from columns present in Stage-2
#   - Wire length (m) = ∑ (WFS_mps * dt_s) where arc_on is True
# -------------------------------------------------------------------

from typing import Optional, Tuple
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, TimestampType, DateType

# ----------------------------
# IO helpers
# ----------------------------
def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)

def write_delta(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df.write.format("delta").mode(mode).save(path)

# ----------------------------
# Auto-detect helpers
# ----------------------------
def has_col(df: DataFrame, name: str) -> bool:
    return name in df.columns

def build_arc_on_expr(
    df: DataFrame,
    weldstate_col: Optional[str] = None,
    prefer_derived: bool = False,
    current_threshold: float = 5.0
):
    """
    Decide an arc_on boolean expression from available columns:
      Priority:
        1) der_welding_active (bool)
        2) arcseam_isactive (bool/int)
        3) weldstate_col > 0 (if provided & exists)
        4) derived: (tps500i_current > threshold) AND (techdevs_isrunning==1 or tps500i_isrunning==1)
    """
    # 1) der_welding_active
    if has_col(df, "der_welding_active"):
        return F.col("der_welding_active").cast(BooleanType())

    # 2) arcseam_isactive
    if has_col(df, "arcseam_isactive"):
        return (F.col("arcseam_isactive") != F.lit(0)).cast(BooleanType())

    # 3) weldstate > 0 (only if explicitly given and present)
    if weldstate_col and has_col(df, weldstate_col):
        return (F.col(weldstate_col).cast("double") > F.lit(0)).cast(BooleanType())

    # 4) derived from current + isrunning
    # techdevs_isrunning OR tps500i_isrunning
    isrunning_col = None
    if has_col(df, "techdevs_isrunning"):
        isrunning_col = "techdevs_isrunning"
    elif has_col(df, "tps500i_isrunning"):
        isrunning_col = "tps500i_isrunning"

    if has_col(df, "tps500i_current") and isrunning_col:
        return (F.col("tps500i_current").cast("double") > F.lit(current_threshold)) & (F.col(isrunning_col) != F.lit(0))

    # If absolutely nothing available, return False to be safe
    return F.lit(False).cast(BooleanType())

def infer_wfs_to_mps_factor(df: DataFrame, wfs_col: str) -> float:
    """
    Heuristic:
      - median in [0.5..30]  => assume m/min -> 1/60
      - median in [50..500]  => assume mm/s  -> 1e-3
      - default: m/min -> 1/60
    """
    med = df.selectExpr(f"percentile_approx({wfs_col}, 0.5, 1000) AS med").collect()[0]["med"]
    if med is None:
        return 1.0 / 60.0
    try:
        med = float(med)
    except Exception:
        return 1.0 / 60.0

    if 0.5 <= med <= 30.0:
        return 1.0 / 60.0
    if 50.0 <= med <= 500.0:
        return 1e-3
    return 1.0 / 60.0

def resolve_wfs_factor(df: DataFrame, wfs_col: str, wfs_unit: str) -> float:
    if wfs_unit == "auto":
        return infer_wfs_to_mps_factor(df, wfs_col)
    if wfs_unit == "m_per_min":
        return 1.0 / 60.0
    if wfs_unit == "mm_per_s":
        return 1e-3
    if wfs_unit == "m_per_s":
        return 1.0
    raise ValueError("wfs_unit must be one of: 'auto','m_per_min','mm_per_s','m_per_s'")

# ----------------------------
# Enrichment
# ----------------------------
def enrich_wire_consumption(
    df: DataFrame,
    time_col: str,
    weldstate_col: Optional[str],
    wfs_col: str,
    wfs_unit: str,
    arc_on_rule: Optional[str],  # kept for compatibility, but we auto-detect
    current_threshold: float = 5.0,
    day_window: bool = True
) -> Tuple[DataFrame, float]:
    """
    Adds:
      - ts, ts_day
      - arc_on (auto-detected)
      - dt_s
      - wfs_mps
      - wire_len_m_sample
      - wire_len_m_cumsum
      - episode_id
    Partitions analytic windows by ts_day to avoid global-window warning.
    """
    # Ensure timestamp
    df = df.withColumn(time_col, F.to_timestamp(F.col(time_col)))\
           .withColumn("ts", F.col(time_col).cast(TimestampType()))\
           .drop(time_col)

    # Day key for window partition (removes "no partition defined" warning and improves parallelism)
    df = df.withColumn("ts_day", F.to_date("ts").cast(DateType()))

    # Build arc_on
    arc_on_expr = build_arc_on_expr(
        df,
        weldstate_col=weldstate_col,
        current_threshold=current_threshold
    )
    df = df.withColumn("arc_on", arc_on_expr.cast(BooleanType()))

    # Compute dt_s (lag within day)
    w = Window.partitionBy("ts_day").orderBy("ts")
    df = df.withColumn("ts_prev", F.lag("ts").over(w))\
           .withColumn(
               "dt_s",
               F.when(F.col("ts_prev").isNull(), F.lit(0.0))
                .otherwise(F.col("ts").cast("long") - F.col("ts_prev").cast("long"))
                .cast(DoubleType())
           )

    # WFS factor
    factor = resolve_wfs_factor(df, wfs_col, wfs_unit)

    # Wire length per sample
    df = df.withColumn("wfs_mps", F.col(wfs_col).cast(DoubleType()) * F.lit(factor))\
           .withColumn(
               "wire_len_m_sample",
               F.when(F.col("arc_on"), F.col("wfs_mps") * F.col("dt_s"))
                .otherwise(F.lit(0.0)).cast(DoubleType())
           )

    # Cumulative wire length (within day)
    df = df.withColumn(
        "wire_len_m_cumsum",
        F.sum("wire_len_m_sample").over(w).cast(DoubleType())
    )

    # Episode id: start where arc_on turns False->True within day
    arc_prev = F.lag(F.col("arc_on").cast("int"), 1, 0).over(w)
    start_flag = (F.col("arc_on").cast("int") - arc_prev) == F.lit(1)
    df = df.withColumn("episode_start_flag", start_flag.cast("int"))\
           .withColumn("episode_id_day",
                       F.sum("episode_start_flag").over(w))\
           .withColumn(
               "episode_id",
               F.when(F.col("arc_on"), F.concat_ws("_", F.col("ts_day").cast("string"),
                                                   F.col("episode_id_day").cast("string")))
                .otherwise(F.lit(None))
           )

    # Housekeeping
    df = df.drop("ts_prev", "episode_start_flag", "episode_id_day")

    # Repartition by ts_day for better downstream performance
    df = df.repartition(F.col("ts_day")).sortWithinPartitions("ts")

    return df, factor

# ----------------------------
# Episode KPIs
# ----------------------------
def compute_episode_kpis(df: DataFrame) -> DataFrame:
    d = df.where(F.col("arc_on") & F.col("episode_id").isNotNull())
    g = d.groupBy("episode_id")
    out = (
        g.agg(
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts"),
            F.first("ts_day").alias("ts_day"),
            F.sum("dt_s").alias("duration_s"),
            F.sum("wire_len_m_sample").alias("wire_len_m_total"),
            F.avg("wfs_mps").alias("avg_wfs_mps"),
        )
        .withColumn("duration_s", F.col("duration_s").cast(DoubleType()))
        .withColumn("wire_len_m_total", F.col("wire_len_m_total").cast(DoubleType()))
        .withColumn("avg_wfs_mps", F.col("avg_wfs_mps").cast(DoubleType()))
        .orderBy("ts_day", "start_ts")
    )
    return out

# ----------------------------
# Public entry point
# ----------------------------
def run_pipeline3_wire_feed_consumption(
    spark: SparkSession,
    input_path: str,
    output_rows_path: str,
    output_episodes_path: str,
    time_col: str = "time",
    # If your Stage-2 actually has weldstate, pass it; else leave None
    weldstate_col: Optional[str] = None,   # e.g., "tps500i_weldstate"
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",                # 'auto' | 'm_per_min' | 'mm_per_s' | 'm_per_s'
    arc_on_rule: Optional[str] = None,     # kept for compatibility; auto-detect used
    current_threshold: float = 5.0,
    write_mode: str = "overwrite"
) -> Tuple[DataFrame, DataFrame]:

    df_in = read_delta(spark, input_path)

    # Enrich
    df_rows, factor = enrich_wire_consumption(
        df_in,
        time_col=time_col,
        weldstate_col=weldstate_col,
        wfs_col=wfs_col,
        wfs_unit=wfs_unit,
        arc_on_rule=arc_on_rule,
        current_threshold=current_threshold,
        day_window=True
    )

    # Episodes
    df_episodes = compute_episode_kpis(df_rows)

    # Write
    write_delta(df_rows, output_rows_path, mode=write_mode)
    write_delta(df_episodes, output_episodes_path, mode=write_mode)

    print(f"[wire] WFS unit factor used: {factor:.6f} (to convert {wfs_col} -> m/s)")
    print(f"[wire] Rows written to: {output_rows_path}")
    print(f"[wire] Episodes written to: {output_episodes_path}")

    return df_rows, df_episodes