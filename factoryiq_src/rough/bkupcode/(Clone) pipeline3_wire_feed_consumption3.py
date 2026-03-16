# Databricks: pipelines/pipeline3_wire_feed_consumption.py
# -------------------------------------------------------------------
# Wire consumption from existing data (IGM / Stage-2):
#   - Arc ON = (arcseam_isactive != 0)  [no extra arc_on column created]
#   - All derived columns are prefixed with 'der_'
#   - Wire length (m) = ∑ (der_wfs_mps * der_dt_s) while arc is active
#   - Episodes table includes cumulative wire usage (day-wise + global)
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
# Column helpers
# ----------------------------
def has_col(df: DataFrame, name: str) -> bool:
    return name in df.columns

def _require_columns(df: DataFrame, cols: list) -> None:
    missing = [c for c in cols if not has_col(df, c)]
    if missing:
        raise ValueError(f"Required column(s) missing: {missing}")

# ----------------------------
# WFS unit helpers
# ----------------------------
def infer_wfs_to_mps_factor(df: DataFrame, wfs_col: str) -> float:
    """
    Heuristic (based on median of the feed column):
      - median in [0.5..30]  => assume m/min  -> factor 1/60
      - median in [50..500]  => assume mm/s   -> factor 1e-3
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
    unit = (wfs_unit or "auto").strip().lower()
    if unit == "auto":
        return infer_wfs_to_mps_factor(df, wfs_col)
    if unit == "m_per_min":
        return 1.0 / 60.0
    if unit == "mm_per_s":
        return 1e-3
    if unit == "m_per_s":
        return 1.0
    raise ValueError("wfs_unit must be one of: 'auto','m_per_min','mm_per_s','m_per_s'")

# ----------------------------
# Enrichment (all 'der_' columns)
# ----------------------------
def enrich_wire_consumption(
    df: DataFrame,
    time_col: str,
    wfs_col: str,
    wfs_unit: str,
    day_window: bool = True
) -> Tuple[DataFrame, float]:
    """
    Adds (derived) columns:
      - der_ts, der_ts_day
      - der_dt_s
      - der_wfs_mps
      - der_wire_len_m_sample
      - der_wire_len_m_cumsum
      - der_episode_id

    Notes:
      * Arc activity is taken directly from arcseam_isactive != 0 (no arc_on column is created).
      * Windows are partitioned by der_ts_day for parallelism and correctness.
    """
    # Validate required inputs
    _require_columns(df, [time_col, wfs_col, "arcseam_isactive"])

    # Ensure timestamp
    df = (
        df.withColumn(time_col, F.to_timestamp(F.col(time_col)))
          .withColumn("der_ts", F.col(time_col).cast(TimestampType()))
          .drop(time_col)
    )

    # Day key for window partition
    df = df.withColumn("der_ts_day", F.to_date("der_ts").cast(DateType()))

    # Arc expression (no new arc column persisted)
    arc_expr = (F.col("arcseam_isactive") != F.lit(0)).cast(BooleanType())

    # Compute der_dt_s (lag within day)
    w = Window.partitionBy("der_ts_day").orderBy("der_ts")
    df = (
        df.withColumn("der_ts_prev", F.lag("der_ts").over(w))
          .withColumn(
              "der_dt_s",
              F.when(F.col("der_ts_prev").isNull(), F.lit(0.0))
               .otherwise(F.col("der_ts").cast("long") - F.col("der_ts_prev").cast("long"))
               .cast(DoubleType())
          )
    )

    # WFS factor
    factor = resolve_wfs_factor(df, wfs_col, wfs_unit)

    # Wire length per sample (only when arc active)
    df = (
        df.withColumn("der_wfs_mps", F.col(wfs_col).cast(DoubleType()) * F.lit(factor))
          .withColumn(
              "der_wire_len_m_sample",
              F.when(arc_expr, F.col("der_wfs_mps") * F.col("der_dt_s"))
               .otherwise(F.lit(0.0))
               .cast(DoubleType())
          )
    )

    # Cumulative wire length (within day)
    df = df.withColumn(
        "der_wire_len_m_cumsum",
        F.sum("der_wire_len_m_sample").over(w).cast(DoubleType())
    )

    # Episode id: start where arc turns False->True within day
    arc_prev = F.lag(arc_expr.cast("int"), 1, 0).over(w)
    start_flag = (arc_expr.cast("int") - arc_prev) == F.lit(1)
    df = (
        df.withColumn("der_episode_start_flag", start_flag.cast("int"))
          .withColumn("der_episode_id_day", F.sum("der_episode_start_flag").over(w))
          .withColumn(
              "der_episode_id",
              F.when(arc_expr,
                     F.concat_ws("_",
                                 F.col("der_ts_day").cast("string"),
                                 F.col("der_episode_id_day").cast("string")))
               .otherwise(F.lit(None))
          )
          .drop("der_ts_prev", "der_episode_start_flag", "der_episode_id_day")
    )

    # Repartition by day for downstream performance and sorted output
    df = df.repartition(F.col("der_ts_day")).sortWithinPartitions("der_ts")

    return df, factor

# ----------------------------
# Episode KPIs (derived columns) + cumulative usage
# ----------------------------
def compute_episode_kpis(df: DataFrame) -> DataFrame:
    # Work only where we actually have an episode id (arc was active)
    d = df.where(F.col("der_episode_id").isNotNull())

    # Aggregate per episode
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

    # Cumulative episode metrics (day-wise + global)
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

    return ep

# ----------------------------
# Optional daily totals from row table (helper)
# ----------------------------
def compute_daily_wire_from_rows(df_rows: DataFrame) -> DataFrame:
    """
    Returns one row per day with total wire used that day,
    using sample-level cumulative already present in df_rows.
    """
    return (
        df_rows.groupBy("der_ts_day")
               .agg(F.max("der_wire_len_m_cumsum").alias("der_wire_len_m_total_day"))
               .withColumn("der_wire_len_m_total_day", F.col("der_wire_len_m_total_day").cast(DoubleType()))
               .orderBy("der_ts_day")
    )

# ----------------------------
# Public entry point
# ----------------------------
def run_pipeline3_wire_feed_consumption(
    spark: SparkSession,
    input_path: str,
    output_rows_path: str,
    output_episodes_path: str,
    time_col: str = "time",
    wfs_col: str = "tps500i_wire_speed",
    wfs_unit: str = "auto",     # 'auto' | 'm_per_min' | 'mm_per_s' | 'm_per_s'
    write_mode: str = "overwrite",
    # Backward-compat signature placeholders (ignored, kept to avoid breaking callers)
    weldstate_col: Optional[str] = None,
    arc_on_rule: Optional[str] = None,
    current_threshold: float = 5.0,
) -> Tuple[DataFrame, DataFrame]:

    df_in = read_delta(spark, input_path)

    # Enrich (derived columns only)
    df_rows, factor = enrich_wire_consumption(
        df_in,
        time_col=time_col,
        wfs_col=wfs_col,
        wfs_unit=wfs_unit,
        day_window=True
    )

    # Episodes (derived KPI columns + cumulative fields)
    df_episodes = compute_episode_kpis(df_rows)

    # Write outputs
    write_delta(df_rows, output_rows_path, mode=write_mode)
    write_delta(df_episodes, output_episodes_path, mode=write_mode)

    print(f"[wire] WFS unit factor used: {factor:.6f} (to convert {wfs_col} -> m/s)")
    print(f"[wire] Rows written to: {output_rows_path}")
    print(f"[wire] Episodes written to: {output_episodes_path}")

    return df_rows, df_episodes