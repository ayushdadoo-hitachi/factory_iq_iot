# pipelines/pipeline3_joint_detection.py
"""
Stage 3 — Joint Detection & Object Categorization (NO WPS, NO Spark-ML)
- Reads Stage 2 table
- Detects weld segments (arc-on windows)
- Applies data‑driven joint split rules (step/camera/motion/task)
- Assigns a stable joint identifier: der_joint_id_no_wps
- Auto-categorizes welding objects (A/B/C/... up to N) per d_activetask_f
- Writes Stage 3 table

Notes:
- All derived columns are prefixed with 'der_'.
- Object categorization uses only envelope & process features (NO joint counts, NO Spark-ML).
- All window operations are PARTITIONED to avoid global shuffles.
"""

# =========================
# Imports
# =========================
import logging
from dataclasses import dataclass
from typing import Optional, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from core.io_utils import read_delta, write_delta

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


# =========================
# Config
# =========================
@dataclass(frozen=True)
class JointDetectConfig:
    # Segmentation / fingerprinting
    seam_jump_mult: float = 6.0     # MAD multiplier for seam-jump detection (kept for future use)
    seam_roll_win: int = 5          # seconds window for local diff stats (kept for future use)
    pose_round: float = 0.5         # mm/deg rounding for posture fingerprint
    seam_round: float = 1.0         # mm rounding for seam fingerprint
    bigmove_split: bool = True      # split on der_bigmove
    use_eucseg_split: bool = True   # split on der_euc_seg_id changes
    write_mode: str = "overwrite"   # or "append"

    # Object categorization (A/B/C/... up to N)
    obj_k: int = 3                          # categories to learn
    obj_min_rows_per_task: int = 50         # ignore tiny tasks when learning
    obj_use_columns: Optional[List[str]] = None  # override feature list if needed
    obj_max_iter: int = 6                   # iterations for the pure-DF k-means
    obj_relerr_q: float = 0.05              # quantile relative error for init


# =========================
# Public Entrypoint
# =========================
def run_pipeline3(
    spark: SparkSession,
    input_path: str,            # Stage 2 input (Delta)
    output_path: str,           # Stage 3 output (Delta)
    cfg: Optional[JointDetectConfig] = None,
) -> DataFrame:

    cfg = cfg or JointDetectConfig()

    logger.info("--------------------------------------------------")
    logger.info("Starting Stage 3 – Joint Detection & Object Categorization (NO WPS, NO Spark-ML)")
    logger.info("--------------------------------------------------")

    # -------------------------
    # 📥 Read Stage 2
    # -------------------------
    logger.info("📥 Reading Stage 2 data")
    df = read_delta(spark, input_path)

    # Ensure partitioning keys exist (safe defaults so windows can partition)
    df = _ensure_partition_keys(df)

    # -------------------------
    # ⚙ Derive weld-on flag & split signals
    # -------------------------
    logger.info("🧭 Deriving weld segments and split events")
    df = _add_weld_on_flag(df)                  # prefers der_welding_active
    df = _add_split_signals(df, cfg)            # step/camera/motion/task prep (PARTITIONED)

    # -------------------------
    # ✂️ Build der_weld_seg_id & apply joint splits
    # -------------------------
    logger.info("✂️  Building der_weld_seg_id and applying joint split rules")
    df = _add_weld_seg_id(df, cfg)              # 0->1 transitions (PARTITIONED)
    df = _apply_joint_splits(df, cfg)           # step/seam/bigmove/task/eucseg -> der_joint_seg_id (PARTITIONED)

    # -------------------------
    # 🔑 Assign stable der_joint_id_no_wps
    # -------------------------
    logger.info("🔑 Creating stable der_joint_id_no_wps")
    df = _assign_joint_id_no_wps(df, cfg)       # seam-fingerprint or posture fallback

    # -------------------------
    # 🧠 Auto object categorization (A/B/C/... up to N), NO Spark-ML
    # -------------------------
    logger.info("🔎 Auto-categorizing objects (unsupervised, pure DataFrame)")
    obj_feat = _build_object_features(df, cfg)
    obj_map  = _auto_categorize_object_type_pure(spark, obj_feat, cfg)  # d_activetask_f -> A/B/C/...
    df       = _attach_object_type_and_episode(df, obj_map)

    # -------------------------
    # 📤 Write Stage 3
    # -------------------------
    logger.info("📤 Writing Stage 3")
    write_delta(df, output_path, mode=cfg.write_mode)

    logger.info("🎉 Stage 3 completed")
    return df


# =========================
# Internal helpers
# =========================
def _ensure_partition_keys(df: DataFrame) -> DataFrame:
    """
    Ensure presence of partitioning keys used in windows:
      - batch_id (string)
      - d_activetask_f (string)
    If missing, create stable defaults so windows remain scoped.
    """
    if "batch_id" not in df.columns:
        df = df.withColumn("batch_id", F.lit("batch_00000"))
    if "d_activetask_f" not in df.columns:
        # Try derive from 'activetask' if present; else 'NA'
        if "activetask" in df.columns:
            df = df.withColumn("d_activetask_f", F.col("activetask").cast("string"))
        else:
            df = df.withColumn("d_activetask_f", F.lit("NA"))
    return df


def _order_column(df: DataFrame) -> Tuple[DataFrame, str]:
    """
    Returns (DataFrame, order_col) where order_col is 'time' if present,
    else creates '__row_order__' via monotonically_increasing_id().
    """
    order_col = "time" if "time" in df.columns else None
    if order_col is None:
        df = df.withColumn("__row_order__", F.monotonically_increasing_id())
        order_col = "__row_order__"
    return df, order_col


def _partitioned_order_window(order_col: str) -> Window:
    """
    Standard PARTITIONED order window for Stage-3:
    per (batch_id, d_activetask_f), ordered by order_col.
    """
    return Window.partitionBy("batch_id", "d_activetask_f").orderBy(F.col(order_col))


# -------------------------
# (Segmentation / splits / fingerprinting)
# -------------------------
def _add_weld_on_flag(df: DataFrame) -> DataFrame:
    """
    Create weld-on indicator column `der_weld_on`:
    - Prefer 'der_welding_active' from Stage 2.
    - Fallback to TPS-based arc detection: (tps500i_weldstate == 1) & (tps500i_isrunning == 1).
    Output: integer 0/1 column `der_weld_on`.
    """
    has_derived = 'der_welding_active' in df.columns
    has_tps = ('tps500i_weldstate' in df.columns) and ('tps500i_isrunning' in df.columns)

    if has_derived:
        df = df.withColumn("der_weld_on", F.col("der_welding_active").cast("int"))
    elif has_tps:
        df = df.withColumn(
            "der_weld_on",
            ((F.col("tps500i_weldstate") == F.lit(1)) & (F.col("tps500i_isrunning") == F.lit(1))).cast("int")
        )
    else:
        df = df.withColumn("der_weld_on", F.lit(0).cast("int"))
    return df


def _add_split_signals(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Prepare all derived flags needed for joint split logic.
    Derived columns:
      - der_step_srv_change, der_step_loc_change, der_motstep_change, der_split_step
      - der_split_task
      - der_cam_on, der_seam_d0/1/2, der_seam_diff_max, der_split_seam_dropout, der_split_seam_jump
      - der_split_bigmove, der_split_eucseg
    All split flags are gated by der_weld_on==1.

    All windows are PARTITIONED by (batch_id, d_activetask_f).
    """
    df, order_col = _order_column(df)
    w = _partitioned_order_window(order_col)

    weld_on = F.col("der_weld_on")

    step_srv = F.coalesce(F.col("serverstation0currentstepnumber"), F.lit(-1))
    step_loc = F.coalesce(F.col("currentstepnumber"), F.lit(-1))
    motstep  = F.coalesce(F.col("motstepnumber"), F.lit(-1))
    task_t   = F.coalesce(F.col("task_last_transition"), F.lit(-1))
    eucseg   = F.coalesce(F.col("der_euc_seg_id"), F.lit(-1)) if "der_euc_seg_id" in df.columns else F.lit(-1)

    cam_active_col = F.col("lasercam_isactive") if "lasercam_isactive" in df.columns else F.lit(0)
    df = df.withColumn("der_cam_on", (cam_active_col == F.lit(1)).cast("int"))

    df = (df
        .withColumn("_prev_step_srv", F.lag(step_srv).over(w))
        .withColumn("_prev_step_loc", F.lag(step_loc).over(w))
        .withColumn("_prev_motstep",  F.lag(motstep).over(w))
        .withColumn("der_step_srv_change", (step_srv != F.coalesce(F.col("_prev_step_srv"), F.lit(-1))).cast("int"))
        .withColumn("der_step_loc_change", (step_loc != F.coalesce(F.col("_prev_step_loc"), F.lit(-1))).cast("int"))
        .withColumn("der_motstep_change",  (motstep  != F.coalesce(F.col("_prev_motstep"),  F.lit(-1))).cast("int"))
    )
    df = df.withColumn(
        "der_split_step",
        F.when(
            (weld_on == 1) &
            ((F.col("der_step_srv_change") == 1) |
             (F.col("der_step_loc_change") == 1) |
             (F.col("der_motstep_change")  == 1)),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    df = (df
        .withColumn("_prev_task_t", F.lag(task_t).over(w))
        .withColumn(
            "der_split_task",
            F.when((weld_on == 1) & (task_t != F.coalesce(F.col("_prev_task_t"), F.lit(-1))), F.lit(1)).otherwise(F.lit(0))
        )
        .drop("_prev_task_t")
    )

    # Seam diffs (per camera) when camera is on
    for i, c in enumerate(["lasercamlasttrackingpoint0", "lasercamlasttrackingpoint1", "lasercamlasttrackingpoint2"]):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("double"))
        df = df.withColumn(f"_prev_{c}", F.lag(F.col(c).cast("double")).over(w))
        df = df.withColumn(
            f"der_seam_d{i}",
            F.when(F.col("der_cam_on") == 1, F.abs(F.col(c).cast("double") - F.col(f"_prev_{c}"))).otherwise(F.lit(None).cast("double"))
        )
    df = df.drop(*[f"_prev_lasercamlasttrackingpoint{i}" for i in range(3)])

    df = df.withColumn(
        "der_split_seam_dropout",
        F.when(
            (weld_on == 1) & (F.col("der_cam_on") == 1) &
            (F.col("lasercamlasttrackingpoint0").isNull() |
             F.col("lasercamlasttrackingpoint1").isNull() |
             F.col("lasercamlasttrackingpoint2").isNull()),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    df = df.withColumn(
        "der_seam_diff_max",
        F.greatest(F.col("der_seam_d0"), F.col("der_seam_d1"), F.col("der_seam_d2"))
    )

    # Note: using a simple threshold here; advanced jump detection can be added later
    seam_jump_threshold = F.lit(max(cfg.seam_round, 0.001) * 1.5)
    df = df.withColumn(
        "der_split_seam_jump",
        F.when((weld_on == 1) & (F.col("der_cam_on") == 1) & (F.col("der_seam_diff_max") > seam_jump_threshold), F.lit(1)).otherwise(F.lit(0))
    )

    if "der_bigmove" not in df.columns:
        df = df.withColumn("der_bigmove", F.lit(0).cast("int"))
    df = df.withColumn(
        "der_split_bigmove",
        F.when((weld_on == 1) & (F.lit(cfg.bigmove_split)) & (F.col("der_bigmove") == 1), F.lit(1)).otherwise(F.lit(0))
    )

    df = (df
        .withColumn("_prev_eucseg", F.lag(eucseg).over(w))
        .withColumn(
            "der_split_eucseg",
            F.when((weld_on == 1) & (F.lit(cfg.use_eucseg_split)) & (eucseg != F.coalesce(F.col("_prev_eucseg"), F.lit(-1))), F.lit(1)).otherwise(F.lit(0))
        )
        .drop("_prev_eucseg")
    )

    if "__row_order__" in df.columns:
        df = df.drop("__row_order__")
    return df


def _add_weld_seg_id(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Build `der_weld_seg_id` based on 0->1 transitions of `der_weld_on`.
    Uses 'time' for ordering if present; otherwise creates a row-order id.
    PARTITIONED by (batch_id, d_activetask_f).
    """
    df, order_col = _order_column(df)
    w = _partitioned_order_window(order_col)

    df = (df
        .withColumn("_prev_on", F.lag(F.col("der_weld_on")).over(w))
        .withColumn("_start_flag", F.when((F.col("der_weld_on") == 1) & (F.coalesce(F.col("_prev_on"), F.lit(0)) == 0), 1).otherwise(0))
        .withColumn("der_weld_seg_id",
                    F.when(F.col("der_weld_on") == 1, F.sum(F.col("_start_flag")).over(w))
                     .otherwise(F.lit(None)))
        .drop("_prev_on", "_start_flag")
    )

    if "__row_order__" in df.columns:
        df = df.drop("__row_order__")
    return df


def _apply_joint_splits(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Convert split flags into final joint-aware segment id `der_joint_seg_id`
    by incrementing a local counter within each der_weld_seg_id.

    PARTITIONING:
      - Any lagging/ordering before segmentation uses (batch_id, d_activetask_f).
      - The final segmentation cumsum is already per der_weld_seg_id.
    """
    df, order_col = _order_column(df)

    # Ensure split flags exist
    split_cols = [
        "der_split_step","der_split_task","der_split_seam_jump",
        "der_split_seam_dropout","der_split_bigmove","der_split_eucseg",
    ]
    for c in split_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(0).cast("int"))

    any_split = None
    for c in split_cols:
        term = (F.col(c) == 1)
        any_split = term if any_split is None else (any_split | term)
    any_split = F.when(F.col("der_weld_on") == 1, any_split).otherwise(F.lit(False))

    # Final segmentation is scoped within each weld segment and ordered by time (or row id)
    part = Window.partitionBy(F.col("der_weld_seg_id")).orderBy(F.col(order_col))
    df = (df
        .withColumn("_split_start", F.when(any_split, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("_split_cumsum", F.sum(F.col("_split_start")).over(part))
        .withColumn(
            "der_joint_seg_id",
            F.when(F.col("der_weld_on") == 1,
                   F.concat_ws("-", F.col("der_weld_seg_id").cast("string"), (F.col("_split_cumsum") + F.lit(1)).cast("string"))
            ).otherwise(F.lit(None))
        )
        .drop("_split_start","_split_cumsum")
    )

    if "__row_order__" in df.columns:
        df = df.drop("__row_order__")
    return df


def _assign_joint_id_no_wps(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Build a stable joint identifier per der_joint_seg_id without WPS:
      - If camera active in segment: use median(lasttrackingpoint0/1/2) rounded
      - Else: use posture fallback (worldtcp* or robax1-3) rounded
    Emits:
      - der_cam_on_ratio, der_seam_med_x/y/z, der_pose_tx/ty/tz, rounded variants
      - der_fingerprint_type ('seam' or 'pose')
      - der_joint_fingerprint ('S:x,y,z' or 'W:tx,ty,tz' or 'R:a1,a2,a3')
      - der_joint_id_no_wps (sha2 hash)
    """
    base = df.filter(F.col("der_joint_seg_id").isNotNull())

    def p50(col_name: str):
        return F.percentile_approx(F.col(col_name).cast("double"), 0.5, 10000)

    agg = (base.groupBy("der_joint_seg_id")
             .agg(
                 F.avg(F.col("der_cam_on").cast("double")).alias("der_cam_on_ratio"),
                 p50("lasercamlasttrackingpoint0").alias("der_seam_med_x"),
                 p50("lasercamlasttrackingpoint1").alias("der_seam_med_y"),
                 p50("lasercamlasttrackingpoint2").alias("der_seam_med_z"),
                 F.first(F.col("worldtcp0").cast("double"), ignorenulls=True).alias("der_pose_tx"),
                 F.first(F.col("worldtcp1").cast("double"), ignorenulls=True).alias("der_pose_ty"),
                 F.first(F.col("worldtcp2").cast("double"), ignorenulls=True).alias("der_pose_tz"),
                 F.first(F.col("robax1_position").cast("double"), ignorenulls=True).alias("der_pose_ax1"),
                 F.first(F.col("robax2_position").cast("double"), ignorenulls=True).alias("der_pose_ax2"),
                 F.first(F.col("robax3_position").cast("double"), ignorenulls=True).alias("der_pose_ax3"),
                 F.first(F.coalesce(F.col("serverstation0currentstepnumber"), F.col("currentstepnumber")).cast("string"),
                         ignorenulls=True).alias("der_step_ctx"),
                 F.first(F.col("d_activetask_f").cast("string"), ignorenulls=True).alias("der_act_ctx"),
             ))

    def round_to(col, res: float):
        res = max(res, 1e-9)
        return F.round(col / F.lit(res)) * F.lit(res)

    agg = (agg
        .withColumn("der_seam_rx", round_to(F.col("der_seam_med_x"), float(cfg.seam_round)))
        .withColumn("der_seam_ry", round_to(F.col("der_seam_med_y"), float(cfg.seam_round)))
        .withColumn("der_seam_rz", round_to(F.col("der_seam_med_z"), float(cfg.seam_round)))
        .withColumn("der_pose_rtx", round_to(F.col("der_pose_tx"), float(cfg.pose_round)))
        .withColumn("der_pose_rty", round_to(F.col("der_pose_ty"), float(cfg.pose_round)))
        .withColumn("der_pose_rtz", round_to(F.col("der_pose_tz"), float(cfg.pose_round)))
    )

    use_seam = (
        (F.col("der_cam_on_ratio") > F.lit(0.5)) &
        F.col("der_seam_med_x").isNotNull() &
        F.col("der_seam_med_y").isNotNull() &
        F.col("der_seam_med_z").isNotNull()
    )

    seam_fp = F.concat_ws(",", F.lit("S"),
        F.col("der_seam_rx").cast("string"),
        F.col("der_seam_ry").cast("string"),
        F.col("der_seam_rz").cast("string"))

    world_fp = F.concat_ws(",", F.lit("W"),
        F.col("der_pose_rtx").cast("string"),
        F.col("der_pose_rty").cast("string"),
        F.col("der_pose_rtz").cast("string"))

    axes_fp = F.concat_ws(",", F.lit("R"),
        round_to(F.col("der_pose_ax1"), float(cfg.pose_round)).cast("string"),
        round_to(F.col("der_pose_ax2"), float(cfg.pose_round)).cast("string"),
        round_to(F.col("der_pose_ax3"), float(cfg.pose_round)).cast("string"))

    pose_fp = F.when(
        F.col("der_pose_rtx").isNotNull() & F.col("der_pose_rty").isNotNull() & F.col("der_pose_rtz").isNotNull(),
        world_fp
    ).otherwise(axes_fp)

    agg = (agg
        .withColumn("der_fingerprint_type", F.when(use_seam, F.lit("seam")).otherwise(F.lit("pose")))
        .withColumn("der_joint_fingerprint", F.when(use_seam, seam_fp).otherwise(pose_fp))
    )

    base_id_str = F.concat_ws("|",
        F.coalesce(F.col("der_act_ctx").cast("string"), F.lit("NA")),
        F.coalesce(F.col("der_step_ctx").cast("string"), F.lit("NA")),
        F.coalesce(F.col("der_joint_fingerprint"), F.lit("NA"))
    )
    agg = agg.withColumn("der_joint_id_no_wps", F.sha2(base_id_str, 256))

    df = df.join(agg, on="der_joint_seg_id", how="left")
    return df


# =========================
# Auto object categorization — Pure DataFrame k-means
# =========================
def _build_object_features(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Build one feature row per d_activetask_f using ONLY arc-on rows (der_weld_on==1),
    WITHOUT using any joint-count signals.

    Output columns:
      d_activetask_f, der_of_arc_rows, der_of_episode_span_secs,
      der_of_span_x/y/z, der_of_mid_x/y/z,
      der_of_i_avg, der_of_v_avg, der_of_wfs_avg, der_of_speed_avg
    """
    base = df.where(F.col("der_weld_on") == 1)
    base = base.withColumn("d_activetask_f_str", F.coalesce(F.col("d_activetask_f").cast("string"), F.lit("NA")))
    tcol = F.to_timestamp("time") if "time" in df.columns else None

    agg_exprs = [F.count("*").alias("der_of_arc_rows")]
    if tcol is not None:
        agg_exprs.extend([F.min(tcol).alias("t_min"), F.max(tcol).alias("t_max")])
    else:
        agg_exprs.extend([F.lit(None).alias("t_min"), F.lit(None).alias("t_max")])

    agg = (base
        .groupBy("d_activetask_f_str")
        .agg(
            *agg_exprs,
            F.min(F.col("worldtcp0")).alias("x_min"),
            F.max(F.col("worldtcp0")).alias("x_max"),
            F.min(F.col("worldtcp1")).alias("y_min"),
            F.max(F.col("worldtcp1")).alias("y_max"),
            F.min(F.col("worldtcp2")).alias("z_min"),
            F.max(F.col("worldtcp2")).alias("z_max"),
            F.avg("tps500i_current").alias("der_of_i_avg"),
            F.avg("tps500i_voltage").alias("der_of_v_avg"),
            F.avg("tps500i_wire_speed").alias("der_of_wfs_avg"),
            F.avg("speed").alias("der_of_speed_avg"),
        )
    )

    agg = (agg
        .withColumn("der_of_episode_span_secs",
                    F.when((F.col("t_min").isNotNull()) & (F.col("t_max").isNotNull()),
                           F.col("t_max").cast("long") - F.col("t_min").cast("long")).otherwise(F.lit(0)))
        .withColumn("der_of_span_x", (F.col("x_max") - F.col("x_min")))
        .withColumn("der_of_span_y", (F.col("y_max") - F.col("y_min")))
        .withColumn("der_of_span_z", (F.col("z_max") - F.col("z_min")))
        .withColumn("der_of_mid_x", (F.col("x_max") + F.col("x_min")) / 2.0)
        .withColumn("der_of_mid_y", (F.col("y_max") + F.col("y_min")) / 2.0)
        .withColumn("der_of_mid_z", (F.col("z_max") + F.col("z_min")) / 2.0)
    )

    # Replace nulls with zeros for robustness (few tasks)
    num_cols = [
        "der_of_arc_rows","der_of_episode_span_secs",
        "der_of_span_x","der_of_span_y","der_of_span_z",
        "der_of_mid_x","der_of_mid_y","der_of_mid_z",
        "der_of_i_avg","der_of_v_avg","der_of_wfs_avg","der_of_speed_avg"
    ]
    for c in num_cols:
        agg = agg.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))

    agg = agg.where(F.col("der_of_arc_rows") >= F.lit(int(cfg.obj_min_rows_per_task)))

    out = (agg
        .select(
            F.col("d_activetask_f_str").alias("d_activetask_f"),
            *num_cols
        )
    )
    return out


def _rank_to_alpha(n: int) -> str:
    """
    Convert 1-based rank to alphabetic label:
      1->A, 2->B, ... 26->Z, 27->AA, 28->AB, ...
    """
    n = int(n)
    if n <= 0:
        return "UNK"
    label = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        label = chr(65 + rem) + label
    return label


def _auto_categorize_object_type_pure(
    spark: SparkSession,
    objdf: DataFrame,
    cfg: JointDetectConfig
) -> DataFrame:
    """
    Unsupervised clustering (pure DataFrame k-means) of object episodes (per d_activetask_f),
    using only envelope & process features. NO Spark-ML constructors.

    Steps:
      1) Standardize features to z-scores.
      2) Initialize k centroids with quantile bins on env_diag_z.
      3) Iterate (assign -> update) obj_max_iter times.
      4) Rank clusters by envelope size and map to 'A','B','C',...,'AA', ...

    Returns: [d_activetask_f, der_object_type]
    """
    task_count = objdf.count()
    if task_count == 0:
        return spark.createDataFrame([], schema="d_activetask_f string, der_object_type string")

    k_eff = max(1, min(int(cfg.obj_k), task_count))

    # Select feature columns
    use_cols = cfg.obj_use_columns or [
        "der_of_span_x","der_of_span_y","der_of_span_z",
        "der_of_mid_x","der_of_mid_y","der_of_mid_z",
        "der_of_i_avg","der_of_v_avg","der_of_wfs_avg","der_of_speed_avg",
        "der_of_episode_span_secs"
    ]

    # ---- 1) Standardize (z-score): (x - mean) / std
    stats_exprs = []
    for c in use_cols:
        stats_exprs.append(F.avg(F.col(c)).alias(f"m_{c}"))
        stats_exprs.append(F.stddev_pop(F.col(c)).alias(f"s_{c}"))
    stats = objdf.agg(*stats_exprs)

    # Cross join stats to compute z_cols once
    objz = objdf
    for c in use_cols:
        objz = objz.join(stats)
        break  # join once

    for c in use_cols:
        m = F.col(f"m_{c}")
        s = F.when(F.col(f"s_{c}") > 0.0, F.col(f"s_{c}")).otherwise(F.lit(1.0))
        objz = objz.withColumn(f"z_{c}", (F.col(c) - m) / s)

    # Envelope diagonal in z-space (for initialization & ranking)
    objz = objz.withColumn(
        "env_diag_z",
        F.sqrt(F.col("z_der_of_span_x")**2 + F.col("z_der_of_span_y")**2 + F.col("z_der_of_span_z")**2)
    )

    z_cols = [f"z_{c}" for c in use_cols]

    # ---- 2) Initialize centroids via quantile bins on env_diag_z
    probs = [i/float(k_eff) for i in range(1, k_eff)]
    cuts = objz.stat.approxQuantile("env_diag_z", probs, cfg.obj_relerr_q) if k_eff > 1 else []
    bin_col = F.lit(0)
    for _, cut in enumerate(cuts):
        bin_col = bin_col + F.when(F.col("env_diag_z") > F.lit(float(cut)), 1).otherwise(0)
    objz = objz.withColumn("cluster_id", bin_col.cast("int"))

    # Initial centroids: mean of z_cols in each bin
    centroids = _compute_centroids(objz, z_cols)

    # Adjust if fewer bins than k_eff
    if centroids.count() < k_eff:
        k_eff = centroids.count()

    # ---- 3) Iterate k-means (assign -> update)
    for _ in range(int(cfg.obj_max_iter)):
        assigned = _assign_to_centroids(objz, centroids, z_cols)
        new_centroids = _compute_centroids(assigned, z_cols)
        centroids = new_centroids
        objz = assigned

    # ---- 4) Rank clusters by envelope size in original scale (stable labels)
    env_orig = objdf.withColumn(
        "env_diag",
        F.sqrt(F.col("der_of_span_x")**2 + F.col("der_of_span_y")**2 + F.col("der_of_span_z")**2)
    )
    final_map = objz.select("d_activetask_f","cluster_id").distinct()
    env_with_cluster = (env_orig.join(final_map, on="d_activetask_f", how="left"))

    ranks = (env_with_cluster.groupBy("cluster_id")
                .agg(F.avg("env_diag").alias("avg_env_diag"))
                .orderBy("avg_env_diag")
                .withColumn("rank", F.row_number().over(Window.orderBy("avg_env_diag"))))

    to_alpha = F.udf(lambda x: _rank_to_alpha(int(x)), "string")
    ranks = ranks.withColumn("der_object_type", to_alpha(F.col("rank")))

    mapping = (final_map.join(ranks.select("cluster_id","der_object_type"), on="cluster_id", how="left")
                        .select("d_activetask_f","der_object_type"))
    return mapping


def _compute_centroids(df_with_cluster: DataFrame, z_cols: List[str]) -> DataFrame:
    """
    Compute centroid means for each cluster_id over the z_cols.
    Returns a DF with columns: cluster_id, c_<z_col> per feature.
    """
    exprs = [F.avg(F.col(c)).alias(f"c_{c}") for c in z_cols]
    return df_with_cluster.groupBy("cluster_id").agg(*exprs)


def _assign_to_centroids(objz: DataFrame, centroids: DataFrame, z_cols: List[str]) -> DataFrame:
    """
    Cross-join tasks with centroids, compute squared Euclidean distance on z_cols,
    pick the nearest centroid_id per task. Returns objz with updated cluster_id.
    """
    joined = objz.select("d_activetask_f", *z_cols, "env_diag_z").distinct().crossJoin(centroids)

    dist2 = None
    for c in z_cols:
        term = (F.col(c) - F.col(f"c_{c}"))**2
        dist2 = term if dist2 is None else (dist2 + term)
    joined = joined.withColumn("dist2", dist2)

    w = Window.partitionBy("d_activetask_f").orderBy(F.col("dist2").asc(), F.col("cluster_id").asc())
    nearest = joined.withColumn("_r", F.row_number().over(w)).where(F.col("_r") == 1) \
                    .select("d_activetask_f","cluster_id")

    out = (objz.drop("cluster_id")
               .join(nearest, on="d_activetask_f", how="left"))
    return out


def _attach_object_type_and_episode(
    df: DataFrame,
    obj_type_map: DataFrame
) -> DataFrame:
    """
    Join the auto-learned object type per d_activetask_f to the row-level Stage-3 DF,
    and create `der_object_episode_id` = "<type>-<d_activetask_f>" for arc-on rows.
    Unknowns become 'UNK'.
    """
    df = df.withColumn("d_activetask_f_str", F.coalesce(F.col("d_activetask_f").cast("string"), F.lit("NA")))
    joined = (df.join(obj_type_map, df["d_activetask_f_str"] == obj_type_map["d_activetask_f"], "left")
                .drop(obj_type_map["d_activetask_f"]))

    joined = joined.withColumn("der_object_type", F.coalesce(F.col("der_object_type"), F.lit("UNK")))
    joined = joined.withColumn(
        "der_object_episode_id",
        F.when(F.col("der_weld_on")==1, F.concat_ws("-", F.col("der_object_type"), F.col("d_activetask_f_str")))
         .otherwise(F.lit(None))
    ).drop("d_activetask_f_str")

    return joined


# =========================
# Optional cleanup (no-op by default)
# =========================
def _drop_intermediate_columns(df: DataFrame, keep_debug_cols: bool = False) -> DataFrame:
    if keep_debug_cols:
        return df
    drop_cols = [
        "der_seam_d0", "der_seam_d1", "der_seam_d2", "der_seam_diff_max",
        "der_step_srv_change", "der_step_loc_change", "der_motstep_change",
    ]
    existing = [c for c in drop_cols if c in df.columns]
    return df.drop(*existing)