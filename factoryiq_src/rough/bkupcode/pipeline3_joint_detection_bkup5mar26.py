# pipelines/pipeline3_joint_detection.py
"""
Stage 3 — Joint Detection Pipeline (NO WPS)
- Reads Stage 2 table
- Detects weld segments (arc-on windows)
- Applies data‑driven joint split rules (step/camera/motion/task)
- Assigns a stable joint identifier: joint_id_no_wps
- Writes Stage 3 table
"""

# =========================
# Imports (match Pipeline 2 style)
# =========================
import importlib
import logging
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql import SparkSession, DataFrame

# Project I/O utilities (same as Pipeline 2)
from core.io_utils import read_delta, write_delta

# If you later move helpers into modules, import & reload them here (kept to match pattern)
# import joint_detection.split_rules as jds
# import joint_detection.fingerprints as jdfp
# importlib.reload(jds)
# importlib.reload(jdfp)

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


# =========================
# Config (similar to Pipeline 2's knobs)
# =========================
@dataclass(frozen=True)
class JointDetectConfig:
    seam_jump_mult: float = 6.0     # MAD multiplier for seam-jump detection
    seam_roll_win: int = 5          # seconds window for local diff stats
    pose_round: float = 0.5         # mm/deg rounding for posture fingerprint
    seam_round: float = 1.0         # mm rounding for seam fingerprint
    bigmove_split: bool = True      # split on der_bigmove
    use_eucseg_split: bool = True   # split on der_euc_seg_id changes
    write_mode: str = "overwrite"   # or "append"


# =========================
# Public Entrypoint (keep the same signature style)
# =========================
def run_pipeline3(
    spark: SparkSession,
    input_path: str,            # Stage 2 input (Delta)
    output_path: str,           # Stage 3 output (Delta)
    cfg: Optional[JointDetectConfig] = None,
) -> DataFrame:

    cfg = cfg or JointDetectConfig()

    logger.info("--------------------------------------------------")
    logger.info("Starting Stage 3 – Joint Detection (NO WPS)")
    logger.info("--------------------------------------------------")

    # -------------------------
    # 📥 Read Stage 2
    # -------------------------
    logger.info("📥 Reading Stage 2 data")
    df = read_delta(spark, input_path)

    # -------------------------
    # ⚙ Derive weld-on flag & split signals
    # -------------------------
    logger.info("🧭 Deriving weld segments and split events")
    df = _add_weld_on_flag(df)                  # prefers der_welding_active
    df = _add_split_signals(df, cfg)            # step/camera/motion/task prep

    # -------------------------
    # ✂️ Build weld_seg_id & apply joint splits
    # -------------------------
    logger.info("✂️  Building weld_seg_id and applying joint split rules")
    df = _add_weld_seg_id(df, cfg)              # 0->1 transitions
    df = _apply_joint_splits(df, cfg)           # step/seam/bigmove/task/eucseg

    # -------------------------
    # 🔑 Assign stable joint_id_no_wps
    # -------------------------
    logger.info("🔑 Creating stable joint_id_no_wps")
    df = _assign_joint_id_no_wps(df, cfg)       # seam-fingerprint or posture fallback

    # -------------------------
    # 🧹 (Optional) drop intermediates later
    # df = _drop_intermediate_columns(df)
    # -------------------------

    # -------------------------
    # 📤 Write Stage 3
    # -------------------------
    logger.info("📤 Writing Stage 3")
    write_delta(df, output_path)  

    logger.info("🎉 Stage 3 completed")
    return df

# =========================
# Internal helpers (stubs; fill incrementally)
# =========================

def _add_weld_on_flag(df: DataFrame) -> DataFrame:
    """
    Create a stable weld-on indicator column `der_weld_on`:
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
            ( (F.col("tps500i_weldstate") == F.lit(1)) & (F.col("tps500i_isrunning") == F.lit(1)) ).cast("int")
        )
    else:
        # Safe default – keeps pipeline running even if neither source is present
        df = df.withColumn("der_weld_on", F.lit(0).cast("int"))

    return df


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def _add_split_signals(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Prepare all derived flags needed for joint split logic.
    Produces ONLY derived columns (no segmentation applied here):
      - Step changes (server/local/motion)  -> der_step_* + der_split_step
      - Task transition change              -> der_split_task
      - Camera activity & seam deltas       -> der_cam_on, der_seam_d0/1/2, der_seam_diff_max,
                                               der_split_seam_dropout, der_split_seam_jump
      - Motion-based boundaries             -> der_split_bigmove, der_split_eucseg

    Notes:
      * All split flags are gated by der_weld_on == 1.
      * We use 'time' to order events; if missing, we create a row-order id.
      * Seam-jump threshold is a simple, configurable heuristic using cfg.seam_round.
    """

    # ---------- Ensure ordering ----------
    order_col = "time" if "time" in df.columns else None
    if order_col is None:
        df = df.withColumn("__row_order__", F.monotonically_increasing_id())
        order_col = "__row_order__"
    w = Window.orderBy(F.col(order_col))

    # ---------- Primitives / Safe defaults ----------
    # weld_on must already exist (created by _add_weld_on_flag)
    weld_on = F.col("der_weld_on")

    # Base columns (existence checks with safe defaults)
    step_srv = F.coalesce(F.col("serverstation0currentstepnumber"), F.lit(-1))
    step_loc = F.coalesce(F.col("currentstepnumber"), F.lit(-1))
    motstep  = F.coalesce(F.col("motstepnumber"), F.lit(-1))
    task_t   = F.coalesce(F.col("task_last_transition"), F.lit(-1))
    eucseg   = F.coalesce(F.col("der_euc_seg_id"), F.lit(-1)) if "der_euc_seg_id" in df.columns else F.lit(-1)

    cam_active_col = F.col("lasercam_isactive") if "lasercam_isactive" in df.columns else F.lit(0)
    df = df.withColumn("der_cam_on", (cam_active_col == F.lit(1)).cast("int"))

    # ---------- Step changes ----------
    df = (
        df
        .withColumn("_prev_step_srv", F.lag(step_srv).over(w))
        .withColumn("_prev_step_loc", F.lag(step_loc).over(w))
        .withColumn("_prev_motstep",  F.lag(motstep).over(w))
        .withColumn(
            "der_step_srv_change",
            (step_srv != F.coalesce(F.col("_prev_step_srv"), F.lit(-1))).cast("int")
        )
        .withColumn(
            "der_step_loc_change",
            (step_loc != F.coalesce(F.col("_prev_step_loc"), F.lit(-1))).cast("int")
        )
        .withColumn(
            "der_motstep_change",
            (motstep  != F.coalesce(F.col("_prev_motstep"),  F.lit(-1))).cast("int")
        )
    )

    df = df.withColumn(
        "der_split_step",
        F.when(
            (weld_on == 1) & (
                (F.col("der_step_srv_change") == 1) |
                (F.col("der_step_loc_change") == 1) |
                (F.col("der_motstep_change")  == 1)
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # ---------- Task transition change ----------
    df = (
        df
        .withColumn("_prev_task_t", F.lag(task_t).over(w))
        .withColumn(
            "der_split_task",
            F.when(
                (weld_on == 1) & (task_t != F.coalesce(F.col("_prev_task_t"), F.lit(-1))),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop("_prev_task_t")
    )

    # ---------- Seam tracking deltas & dropouts ----------
    # Trackers: use safe cast to double; deltas only when camera is on
    for i, c in enumerate(["lasercamlasttrackingpoint0", "lasercamlasttrackingpoint1", "lasercamlasttrackingpoint2"]):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("double"))
        df = df.withColumn(f"_prev_{c}", F.lag(F.col(c).cast("double")).over(w))
        df = df.withColumn(
            f"der_seam_d{i}",
            F.when(F.col("der_cam_on") == 1, F.abs(F.col(c).cast("double") - F.col(f"_prev_{c}"))).otherwise(F.lit(None).cast("double"))
        )

    df = df.drop(*[f"_prev_lasercamlasttrackingpoint{i}" for i in range(3)])

    # Seam dropout: camera is on but any axis is null
    df = df.withColumn(
        "der_split_seam_dropout",
        F.when(
            (weld_on == 1) &
            (F.col("der_cam_on") == 1) &
            (
                F.col("lasercamlasttrackingpoint0").isNull() |
                F.col("lasercamlasttrackingpoint1").isNull() |
                F.col("lasercamlasttrackingpoint2").isNull()
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # Max per-row seam diff (for tuning/inspection and jump heuristic)
    df = df.withColumn(
        "der_seam_diff_max",
        F.greatest(F.col("der_seam_d0"), F.col("der_seam_d1"), F.col("der_seam_d2"))
    )

    # Simple jump heuristic (configurable): if diff exceeds (1.5 * seam_round) while cam==on
    seam_jump_threshold = F.lit(max(cfg.seam_round, 0.001) * 1.5)
    df = df.withColumn(
        "der_split_seam_jump",
        F.when(
            (weld_on == 1) & (F.col("der_cam_on") == 1) &
            (F.col("der_seam_diff_max") > seam_jump_threshold),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    # ---------- Motion-based boundaries ----------
    # Big move (already derived in Stage 2 as der_bigmove)
    if "der_bigmove" not in df.columns:
        df = df.withColumn("der_bigmove", F.lit(0).cast("int"))

    df = df.withColumn(
        "der_split_bigmove",
        F.when((weld_on == 1) & (F.lit(cfg.bigmove_split)) & (F.col("der_bigmove") == 1), F.lit(1)).otherwise(F.lit(0))
    )

    # EUC segment change (optional)
    df = (
        df
        .withColumn("_prev_eucseg", F.lag(eucseg).over(w))
        .withColumn(
            "der_split_eucseg",
            F.when(
                (weld_on == 1) & (F.lit(cfg.use_eucseg_split)) &
                (eucseg != F.coalesce(F.col("_prev_eucseg"), F.lit(-1))),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop("_prev_eucseg")
    )

    # ---------- Cleanup helper column ----------
    if "__row_order__" in df.columns:
        df = df.drop("__row_order__")

    return df



def _add_weld_seg_id(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Build `der_weld_seg_id` based on 0->1 transitions of `der_weld_on`.
    - Uses `time` for ordering if present, otherwise a generated row id.
    - Segment id increases only within arc-on regions; off-rows remain NULL.
    """
    # Choose ordering column
    order_col = "time" if "time" in df.columns else None
    if order_col is None:
        df = df.withColumn("__row_order__", F.monotonically_increasing_id())
        order_col = "__row_order__"

    w = Window.orderBy(F.col(order_col))

    # Rising edges: prev=0 -> curr=1
    df = (
        df
        .withColumn("_prev_on", F.lag(F.col("der_weld_on")).over(w))
        .withColumn(
            "_start_flag",
            F.when((F.col("der_weld_on") == 1) & (F.coalesce(F.col("_prev_on"), F.lit(0)) == 0), 1).otherwise(0)
        )
        .withColumn(
            "der_weld_seg_id",
            F.when(F.col("der_weld_on") == 1, F.sum(F.col("_start_flag")).over(w)).otherwise(F.lit(None))
        )
        .drop("_prev_on", "_start_flag")
    )

    if "__row_order__" in df.columns:
        df = df.drop("__row_order__")

    return df



from pyspark.sql import functions as F
from pyspark.sql.window import Window

def _apply_joint_splits(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Convert derived split flags into a final joint-aware segment id.
    Input requirements (must already exist):
      - der_weld_on (0/1)
      - der_weld_seg_id (base segment id assigned on 0->1 arcs)
      - Split flags (0/1): der_split_step, der_split_task, der_split_seam_jump,
                           der_split_seam_dropout, der_split_bigmove, der_split_eucseg
    Output:
      - der_joint_seg_id : monotonically increasing id within each der_weld_seg_id
                           split further whenever any split flag fires.
    Notes:
      - Ordering by `time` if present; otherwise we synthesize row order.
      - Rows where der_weld_on==0 keep der_joint_seg_id as NULL.
    """

    # ---------- Ensure ordering ----------
    order_col = "time" if "time" in df.columns else None
    if order_col is None:
        df = df.withColumn("__row_order__", F.monotonically_increasing_id())
        order_col = "__row_order__"

    # ---------- Gather/gate split triggers ----------
    # Any split condition that should break the joint inside the arc-on region.
    split_cols = [
        "der_split_step",
        "der_split_task",
        "der_split_seam_jump",
        "der_split_seam_dropout",
        "der_split_bigmove",
        "der_split_eucseg",
    ]
    for c in split_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(0).cast("int"))

    # Gate split triggers by weld_on (defensive)
    any_split = None
    for c in split_cols:
        term = (F.col(c) == 1)
        any_split = term if any_split is None else (any_split | term)

    any_split = F.when(F.col("der_weld_on") == 1, any_split).otherwise(F.lit(False))

    # ---------- Running split counter within each base weld segment ----------
    # We want to start from 1 inside each der_weld_seg_id and add +1 on every split trigger.
    # Partition within each base weld segment; order by time.
    part = Window.partitionBy(F.col("der_weld_seg_id")).orderBy(F.col(order_col))

    df = (
        df
        # mark a 1 when a split fires, else 0 (only relevant where der_weld_on==1)
        .withColumn("_split_start", F.when(any_split, F.lit(1)).otherwise(F.lit(0)))
        # cumulative sum within the base weld segment
        .withColumn("_split_cumsum", F.sum(F.col("_split_start")).over(part))
        # derive joint-aware id: base weld segment id + local increment
        # Start from 1 inside each base segment; add cumulative sum to make sub-ids
        .withColumn(
            "der_joint_seg_id",
            F.when(F.col("der_weld_on") == 1,
                   # Example: combine as "<base>-<sub>"
                   F.concat_ws(
                       "-",
                       F.col("der_weld_seg_id").cast("string"),
                       (F.col("_split_cumsum") + F.lit(1)).cast("string")
                   )
            ).otherwise(F.lit(None))
        )
        .drop("_split_start", "_split_cumsum")
    )

    # ---------- Cleanup helper column ----------
    if "__row_order__" in df.columns:
        df = df.drop("__row_order__")

    return df


from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def _assign_joint_id_no_wps(df: DataFrame, cfg: JointDetectConfig) -> DataFrame:
    """
    Create a stable joint identifier for each joint-aware segment (NO WPS):
      - If camera is active for the segment, use a seam fingerprint:
            median(lasercamlasttrackingpoint0/1/2), rounded to cfg.seam_round
      - Else, use a posture fingerprint (fallback):
            prefer worldtcp0/1/2 (first non-null within segment), rounded to cfg.pose_round
            if not available, fallback to robax1/2/3 (first non-null), rounded likewise

    Emits (all derived):
      - der_cam_on_ratio               : avg camera-on ratio per der_joint_seg_id
      - der_seam_med_x/y/z             : median seam tracking points
      - der_seam_rx/ry/rz              : rounded seam medians (cfg.seam_round)
      - der_pose_tx/ty/tz              : first non-null worldtcp0/1/2 (fallback posture)
      - der_pose_rtx/rty/rtz           : rounded posture values (cfg.pose_round)
      - der_fingerprint_type           : 'seam' or 'pose'
      - der_joint_fingerprint          : 'S:x,y,z' or 'W:x,y,z' or 'R:a,b,c'
      - der_joint_id_no_wps            : sha256 hash of (activetask | step | fingerprint)

    NOTE:
      - Works only where der_joint_seg_id is present (arc-on & split applied).
      - Keeps original rows, joining segment-level fingerprints back to df.
    """

    # Only compute for rows that belong to a joint-aware segment
    base = df.filter(F.col("der_joint_seg_id").isNotNull())

    # Helper for median via approx percentile
    def p50(col_name: str):
        return F.percentile_approx(F.col(col_name).cast("double"), 0.5, 10000)

    # Compute camera-on ratio and seam medians per joint segment
    agg = (
        base.groupBy("der_joint_seg_id")
            .agg(
                F.avg(F.col("der_cam_on").cast("double")).alias("der_cam_on_ratio"),
                p50("lasercamlasttrackingpoint0").alias("der_seam_med_x"),
                p50("lasercamlasttrackingpoint1").alias("der_seam_med_y"),
                p50("lasercamlasttrackingpoint2").alias("der_seam_med_z"),
                # posture candidates (first non-null within segment)
                F.first(F.col("worldtcp0").cast("double"), ignorenulls=True).alias("der_pose_tx"),
                F.first(F.col("worldtcp1").cast("double"), ignorenulls=True).alias("der_pose_ty"),
                F.first(F.col("worldtcp2").cast("double"), ignorenulls=True).alias("der_pose_tz"),
                F.first(F.col("robax1_position").cast("double"), ignorenulls=True).alias("der_pose_ax1"),
                F.first(F.col("robax2_position").cast("double"), ignorenulls=True).alias("der_pose_ax2"),
                F.first(F.col("robax3_position").cast("double"), ignorenulls=True).alias("der_pose_ax3"),
                # optional context to enrich ID (activetask/step numbers)
                F.first(F.col("activetask"), ignorenulls=True).alias("der_act_ctx"),
                F.first(F.coalesce(F.col("serverstation0currentstepnumber"), F.col("currentstepnumber")).cast("string"), ignorenulls=True).alias("der_step_ctx"),
            )
    )

    # Rounding helpers: round to a given resolution (e.g., 1.0 mm), not decimal places
    def round_to(col, res: float):
        res = max(res, 1e-9)
        return F.round(col / F.lit(res)) * F.lit(res)

    # Seam fingerprint rounding
    agg = (
        agg
        .withColumn("der_seam_rx", round_to(F.col("der_seam_med_x"), float(cfg.seam_round)))
        .withColumn("der_seam_ry", round_to(F.col("der_seam_med_y"), float(cfg.seam_round)))
        .withColumn("der_seam_rz", round_to(F.col("der_seam_med_z"), float(cfg.seam_round)))
    )

    # Posture fingerprint candidates (prefer worldtcp; else axes)
    has_worldtcp = (
        F.col("der_pose_tx").isNotNull() &
        F.col("der_pose_ty").isNotNull() &
        F.col("der_pose_tz").isNotNull()
    )
    # Rounded posture
    agg = (
        agg
        .withColumn("der_pose_rtx", round_to(F.col("der_pose_tx"), float(cfg.pose_round)))
        .withColumn("der_pose_rty", round_to(F.col("der_pose_ty"), float(cfg.pose_round)))
        .withColumn("der_pose_rtz", round_to(F.col("der_pose_tz"), float(cfg.pose_round)))
    )

    # Choose fingerprint type: 'seam' if camera was mostly on & medians exist; else 'pose'
    use_seam = (
        (F.col("der_cam_on_ratio") > F.lit(0.5)) &
        F.col("der_seam_med_x").isNotNull() &
        F.col("der_seam_med_y").isNotNull() &
        F.col("der_seam_med_z").isNotNull()
    )

    # Build the fingerprint string
    seam_fp = F.concat_ws(
        ",",
        F.lit("S"),
        F.col("der_seam_rx").cast("string"),
        F.col("der_seam_ry").cast("string"),
        F.col("der_seam_rz").cast("string"),
    )

    world_fp = F.concat_ws(
        ",",
        F.lit("W"),
        F.col("der_pose_rtx").cast("string"),
        F.col("der_pose_rty").cast("string"),
        F.col("der_pose_rtz").cast("string"),
    )

    axes_fp = F.concat_ws(
        ",",
        F.lit("R"),
        round_to(F.col("der_pose_ax1"), float(cfg.pose_round)).cast("string"),
        round_to(F.col("der_pose_ax2"), float(cfg.pose_round)).cast("string"),
        round_to(F.col("der_pose_ax3"), float(cfg.pose_round)).cast("string"),
    )

    pose_fp = F.when(has_worldtcp, world_fp).otherwise(axes_fp)

    agg = (
        agg
        .withColumn("der_fingerprint_type", F.when(use_seam, F.lit("seam")).otherwise(F.lit("pose")))
        .withColumn("der_joint_fingerprint", F.when(use_seam, seam_fp).otherwise(pose_fp))
    )

    # Build a stable ID: hash(activetask | step_ctx | fingerprint)
    base_id_str = F.concat_ws(
        "|",
        F.coalesce(F.col("der_act_ctx").cast("string"), F.lit("NA")),
        F.coalesce(F.col("der_step_ctx").cast("string"), F.lit("NA")),
        F.coalesce(F.col("der_joint_fingerprint"), F.lit("NA")),
    )
    agg = agg.withColumn("der_joint_id_no_wps", F.sha2(base_id_str, 256))

    # Join fingerprints/IDs back to all rows by der_joint_seg_id
    df = (
        df.join(agg, on="der_joint_seg_id", how="left")
    )

    return df