from pyspark.sql import functions as F
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def add_wps_joint_id(
    stage3_df,
    wps_ref_df,
    step_col: str = "d_step_major_f",
    output_col: str = "wps_joint_id",
):
    """Attach WPS joint id by joining step range: first_step_number <= step_col <= last_step_number."""
    logger.debug(
        "add_wps_joint_id: step_col=%s, output_col=%s, ref_cols(sample)=%s",
        step_col, output_col, wps_ref_df.columns[:5]
    )

    # Alias for clarity (no functional change)
    s = stage3_df.alias("s")
    w = wps_ref_df.alias("w")

    # Range join (inclusive bounds); skip when step is NULL
    logger.debug("add_wps_joint_id: performing left join on step range [w.first_step_number, w.last_step_number]")
    joined_df = (
        s.join(
            w,
            (F.col(f"s.{step_col}").isNotNull())
            & (F.col(f"s.{step_col}") >= F.col("w.first_step_number"))
            & (F.col(f"s.{step_col}") <= F.col("w.last_step_number")),
            how="left",
        )
    )

    # Assign joint id only when step exists
    logger.debug("add_wps_joint_id: projecting output_col='%s' from w.id when step present", output_col)
    result_df = joined_df.withColumn(
        output_col,
        F.when(F.col(f"s.{step_col}").isNull(), F.lit(None)).otherwise(F.col("w.id")),
    )

    # Drop reference-table columns to avoid polluting schema
    ref_cols = [c for c in wps_ref_df.columns]
    logger.debug("add_wps_joint_id: dropping reference columns to keep schema clean (count=%d)", len(ref_cols))
    result_df = result_df.drop(*ref_cols)

    logger.debug("add_wps_joint_id: completed; output_col='%s' added", output_col)
    return result_df