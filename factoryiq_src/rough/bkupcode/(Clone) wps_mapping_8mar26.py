from pyspark.sql import functions as F

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 

def add_wps_joint_id(
    stage3_df,
    wps_ref_df,
    step_col="d_step_major_f",
    output_col="wps_joint_id"
):
    """
    Adds wps_joint_id to stage3_df by matching step ranges
    against wps_reference_table.

    Matching rule:
        step_col BETWEEN first_step_number AND last_step_number (inclusive)

    Null behavior:
        If step_col is NULL → output_col remains NULL
    """

    # Alias for clarity
    s = stage3_df.alias("s")
    w = wps_ref_df.alias("w")

    # Range join
    joined_df = (
        s.join(
            w,
            (F.col(f"s.{step_col}").isNotNull()) &
            (F.col(f"s.{step_col}") >= F.col("w.first_step_number")) &
            (F.col(f"s.{step_col}") <= F.col("w.last_step_number")),
            how="left"
        )
    )

    # Assign joint id only when step exists
    result_df = joined_df.withColumn(
        output_col,
        F.when(
            F.col(f"s.{step_col}").isNull(),
            F.lit(None)
        ).otherwise(F.col("w.id"))
    )

    # Drop reference-table columns to avoid polluting schema
    ref_cols = [c for c in wps_ref_df.columns]
    result_df = result_df.drop(*ref_cols)

    return result_df