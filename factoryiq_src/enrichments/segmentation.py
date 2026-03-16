# enrichments/segmentation.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import importlib

from core.logging_config import get_module_logger
logger = get_module_logger(__name__)  # produces 'factoryiq.pipelines.gold_data_enrichment'


def add_atask_segment_id(
    df,
    src_col="der_activetask_num",
    order_col="time",
    output_col="atask_segment_id"
):
    """
    Creates new segment id whenever active task changes.
    """

    logger.debug(f"Creating {output_col}")

    w = Window.orderBy(order_col)

    df = df.withColumn(
        "_prev_task",
        F.lag(F.col(src_col)).over(w)
    )

    df = df.withColumn(
        "_task_change_flag",
        F.when(
            (F.col(src_col).isNotNull()) &
            (F.col(src_col) != F.col("_prev_task")),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        output_col,
        F.sum(F.col("_task_change_flag")).over(w)
    ).drop("_prev_task", "_task_change_flag")

    return df


def add_mstepnumber_segment_id(
    df,
    src_col="der_step_major",
    order_col="time",
    output_col="mstep_segment_id"
):
    """
    Creates a new segment id whenever step_major changes.
    """

    logger.debug(f"Creating {output_col}")

    w = Window.orderBy(order_col)

    df = df.withColumn(
        "_prev_step",
        F.lag(F.col(src_col)).over(w)
    )

    df = df.withColumn(
        "_step_change_flag",
        F.when(
            (F.col(src_col).isNotNull()) &
            (F.col(src_col) != F.col("_prev_step")),
            1
        ).otherwise(0)
    )

    df = df.withColumn(
        output_col,
        F.sum(F.col("_step_change_flag")).over(w)
    ).drop("_prev_step", "_step_change_flag")

    return df


def add_d_activetask_f(
    df,
    task_col="der_activetask_num",
    order_col="time",
    output_col="d_activetask_f"
):
    """
    Forward fill active task using window function.
    """

    logger.debug(f"Creating {output_col}")

    w = Window.orderBy(order_col).rowsBetween(
        Window.unboundedPreceding, 0
    )

    df = df.withColumn(
        output_col,
        F.coalesce(
            F.last(F.col(task_col), ignorenulls=True).over(w),
            F.lit(0)
        )
    )

    return df


def add_d_step_major_f(
    df,
    step_col="der_step_major",
    order_col="time",
    output_col="d_step_major_f"
):
    """
    Forward fill step major using window function.
    """

    logger.debug(f"Creating {output_col}")

    w = Window.orderBy(order_col).rowsBetween(
        Window.unboundedPreceding, 0
    )

    df = df.withColumn(
        output_col,
        F.coalesce(
            F.last(F.col(step_col), ignorenulls=True).over(w),
            F.lit(0)
        )
    )

    return df