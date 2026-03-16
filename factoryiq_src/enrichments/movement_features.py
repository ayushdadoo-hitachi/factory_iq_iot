from pyspark.sql import functions as F
from pyspark.sql.window import Window
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 

def compute_der_euc_distance_spark(
    df,
    x_col="tcp_x",
    y_col="tcp_y",
    z_col="tcp_z",
    order_col="time",
    partition_col="d_activetask_f",
    output_col="der_euc_distance",
):
    """
    Computes Euclidean distance between consecutive TCP points.

    Distance is computed within each partition (e.g., active task)
    ordered by time.

    Null-safe:
        First row per partition → distance = 0
    """

    logger.debug(f"Computing {output_col}")

    w = Window.partitionBy(partition_col).orderBy(order_col)

    # Compute deltas inline (no intermediate persisted columns)
    dx = F.col(x_col) - F.lag(x_col).over(w)
    dy = F.col(y_col) - F.lag(y_col).over(w)
    dz = F.col(z_col) - F.lag(z_col).over(w)

    df = df.withColumn(
        output_col,
        F.sqrt(
            F.coalesce(dx, F.lit(0)) ** 2 +
            F.coalesce(dy, F.lit(0)) ** 2 +
            F.coalesce(dz, F.lit(0)) ** 2
        )
    )

    return df



def compute_der_bigmove_spark(
    df,
    distance_col="der_euc_distance",
    threshold=150.0,
    output_col="der_bigmove",
):
    """
    Flags big movements based on distance threshold.

    Null-safe:
        If distance is NULL → False
    """

    logger.debug(f"Computing {output_col} with threshold={threshold}")

    return df.withColumn(
        output_col,
        F.when(
            F.col(distance_col) > F.lit(threshold),
            F.lit(True)
        ).otherwise(F.lit(False))
    )


def add_der_euc_seg_id(df):
    w = Window.partitionBy("d_activetask_f").orderBy("time")

    return df.withColumn(
        "der_euc_seg_id",
        F.sum(F.col("der_bigmove").cast("int")).over(w)
    )