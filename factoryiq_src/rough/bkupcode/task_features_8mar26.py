# enrichments/task_features.py

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 


def extract_active_task_num(
    df: DataFrame,
    source_col: str = "activetask",
    output_col: str = "der_activetask_num"
) -> DataFrame:
    """
    Extract numeric part from task column (e.g. Task12 → 12).

    Steps:
    1. Extract digits using regex
    2. Convert empty string to NULL
    3. Cast to Integer safely
    """

    df = df.withColumn(
        output_col,
        F.regexp_extract(F.col(source_col), r"(\d+)", 1)
    )

    df = df.withColumn(
        output_col,
        F.when(F.col(output_col) == "", None)
         .otherwise(F.col(output_col))
    )

    df = df.withColumn(
        output_col,
        F.col(output_col).cast("int")
    )

    return df


def add_step_major(
    df,
    source_col: str = "currentstepnumber",
    output_col: str = "der_step_major",
):
    return df.withColumn(
        output_col,
        F.when(F.col(source_col).isNull(), None)
         .when(F.trim(F.col(source_col)) == "", None)
         .when(F.col(source_col) == "0.0.0", None)
         .otherwise(
             F.expr(f"try_cast(split({source_col}, '\\\\.')[0] as int)")
         )
    )