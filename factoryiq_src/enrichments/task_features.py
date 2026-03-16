# enrichments/task_features.py

"""Task feature derivations: extract active task number; derive major step."""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def extract_active_task_num(
    df: DataFrame,
    source_col: str = "activetask",
    output_col: str = "der_activetask_num"
) -> DataFrame:
    """Extract numeric part from task column (e.g., 'Task12' -> 12)."""
    logger.debug(
        "extract_active_task_num: source_col=%s -> output_col=%s",
        source_col, output_col
    )

    # Step 1: regex extract digits into output_col
    df = df.withColumn(
        output_col,
        F.regexp_extract(F.col(source_col), r"(\d+)", 1)
    )

    # Step 2: convert empty string to NULL
    df = df.withColumn(
        output_col,
        F.when(F.col(output_col) == "", None).otherwise(F.col(output_col))
    )

    # Step 3: cast to int safely
    df = df.withColumn(
        output_col,
        F.col(output_col).cast("int")
    )

    logger.debug("extract_active_task_num: completed")
    return df


def add_step_major(
    df,
    source_col: str = "currentstepnumber",
    output_col: str = "der_step_major",
):
    """Derive major step (int) from dot-separated step string (e.g., '3.2.1' -> 3)."""
    logger.debug(
        "add_step_major: source_col=%s -> output_col=%s",
        source_col, output_col
    )

    out = df.withColumn(
        output_col,
        F.when(F.col(source_col).isNull(), None)
         .when(F.trim(F.col(source_col)) == "", None)
         .when(F.col(source_col) == "0.0.0", None)
         .otherwise(F.expr(f"try_cast(split({source_col}, '\\\\.')[0] as int)"))
    )

    logger.debug("add_step_major: completed")
    return out