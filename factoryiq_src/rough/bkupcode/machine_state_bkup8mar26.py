# enrichments/machine_state.py
from pyspark.sql import functions as F

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 


def add_der_machine_state(
    df,
    current_col: str = "tps500i_current",
    techdev_col: str = "arcseam_isactive",
    output_col: str = "der_machine_state",
):
    return df.withColumn(
        output_col,
        F.when(F.col(techdev_col) == 1, F.lit("WELDING"))
         .when(F.col(current_col) > 50, F.lit("POWERED_ON_IDLE"))
         .otherwise(F.lit("OFF"))
    )


def add_welding_active(df, state_col: str = "der_machine_state"):
    return df.withColumn(
        "der_welding_active",
        F.when(F.col(state_col) == "WELDING", 1).otherwise(0),
    )