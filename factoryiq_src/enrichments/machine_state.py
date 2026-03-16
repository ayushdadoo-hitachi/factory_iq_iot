# enrichments/machine_state.py
from pyspark.sql import functions as F
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def add_der_machine_state(
    df,
    current_col: str = "tps500i_current",
    techdev_col: str = "arcseam_isactive",
    output_col: str = "der_machine_state",
):
    """
    Derive machine state: WELDING if tech device active, else POWERED_ON_IDLE if current > 50, else OFF.
    """
    logger.debug(
        "add_der_machine_state: current_col=%s, techdev_col=%s, output_col=%s, idle_threshold=%d",
        current_col, techdev_col, output_col, 50
    )
    out = df.withColumn(
        output_col,
        F.when(F.col(techdev_col) == 1, F.lit("WELDING"))
         .when(F.col(current_col) > 50, F.lit("POWERED_ON_IDLE"))
         .otherwise(F.lit("OFF"))
    )
    logger.debug(
        "add_der_machine_state: completed; output_col='%s' created",
        output_col
    )
    return out


def add_welding_active(df, state_col: str = "der_machine_state"):
    """
    Flag welding active: 1 when der_machine_state == 'WELDING', else 0.
    """
    logger.debug("add_welding_active: state_col=%s -> output_col=der_welding_active", state_col)
    out = df.withColumn(
        "der_welding_active",
        F.when(F.col(state_col) == "WELDING", 1).otherwise(0),
    )
    logger.debug("add_welding_active: completed; output_col='der_welding_active' created")
    return out