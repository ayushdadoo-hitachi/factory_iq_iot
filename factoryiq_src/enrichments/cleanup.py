from pyspark.sql import DataFrame
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__) 

def drop_intermediate_columns(df: DataFrame) -> DataFrame:
    cols_to_drop = {
        "currentprogram",
        "der_activetask_num",
        "der_time_diff_sec",
        "der_machine_state",
        "der_step_major",
        "atask_segment_id",
        "mstep_segment_id",
        "atseg_id",
        "smseg_id",
    }

    existing_cols = list(cols_to_drop.intersection(df.columns))

    if existing_cols:
        logger.debug(f"Dropping intermediate columns: {existing_cols}")
        df = df.drop(*existing_cols)
    else:
        logger.debug("No intermediate columns found to drop")

    return df