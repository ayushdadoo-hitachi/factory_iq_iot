from typing import Iterable, Optional
from pyspark.sql import DataFrame
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def filter_columns_by_reference(
    df: DataFrame,
    ref_df: DataFrame,
    keep_extra_cols: Optional[Iterable[str]] = None
) -> DataFrame:

    logger.debug("=== SCHEMA UTILS DEBUG: START filter_columns_by_reference ===")

    # Log initial DF columns
    logger.debug("[schema_utils] Input DF columns (%d): %s", len(df.columns), df.columns)

    # Read the reference list
    first_col_name = ref_df.columns[0]
    ref_list = [row[first_col_name] for row in ref_df.select(first_col_name).collect()]

    logger.debug("[schema_utils] Reference column name: %s", first_col_name)
    logger.debug("[schema_utils] Reference column list size: %d", len(ref_list))
    logger.debug("[schema_utils] First 10 reference names: %s", ref_list[:10])

    # Extra columns to keep
    extra = set(keep_extra_cols or [])
    logger.debug("[schema_utils] Extra columns requested to keep: %s", list(extra))

    # Compute final list of columns
    columns_to_keep = [c for c in df.columns if (c in ref_list) or (c in extra)]

    logger.debug(
        "[schema_utils] Columns retained (%d): %s",
        len(columns_to_keep),
        columns_to_keep
    )

    dropped_columns = set(df.columns) - set(columns_to_keep)
    logger.debug("[schema_utils] Columns dropped (%d): %s", len(dropped_columns), list(dropped_columns))

    # No matches means critical error
    if not columns_to_keep:
        msg = (
            f"No overlapping columns between input df ({len(df.columns)} cols) "
            f"and reference list ({len(ref_list)} names). "
            f"Input cols: {df.columns[:5]} ... "
            f"Ref names: {ref_list[:5]} ..."
        )
        logger.error(msg)
        raise ValueError(msg)

    # Final DF
    result = df.select(*columns_to_keep)

    logger.debug("[schema_utils] Output DF column count: %d", len(result.columns))
    logger.debug("[schema_utils] Output columns: %s", result.columns)
    logger.debug("=== SCHEMA UTILS DEBUG: END filter_columns_by_reference ===")

    return result