from pyspark.sql import DataFrame
import importlib

# from core.logging_config import get_module_logger
# logger = get_module_logger(__name__)  # produces 'factoryiq.pipelines.gold_data_enrichment'

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)  # produces 'factoryiq.pipelines.gold_data_enrichment'

# def filter_columns_by_reference(df: DataFrame, ref_df: DataFrame) -> DataFrame:
#     """
#     Filters columns of a DataFrame to keep only those present in the reference DataFrame.
    
#     Args:
#         df (DataFrame): Input Spark DataFrame.
#         ref_df (DataFrame): Reference DataFrame with column names in the first column.
        
#     Returns:
#         DataFrame: Filtered DataFrame containing only allowed columns.
#     """
#     # Get reference list
#     first_col_name = ref_df.columns[0]
#     ref_list = [row[first_col_name] for row in ref_df.select(first_col_name).collect()]
    
#     # Filter columns
#     columns_to_keep = [c for c in df.columns if c in ref_list]
#     df_filtered = df.select(columns_to_keep)
    
#     # Logging instead of print
#     logging.debug(f"Original columns: {len(df.columns)}; Filtered columns: {len(columns_to_keep)}")
    
#     return df_filtered


# from pyspark.sql import DataFrame
# from core.logging_config import get_module_logger

# logger = get_module_logger(__name__)

def filter_columns_by_reference(df: DataFrame, ref_df: DataFrame) -> DataFrame:
    """
    Filters columns of a DataFrame to keep only those present in the reference DataFrame.
    """
    first_col_name = ref_df.columns[0]
    ref_list = [row[first_col_name] for row in ref_df.select(first_col_name).collect()]

    columns_to_keep = [c for c in df.columns if c in ref_list]

    # Use logger (not logging)
    logger.debug("Original columns: %d; Filtered columns: %d", len(df.columns), len(columns_to_keep))

    # Defensive guard: if nothing to keep, log and return empty frame with a clear error
    if not columns_to_keep:
        msg = (
            f"No overlapping columns between input df ({len(df.columns)} cols) and reference list "
            f"({len(ref_list)} names). First few input cols: {df.columns[:5]}; "
            f"first few ref names: {ref_list[:5]}"
        )
        logger.error(msg)
        # Either raise or proceed with df unchanged; raising is safer for data quality
        raise ValueError(msg)

    # Use star-unpack to avoid select([]) AnalysisException in some Spark versions
    df_filtered = df.select(*columns_to_keep)
    return df_filtered
