# stage1_data_quality_pipeline.py
"""
Stage 1 Data Pipeline
- Reads input Delta table
- Cleans and normalizes columns
- Applies reference schema and data types
- Writes output Delta table
"""

# Core utilities
from core.cleaning_utils import (
    clean_columns,
    regex_clean_columns,
    normalize_strings,
    apply_comma_to_dot,
    drop_rows_with_invalid_int_values,
    apply_datatype,
    apply_roundoff_to_numeric_columns,
    fix_and_parse_timestamp,
)

# I/O utilities
from core.io_utils import read_delta, write_delta

# Schema utilities
from core.schema_utils import filter_columns_by_reference


def run_stage1_pipeline(spark, input_path: str, ref_weld_path: str, output_path: str):
    """
    Executes Stage 1 of the ETL pipeline.
    
    Parameters:
    - spark: SparkSession
    - input_path: str, path to input Delta table
    - ref_weld_path: str, path to reference Delta table
    - output_path: str, path to write processed Delta table
    
    Returns:
    - df: Spark DataFrame after Stage 1 transformations
    """
    
    # 1️⃣ Read input data
    df = read_delta(spark, input_path)
    
    # 2️⃣ Clean column names
    df = clean_columns(df)
    
    # 3️⃣ Read reference data
    ref_weld_df = read_delta(spark, ref_weld_path)
    
    # 4️⃣ Filter columns based on reference
    df = filter_columns_by_reference(df, ref_weld_df)
    
    # 5️⃣ Cleaning & normalization
    df = regex_clean_columns(df)
    df = normalize_strings(df)
    
    # 6️⃣ Apply transformations based on reference
    df = apply_comma_to_dot(df, ref_weld_df)
    df, removed = drop_rows_with_invalid_int_values(df, ref_weld_df)
    # print(f"Removed rows with invalid integers: {removed}")
    
    df = apply_datatype(df, ref_weld_df)
    df = apply_roundoff_to_numeric_columns(df, ref_weld_df)
    df = fix_and_parse_timestamp(df)
    
    # 7️⃣ Write processed data
    write_delta(df, output_path)
    
    return df
