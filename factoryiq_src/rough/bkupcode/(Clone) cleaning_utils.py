from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, trim, lower, when, regexp_replace, round as spark_round
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    BooleanType,
    TimestampType,
)
from functools import reduce
import re


def _normalize_column_name(col_name: str) -> str:
    """
    Internal helper to normalize a single column name.
    """

    # Strip leading/trailing spaces
    col_name = col_name.strip()

    # Lowercase
    col_name = col_name.lower()

    # Replace spaces with underscore
    col_name = re.sub(r"\s+", "_", col_name)

    # Remove special characters except underscore
    col_name = re.sub(r"[^a-z0-9_]", "", col_name)

    # Remove duplicate underscores
    col_name = re.sub(r"_+", "_", col_name)

    return col_name


def clean_columns(df: DataFrame) -> DataFrame:
    """
    Cleans and standardizes column names in a Spark DataFrame.

    - Strips spaces
    - Lowercases
    - Replaces spaces with underscore
    - Removes special characters
    - Ensures unique column names
    """

    new_columns = []
    seen = set()

    for col in df.columns:
        cleaned = _normalize_column_name(col)

        # Handle duplicate column names
        if cleaned in seen:
            i = 1
            while f"{cleaned}_{i}" in seen:
                i += 1
            cleaned = f"{cleaned}_{i}"

        seen.add(cleaned)
        new_columns.append(cleaned)

    # Rename columns
    for old, new in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old, new)

    return df

def regex_clean_columns(df):
    """
    Applies column-wise regex cleaning:
      - Removes single and double quotes
      - Fixes broken scientific notation (5.25-05 → 5.25E-05)
      - Collapses multiple dots (3....14 → 3.14)
    """
    import re
    from pyspark.sql.functions import col, regexp_replace

    for c in df.columns:
        df = df.withColumn(c, regexp_replace(col(c), r'["\']', ""))  # remove quotes
        df = df.withColumn(c, regexp_replace(col(c), r"([0-9])\.?([0-9]+)-([0-9]+)$", r"\1.\2E-\3"))  # fix sci notation
        df = df.withColumn(c, regexp_replace(col(c), r"\.+", "."))  # collapse multiple dots

    return df

def normalize_strings(df):
    """
    Normalizes column values:
      - Converts NaN-like values (nan, null, none, inf, infinity) to empty string
      - Converts whitespace-only values to empty string
    """
    from pyspark.sql.functions import when, trim, lower

    nan_pattern = r'^\s*(nan|null|none|inf|infinity)\s*$'

    for c in df.columns:
        df = df.withColumn(
            c,
            when(lower(trim(col(c))).rlike(nan_pattern), "")
            .when(trim(col(c)) == "", "")
            .otherwise(col(c))
        )

    return df

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def apply_comma_to_dot(df: DataFrame, ref_df: DataFrame) -> DataFrame:
    # Identify columns to fix
    columns_to_fix = [
        row["ColumnName"]
        for row in ref_df
            .filter(F.col("comma_to_dot") == "yes")
            .select("ColumnName")
            .collect()
    ]

    # Replace commas with dots
    for col_name in columns_to_fix:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.regexp_replace(F.col(col_name), ",", ".")
            )

    return df


def drop_rows_with_invalid_int_values(df: DataFrame, ref_df: DataFrame, log: bool = True) -> tuple[DataFrame, int]:
    """
    Drops rows where ANY integer column (from ref_df) contains a non-integer value.

    Args:
        df: Spark DataFrame to clean.
        ref_df: Reference DataFrame with columns metadata. Must have 'ColumnName' and 'DataType'.
        log: Whether to print rows removed info.

    Returns:
        Tuple: (cleaned DataFrame, number of rows removed)
    """

    # -----------------------
    # 1️⃣ Extract integer columns from reference
    # -----------------------
    int_columns = [
        row["ColumnName"]
        for row in ref_df
            .filter(col("DataType").rlike("(?i)^integer$"))  # case-insensitive match
            .select("ColumnName")
            .collect()
    ]

    if not int_columns:
        if log:
            print("ℹ️ No integer columns found in reference — no rows removed")
        return df, 0

    # -----------------------
    # 2️⃣ Build invalid condition
    # -----------------------
    invalid_pattern = r'^[+-]?[0-9]+$'
    initial_count = df.count()

    invalid_conditions = [
        (trim(col(c)).isNotNull()) &
        (trim(col(c)) != "") &
        (~trim(col(c)).rlike(invalid_pattern))
        for c in int_columns if c in df.columns
    ]

    if not invalid_conditions:
        if log:
            print("ℹ️ No integer columns present in DataFrame — no rows removed")
        return df, 0

    combined_invalid_condition = reduce(lambda a, b: a | b, invalid_conditions)

    # -----------------------
    # 3️⃣ Filter invalid rows
    # -----------------------
    df_clean = df.filter(~combined_invalid_condition)
    removed_rows = initial_count - df_clean.count()

    if log:
        print(f"🧹 Rows removed due to invalid INT values: {removed_rows}")
        print(f"✅ Rows remaining: {df_clean.count()}")

    return df_clean, removed_rows


type_map = {
    "double": DoubleType(),
    "float": FloatType(),
    "integer": IntegerType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "long": LongType(),
    "timestamp": TimestampType()
}

def apply_datatype(df: DataFrame, ref_df: DataFrame) -> DataFrame:
    """
    Filters columns as per reference table, cleans numeric columns, 
    and applies proper data types.
    """

    # Keep only allowed columns
    ref_cols = ref_df.select("ColumnName", "DataType").collect()
    allowed_cols = [r["ColumnName"] for r in ref_cols]
    df = df.select([c for c in df.columns if c in allowed_cols])

    # Prepare mapping for numeric cleaning
    numeric_types = {"double", "float", "integer"}

    for row in ref_cols:
        col_name = row["ColumnName"]
        dtype = row["DataType"].lower().strip()

        if col_name in df.columns:
            # Clean numeric columns
            if dtype in numeric_types:
                df = df.withColumn(col_name, regexp_replace(col(col_name), ",", ""))
                df = df.withColumn(col_name,
                    when(trim(col(col_name)) == "", None)
                    .otherwise(col(col_name))
                )
            
            # Apply type casting
            if dtype in type_map:
                df = df.withColumn(col_name, col(col_name).cast(type_map[dtype]))
            else:
                print(f"⚠ Unknown datatype in reference: {dtype} (column {col_name})")

    return df




def apply_roundoff_to_numeric_columns(df: DataFrame, ref_df: DataFrame, log: bool = True) -> DataFrame:
    """
    Rounds numeric columns based on 'roundoff' specified in ref_df.
    
    Args:
        df: Spark DataFrame to process.
        ref_df: Reference DataFrame containing 'ColumnName' and 'roundoff'.
        log: Whether to print info about applied roundoff.

    Returns:
        Spark DataFrame with rounded numeric columns.
    """

    # Collect roundoff rules from reference
    round_rules = (
        ref_df
        .filter(col("roundoff").isNotNull())
        .select("ColumnName", "roundoff")
        .collect()
    )

    for row in round_rules:
        col_name = row["ColumnName"]
        scale = int(row["roundoff"])

        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                spark_round(col(col_name).cast("double"), scale)
            )
            if log:
                print(f"✅ Rounded column '{col_name}' to {scale} decimal places")

    return df



def fix_and_parse_timestamp(df: DataFrame, colname: str = "time", use_try_to_timestamp: bool = False) -> DataFrame:
    """
    Cleans a timestamp column by removing quotes, trimming whitespace, parsing to timestamp,
    truncating to seconds, and returning the DataFrame ordered by the column.

    Args:
        df (DataFrame): Input Spark DataFrame.
        colname (str): Name of the timestamp column.
        use_try_to_timestamp (bool): Whether to use try_to_timestamp (safe parsing).

    Returns:
        DataFrame: DataFrame with cleaned and parsed timestamp column, ordered by it.
    """
    # Remove quotes and trim
    cleaned_col = F.trim(F.regexp_replace(F.col(colname), '"', ""))

    # Parse timestamp (safe or normal)
    if use_try_to_timestamp:
        parsed_col = F.date_trunc(
            "second",
            F.expr(f"try_to_timestamp({colname}, 'dd.MM.yyyy HH:mm:ss.SSSSSS')")
        )
    else:
        parsed_col = F.date_trunc(
            "second",
            F.to_timestamp(cleaned_col, "dd.MM.yyyy HH:mm:ss.SSSSSS")
        )

    # Apply parsed column and order
    return df.withColumn(colname, parsed_col).orderBy(colname)

