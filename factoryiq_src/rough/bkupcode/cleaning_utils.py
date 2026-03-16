from core.io_utils import read_excel_from_abfss
import logging
logger = logging.getLogger(__name__)

# core/cleaning_utils.py
"""
Core cleaning and transformation utilities for Spark DataFrames.

Includes:
- Column name normalization
- Regex-based cleaning
- String normalization
- Comma-to-dot conversion
- Invalid integer row removal
- Datatype application
- Numeric round-off
- Timestamp parsing
"""

import re
from functools import reduce
import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    BooleanType,
    TimestampType,
)


# Spark types
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType, TimestampType
)


def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Return a new DataFrame with column names normalized by
    core.cleaning_utils._normalize_column_name.
    """
    new_cols = [_normalize_column_name(c) for c in df.columns]
    return df.toDF(*new_cols)


# ------------------------------------------
# 3️⃣ Excel PDF Sanitizer
# ------------------------------------------

def sanitize_excel_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize Excel-imported Pandas DF for Spark + Arrow compatibility.
    """
    clean = pdf.copy()

    for col in clean.columns:
        dtype = clean[col].dtype

        if dtype in ["int64", "float64"]:
            continue

        clean[col] = (
            clean[col]
            .astype(str)
            .replace({
                "nan": None,
                "NaN": None,
                "None": None,
                "": None
            })
        )

    return clean

# ------------------------------------------
# 2️⃣ Pandas → Spark Schema Mapper
# ------------------------------------------

def pandas_to_spark_schema(pdf: pd.DataFrame) -> StructType:
    fields = []

    for col, dtype in pdf.dtypes.items():
        if dtype == "int64":
            spark_type = LongType()
        elif dtype == "float64":
            spark_type = DoubleType()
        else:
            spark_type = StringType()

        fields.append(StructField(col, spark_type, True))

    return StructType(fields)


# ------------------------------------------
# 5️⃣ Excel Path → Spark DataFrame
# ------------------------------------------

def excel_path_to_spark_df(spark, path: str) -> DataFrame:
    pdf = read_excel_from_abfss(spark, path)
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    sdf = spark.createDataFrame(pdf, schema=schema)
    return normalize_columns(sdf)



def regex_clean_columns(df: DataFrame) -> DataFrame:
    """Applies column-wise regex cleaning: remove quotes, fix scientific notation, collapse dots."""
    for c in df.columns:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r'["\']', ""))
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"([0-9])\.?([0-9]+)-([0-9]+)$", r"\1.\2E-\3"))
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"\.+", "."))
    return df


def normalize_strings(df: DataFrame) -> DataFrame:
    """Normalize string values: convert nan/null/none/inf/infinity/whitespace to empty string."""
    nan_pattern = r'^\s*(nan|null|none|inf|infinity)\s*$'
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.lower(F.trim(F.col(c))).rlike(nan_pattern), "")
             .when(F.trim(F.col(c)) == "", "")
             .otherwise(F.col(c))
        )
    return df


# -----------------------
# Column transformations
# -----------------------
def apply_comma_to_dot(df: DataFrame, ref_df: DataFrame) -> DataFrame:
    """Replace commas with dots for specified columns in reference DataFrame."""
    columns_to_fix = [r["ColumnName"] for r in ref_df.filter(F.col("comma_to_dot") == "yes").select("ColumnName").collect()]
    for col_name in columns_to_fix:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", "."))
    return df


def drop_rows_with_invalid_int_values(df: DataFrame, ref_df: DataFrame, log: bool = True) -> tuple[DataFrame, int]:
    """Drops rows where integer columns contain non-integer values."""
    int_columns = [r["ColumnName"] for r in ref_df.filter(F.col("DataType").rlike("(?i)^integer$")).select("ColumnName").collect()]
    if not int_columns:
        return df, 0

    invalid_pattern = r'^[+-]?[0-9]+$'
    initial_count = df.count()

    invalid_conditions = [
        (F.trim(F.col(c)).isNotNull()) & (F.trim(F.col(c)) != "") & (~F.trim(F.col(c)).rlike(invalid_pattern))
        for c in int_columns if c in df.columns
    ]
    if not invalid_conditions:
        return df, 0

    combined_condition = reduce(lambda a, b: a | b, invalid_conditions)
    df_clean = df.filter(~combined_condition)
    removed_rows = initial_count - df_clean.count()

    if log:
        logger.info("🧹 Rows removed due to invalid INT values: %d", removed_rows)
        logger.info("✅ Rows remaining: %d", df_clean.count())

    return df_clean, removed_rows


# -----------------------
# Datatype & rounding
# -----------------------
type_map = {
    "double": DoubleType(),
    "float": FloatType(),
    "integer": IntegerType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "long": LongType(),
    "timestamp": TimestampType(),
}

def apply_datatype(df: DataFrame, ref_df: DataFrame) -> DataFrame:
    """Applies datatypes to columns as per reference DataFrame."""
    ref_cols = ref_df.select("ColumnName", "DataType").collect()
    allowed_cols = [r["ColumnName"] for r in ref_cols]
    df = df.select([c for c in df.columns if c in allowed_cols])

    numeric_types = {"double", "float", "integer"}

    for row in ref_cols:
        col_name = row["ColumnName"]
        dtype = row["DataType"].lower().strip()
        if col_name not in df.columns:
            continue

        if dtype in numeric_types:
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", ""))
            df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)))

        if dtype in type_map:
            df = df.withColumn(col_name, F.col(col_name).cast(type_map[dtype]))
        else:
            logger.warning("⚠ Unknown datatype in reference: %s (column %s)", dtype, col_name)

    return df


def apply_roundoff_to_numeric_columns(df: DataFrame, ref_df: DataFrame, log: bool = True) -> DataFrame:
    """Rounds numeric columns based on 'roundoff' specified in reference DataFrame."""
    round_rules = ref_df.filter(F.col("roundoff").isNotNull()).select("ColumnName", "roundoff").collect()

    for row in round_rules:
        col_name = row["ColumnName"]
        scale = int(row["roundoff"])
        if col_name in df.columns:
            df = df.withColumn(col_name, F.round(F.col(col_name).cast("double"), scale))
            if log:
                logger.info(f"✅ Rounded column '{col_name}' to {scale} decimal places")
    return df


# -----------------------
# Timestamp parsing
# -----------------------
def fix_and_parse_timestamp(df: DataFrame, colname: str = "time", use_try_to_timestamp: bool = False) -> DataFrame:
    """Cleans timestamp column and parses to Spark TimestampType, truncated to seconds."""
    cleaned_col = F.trim(F.regexp_replace(F.col(colname), '"', ""))

    if use_try_to_timestamp:
        parsed_col = F.date_trunc(
            "second",
            F.expr(f"try_to_timestamp({colname}, 'dd.MM.yyyy HH:mm:ss.SSSSSS')")
        )
    else:
        parsed_col = F.date_trunc("second", F.to_timestamp(cleaned_col, "dd.MM.yyyy HH:mm:ss.SSSSSS"))

    return df.withColumn(colname, parsed_col).orderBy(colname)


# -----------------------
# Filter out of range rows
# -----------------------
def filter_out_of_range_rows(
    df: DataFrame,
    ref_df: DataFrame,
) -> DataFrame:
    """
    Removes rows where any numeric column violates min/max bounds.

    Args:
        df: Input Spark DataFrame
        ref_df: Reference DataFrame containing ColumnName, Min, Max

    Returns:
        Filtered Spark DataFrame
    """

    bounds = (
        ref_df
        .select("ColumnName", "Min", "Max")
        .filter(F.col("Min").isNotNull() | F.col("Max").isNotNull())
        .collect()
    )

    if not bounds:
        return df

    keep_condition = F.lit(True)

    for row in bounds:
        col_name = row["ColumnName"]

        if col_name not in df.columns:
            continue

        min_v = float(row["Min"]) if row["Min"] is not None else None
        max_v = float(row["Max"]) if row["Max"] is not None else None

        col_condition = F.lit(True)

        if min_v is not None:
            col_condition = col_condition & (F.col(col_name) >= min_v)

        if max_v is not None:
            col_condition = col_condition & (F.col(col_name) <= max_v)

        keep_condition = keep_condition & col_condition

    return df.filter(keep_condition)


# def drop_min_max_cols(df):
#     pattern = re.compile(r".*_(min|max)(_\d+)?$")
#     cols_to_keep = [c for c in df.columns if not pattern.match(c)]
#     return df.select(*cols_to_keep)



