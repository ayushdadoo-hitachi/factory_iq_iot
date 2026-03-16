"""Spark DF cleaning/transforms: name normalize, regex sanitization, string normalize, comma->dot, drop invalid ints, dtype cast, round-off, timestamp parse, Excel->Spark helpers."""

# --- Standard library ---
import re
import importlib
from functools import reduce
import pandas as pd  # <- Only needed if using the Excel -> Spark helpers

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, FloatType, IntegerType, BooleanType, TimestampType,
)

# --- Project ---
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)

from core.io_utils import read_excel_from_abfss
from typing import Iterable, Optional


def normalize_columns(df: DataFrame) -> DataFrame:
    """Normalize all column names via _normalize_column_name()."""
    logger.debug("normalize_columns: input columns=%d", len(df.columns))
    new_cols = [_normalize_column_name(c) for c in df.columns]
    out = df.toDF(*new_cols)
    logger.debug("normalize_columns: output columns=%d", len(out.columns))
    return out


# -----------------------
# Internal helpers
# -----------------------
def _normalize_column_name(col_name: str) -> str:
    """Strip, lowercase, underscore spaces, remove special chars, collapse underscores."""
    # Intentionally no logging here (hot path per-column)
    col_name = col_name.strip().lower()
    col_name = re.sub(r"\s+", "_", col_name)
    col_name = re.sub(r"[^a-z0-9_]", "", col_name)
    col_name = re.sub(r"_+", "_", col_name)
    return col_name


# ------------------------------------------
# Excel PDF Sanitizer
# ------------------------------------------
def sanitize_excel_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """Sanitize Excel-imported Pandas DF for Spark/Arrow compatibility."""
    logger.debug("sanitize_excel_pdf: shape=%s, columns=%s", getattr(pdf, "shape", None), list(pdf.columns)[:5])
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

    logger.debug("sanitize_excel_pdf: completed")
    return clean


# ------------------------------------------
# Pandas → Spark Schema Mapper
# ------------------------------------------
def pandas_to_spark_schema(pdf: pd.DataFrame) -> StructType:
    logger.debug("pandas_to_spark_schema: columns=%d", len(pdf.columns))
    fields = []

    for col, dtype in pdf.dtypes.items():
        if dtype == "int64":
            spark_type = LongType()
        elif dtype == "float64":
            spark_type = DoubleType()
        else:
            spark_type = StringType()
        fields.append(StructField(col, spark_type, True))

    schema = StructType(fields)
    logger.debug("pandas_to_spark_schema: fields=%d", len(schema))
    return schema


# ------------------------------------------
# Excel Path → Spark DataFrame
# ------------------------------------------
def excel_path_to_spark_df(spark, path: str) -> DataFrame:
    logger.debug("excel_path_to_spark_df: reading %s", path)
    pdf = read_excel_from_abfss(spark, path)
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    sdf = spark.createDataFrame(pdf, schema=schema)
    out = normalize_columns(sdf)
    logger.debug("excel_path_to_spark_df: created spark df with columns=%d", len(out.columns))
    return out


# -----------------------
# Column cleaning
# -----------------------
def clean_columns(df: DataFrame) -> DataFrame:
    """Clean and standardize column names; ensure uniqueness."""
    logger.debug("clean_columns: input columns=%d", len(df.columns))
    new_columns = []
    seen = set()

    for c in df.columns:
        cleaned = _normalize_column_name(c)
        if cleaned in seen:
            i = 1
            while f"{cleaned}_{i}" in seen:
                i += 1
            cleaned = f"{cleaned}_{i}"
        seen.add(cleaned)
        new_columns.append(cleaned)

    for old, new in zip(df.columns, new_columns):
        if old != new:
            logger.debug("clean_columns: rename '%s' -> '%s'", old, new)
        df = df.withColumnRenamed(old, new)

    logger.debug("clean_columns: output columns=%d", len(df.columns))
    return df


def regex_clean_columns(df: DataFrame) -> DataFrame:
    """Remove quotes, fix scientific notation artifacts, collapse repeated dots."""
    logger.debug("regex_clean_columns: columns=%d", len(df.columns))
    for c in df.columns:
        df = df.withColumn(c, F.regexp_replace(F.col(c), r'["\']', ""))
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"([0-9])\.?([0-9]+)-([0-9]+)$", r"\1.\2E-\3"))
        df = df.withColumn(c, F.regexp_replace(F.col(c), r"\.+", "."))
    logger.debug("regex_clean_columns: completed")
    return df


def normalize_strings(df: DataFrame) -> DataFrame:
    """Convert nan/null/none/inf/infinity/blank to empty string."""
    logger.debug("normalize_strings: columns=%d", len(df.columns))
    nan_pattern = r'^\s*(nan|null|none|inf|infinity)\s*$'
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.lower(F.trim(F.col(c))).rlike(nan_pattern), "")
             .when(F.trim(F.col(c)) == "", "")
             .otherwise(F.col(c))
        )
    logger.debug("normalize_strings: completed")
    return df


# -----------------------
# Column transformations
# -----------------------
def apply_comma_to_dot(df: DataFrame, ref_df: DataFrame) -> DataFrame:
    """Replace commas with dots for columns flagged in reference."""
    columns_to_fix = [r["ColumnName"] for r in ref_df.filter(F.col("comma_to_dot") == "yes").select("ColumnName").collect()]
    logger.debug("apply_comma_to_dot: columns_to_fix=%d", len(columns_to_fix))
    for col_name in columns_to_fix:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", "."))
    logger.debug("apply_comma_to_dot: completed")
    return df


def drop_rows_with_invalid_int_values(df: DataFrame, ref_df: DataFrame, log: bool = True) -> tuple[DataFrame, int]:
    """Drop rows where integer columns contain non-integer values."""
    int_columns = [r["ColumnName"] for r in ref_df.filter(F.col("DataType").rlike("(?i)^integer$")).select("ColumnName").collect()]
    logger.debug("drop_rows_with_invalid_int_values: int_columns=%d", len(int_columns))
    if not int_columns:
        return df, 0

    invalid_pattern = r'^[+-]?[0-9]+$'
    initial_count = df.count()

    invalid_conditions = [
        (F.trim(F.col(c)).isNotNull()) & (F.trim(F.col(c)) != "") & (~F.trim(F.col(c)).rlike(invalid_pattern))
        for c in int_columns if c in df.columns
    ]
    if not invalid_conditions:
        logger.debug("drop_rows_with_invalid_int_values: no invalid conditions constructed")
        return df, 0

    combined_condition = reduce(lambda a, b: a | b, invalid_conditions)
    df_clean = df.filter(~combined_condition)
    removed_rows = initial_count - df_clean.count()

    if log:
        logger.debug("🧹 Rows removed due to invalid INT values: %d", removed_rows)
        logger.debug("✅ Rows remaining: %d", df_clean.count())

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

# def apply_datatype(df: DataFrame, ref_df: DataFrame) -> DataFrame:
#     """Cast columns to types as per reference; strip commas from numeric text; empty->null."""
#     ref_cols = ref_df.select("ColumnName", "DataType").collect()
#     allowed_cols = [r["ColumnName"] for r in ref_cols]
#     logger.debug("apply_datatype: allowed_cols=%d", len(allowed_cols))
#     df = df.select([c for c in df.columns if c in allowed_cols])

#     numeric_types = {"double", "float", "integer"}

#     for row in ref_cols:
#         col_name = row["ColumnName"]
#         dtype = row["DataType"].lower().strip()
#         if col_name not in df.columns:
#             continue

#         if dtype in numeric_types:
#             df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", ""))
#             df = df.withColumn(col_name, F.when(F.trim(F.col(col_name)) == "", None).otherwise(F.col(col_name)))

#         if dtype in type_map:
#             df = df.withColumn(col_name, F.col(col_name).cast(type_map[dtype]))
#         else:
#             logger.warning("⚠ Unknown datatype in reference: %s (column %s)", dtype, col_name)

#     logger.debug("apply_datatype: completed casting")
#     return df


def apply_datatype(df: DataFrame, ref_df: DataFrame, keep_extra_cols: Optional[Iterable[str]] = None) -> DataFrame:
    ref_cols = ref_df.select("ColumnName", "DataType").collect()
    allowed_cols = [r["ColumnName"] for r in ref_cols]

    extras = set(keep_extra_cols or [])
    selected_cols = [c for c in df.columns if (c in allowed_cols) or (c in extras)]
    logger.debug(
        "apply_datatype: allowed_cols=%d extras=%s selected_cols=%d",
        len(allowed_cols),
        ",".join(sorted(extras)) if extras else "-",
        len(selected_cols),
    )

    df = df.select(*selected_cols)

    numeric_types = {"double", "float", "integer"}  # keep your original behavior

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

    logger.debug("apply_datatype: completed casting")
    return df


def apply_roundoff_to_numeric_columns(df: DataFrame, ref_df: DataFrame, log: bool = True) -> DataFrame:
    """Round numeric columns according to 'roundoff' in reference."""
    round_rules = ref_df.filter(F.col("roundoff").isNotNull()).select("ColumnName", "roundoff").collect()
    logger.debug("apply_roundoff_to_numeric_columns: rules=%d", len(round_rules))

    for row in round_rules:
        col_name = row["ColumnName"]
        scale = int(row["roundoff"])
        if col_name in df.columns:
            df = df.withColumn(col_name, F.round(F.col(col_name).cast("double"), scale))
            if log:
                logger.debug("apply_roundoff_to_numeric_columns: rounded '%s' to %d", col_name, scale)
    return df


# -----------------------
# Timestamp parsing
# -----------------------
def fix_and_parse_timestamp(df: DataFrame, colname: str = "time", use_try_to_timestamp: bool = False) -> DataFrame:
    """Parse timestamp column to TimestampType (truncated to seconds)."""
    logger.debug("fix_and_parse_timestamp: column=%s, try=%s", colname, use_try_to_timestamp)
    cleaned_col = F.trim(F.regexp_replace(F.col(colname), '"', ""))

    if use_try_to_timestamp:
        parsed_col = F.date_trunc(
            "second",
            F.expr(f"try_to_timestamp({colname}, 'dd.MM.yyyy HH:mm:ss.SSSSSS')")
        )
    else:
        parsed_col = F.date_trunc("second", F.to_timestamp(cleaned_col, "dd.MM.yyyy HH:mm:ss.SSSSSS"))

    out = df.withColumn(colname, parsed_col).orderBy(colname)
    logger.debug("fix_and_parse_timestamp: completed")
    return out


# -----------------------
# Filter out of range rows
# -----------------------
def filter_out_of_range_rows(
    df: DataFrame,
    ref_df: DataFrame,
) -> DataFrame:
    """Filter rows with numeric values outside [Min, Max] in reference."""
    bounds = (
        ref_df
        .select("ColumnName", "Min", "Max")
        .filter(F.col("Min").isNotNull() | F.col("Max").isNotNull())
        .collect()
    )
    logger.debug("filter_out_of_range_rows: constraints=%d", len(bounds))

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

    out = df.filter(keep_condition)
    logger.debug("filter_out_of_range_rows: completed")
    return out


# def drop_min_max_cols(df):
#     pattern = re.compile(r".*_(min|max)(_\d+)?$")
#     cols_to_keep = [c for c in df.columns if not pattern.match(c)]
#     return df.select(*cols_to_keep)