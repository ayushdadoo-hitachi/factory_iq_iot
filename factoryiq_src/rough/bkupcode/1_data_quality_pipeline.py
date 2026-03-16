# Databricks notebook source
# MAGIC %run ./common/data_utils

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %run ./global/global_functions

# COMMAND ----------

ref_df = read_data_from_tables_inabfss(spark,REQUIRED_WELD_FEATURES_TABLE_PATH)

# COMMAND ----------

# DBTITLE 1,Import all libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# COMMAND ----------

from pyspark.sql.types import DoubleType, FloatType, IntegerType, StringType, BooleanType, LongType, TimestampType

type_map = {
    "double": DoubleType(),
    "float": FloatType(),
    "integer": IntegerType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "long": LongType(),
    "timestamp": TimestampType()
}

# COMMAND ----------

# MAGIC %md
# MAGIC Filters the input DataFrame to keep only columns listed in a CSV file.
# MAGIC     The CSV must have a header row; all rows below are considered allowed columns.
# MAGIC     Drops any column from df not in the reference list.
# MAGIC     Compatible with serverless compute (no RDD usage).

# COMMAND ----------

def filter_columns_using_reference_csv(df: DataFrame):
    first_col_name = ref_df.columns[0] # Take the first column name
    ref_list = [row[first_col_name] for row in ref_df.select(first_col_name).collect()] # Collect all values from the first column into a Python list
    columns_to_keep = [c for c in df.columns if c in ref_list]
    df_filtered = df.select(columns_to_keep) # Create a new filtered DataFrame
    print(f"Original DF columns: {len(df.columns)}")
    print(f"Filtered DF columns: {len(df_filtered.columns)}")
    return df_filtered


# COMMAND ----------

from pyspark.sql import functions as F

def apply_comma_to_dot_string_only(df):
    """
    Uses ref_df to identify columns with comma_to_dot = 'yes'
    and replaces ',' with '.' while keeping the column as STRING.
    """

    # Collect column names safely (DataFrame API only)
    columns_to_fix = [
        row["ColumnName"]
        for row in ref_df
            .filter(F.col("comma_to_dot") == "yes")
            .select("ColumnName")
            .collect()
    ]

    for col_name in columns_to_fix:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.regexp_replace(F.col(col_name), ",", ".")
            )

    return df


# COMMAND ----------

def apply_datatype(df: DataFrame):
    ref = ref_df
    ref = ref.select("ColumnName", "DataType")     # Expecting columns: ColumnName, DataType
    allowed_cols = [r["ColumnName"] for r in ref.collect()]     # List of allowed columns
    df = df.select([c for c in df.columns if c in allowed_cols])     # ---- Step 2: Filter DF to keep only reference columns ----
    numeric_types = {"double", "float", "integer"}     # ---- Step 3: Clean numeric columns (remove comma only) ----

    for row in ref.collect():
        col_name = row["ColumnName"]
        dtype = row["DataType"].lower().strip()

        if col_name in df.columns and dtype in numeric_types:
            # Remove commas ONLY
            df = df.withColumn(col_name, regexp_replace(col(col_name), ",", ""))

            # Convert empty → NULL
            df = df.withColumn(col_name,
                when(trim(col(col_name)) == "", None)
                .otherwise(col(col_name))
            )

    # ---- Step 4: Apply type casting as per reference ----
    # Mapping
    
    for row in ref.collect():
        col_name = row["ColumnName"]
        dtype = row["DataType"].lower().strip()

        if col_name in df.columns:
            if dtype in type_map:
                df = df.withColumn(col_name, col(col_name).cast(type_map[dtype]))
            else:
                print(f"⚠ Unknown datatype in reference: {dtype} (column {col_name})")
    return df


# COMMAND ----------

def apply_roundoff_to_numeric_columns(df):
    """
    Rounds numeric columns based on 'roundoff' specified in ref_df.
    Keeps columns NUMERIC (DoubleType).
    """

    round_rules = (
        ref_df
        .filter(F.col("roundoff").isNotNull())
        .select("columnname", "roundoff")
        .collect()
    )

    for row in round_rules:
        col_name = row["columnname"]
        scale = int(row["roundoff"])

        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.round(F.col(col_name).cast("double"), scale)
            )

    return df


# COMMAND ----------

from pyspark.sql import functions as F

def fix_and_parse_timestamp(df, colname="time", use_try_to_timestamp=False):
    # Remove quotes and trim
    df = df.withColumn(colname, F.trim(F.regexp_replace(F.col(colname), '"', "")))

    if use_try_to_timestamp:
        # Parse with try_to_timestamp, keeping only seconds
        df = df.withColumn(
            colname,
            F.date_trunc("second", F.expr(f"try_to_timestamp({colname}, 'dd.MM.yyyy HH:mm:ss.SSSSSS')"))
        )
    else:
        # Parse normally, keeping only seconds
        df = df.withColumn(
            colname,
            F.date_trunc("second", F.to_timestamp(F.col(colname), "dd.MM.yyyy HH:mm:ss.SSSSSS"))
        )

    return df.orderBy(colname)


# COMMAND ----------

from pyspark.sql import functions as F

def filter_valid_rows(df, time_col, run_col):
    return (
        df.filter(
            (F.trim(F.col(time_col)).isNotNull()) &
            (F.trim(F.col(time_col)) != "-") &
            (F.trim(F.col(time_col)) != "null") &
            (F.trim(F.col(run_col)).isNotNull()) &
            (F.trim(F.col(run_col)) != "") &
            (F.trim(F.col(run_col)) != "null")
        )
    )


# COMMAND ----------

# df_raw = spark.table(STAGE0_TABLE)
df_raw = read_data_from_tables_inabfss(spark,STAGE0_TABLE)
# display(df_raw.limit(10))
df = filter_columns_using_reference_csv(df_raw)
df = remove_quotes_all_columns(df)
df = fix_broken_scientific_notation(df)
df = collapse_multiple_dots(df)
df = normalize_nan_like_strings(df)
df = normalize_empty_strings(df)
df = apply_comma_to_dot_string_only(df)



# COMMAND ----------

int_columns = [
    row["ColumnName"]
    for row in ref_df
        .filter(F.lower(F.col("DataType")) == "integer")
        .select("ColumnName")
        .collect()
]

print("Integer columns:", int_columns)

# COMMAND ----------

from pyspark.sql.functions import col, trim

def find_invalid_int_values(df, int_columns, sample_limit=20):
    invalid_pattern = r'^[+-]?[0-9]+$'  # valid integer

    for c in int_columns:
        if c not in df.columns:
            continue

        bad_df = (
            df
            .filter(
                (trim(col(c)).isNotNull()) &
                (trim(col(c)) != "") &
                (~trim(col(c)).rlike(invalid_pattern))
            )
            .select(c)
            .distinct()
            .limit(sample_limit)
        )

        if bad_df.count() > 0:
            print(f"\n❌ Invalid INT values found in column: {c}")
            display(bad_df)


# COMMAND ----------

# df_stage1 = clean_numeric_columns_first_stage(df_stage1)

find_invalid_int_values(df, int_columns) # Detect problematic values (audit only)
df, removed_rows = drop_rows_with_invalid_int_values(df, int_columns) # Drop bad rows completely
print(removed_rows)

df = apply_datatype(df)
# for f in df.schema.fields:
        # print(f.name, "→", f.dataType)
df = apply_roundoff_to_numeric_columns(df)
# for f in df.schema.fields:
        # print(f.name, "→", f.dataType)
df = fix_and_parse_timestamp(df, use_try_to_timestamp=True)
# print(df.count())
df = filter_valid_rows(df, time_col=TIME_COL, run_col=TECHDEV_ISRUNNING_COL)
# print(df.count())
write_df_to_abfss(df,STAGE1_TABLE) 
# df.write.format("delta").mode("overwrite").saveAsTable(STAGE1_TABLE)

# COMMAND ----------

# display(df_stage1.limit(1000))

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage1;

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

df = read_data_from_tables_inabfss(spark,STAGE1_TABLE)
display(df.limit(10))

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC %pip install pyyaml
# MAGIC %pip install fsspec adlfs openpyxl