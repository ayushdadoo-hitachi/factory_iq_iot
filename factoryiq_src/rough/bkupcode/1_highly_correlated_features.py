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

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StringType, DoubleType

def convert_all_possible_numeric_columns(df):
    """
    Converts all string columns (except 'time') that contain numeric values
    with mixed decimal separators into DoubleType.
    
    The 'time' column is excluded.
    """
    
    for field in df.schema.fields:
        column_name = field.name
        
        # Skip time column
        if column_name.lower() == "time":
            continue
        
        # Process only string columns
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                column_name,
                regexp_replace(col(column_name), ",", ".").cast(DoubleType())
            )
    
    return df


# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.functions import col, regexp_replace
# MAGIC from pyspark.sql.types import StringType, DoubleType
# MAGIC
# MAGIC def convert_all_possible_numeric_columns(df):
# MAGIC     """
# MAGIC     Automatically converts all columns that look numeric 
# MAGIC     (including those with comma decimal separators) into DoubleType.
# MAGIC     
# MAGIC     Non-numeric columns remain unchanged.
# MAGIC     """
# MAGIC     
# MAGIC     for field in df.schema.fields:
# MAGIC         column_name = field.name
# MAGIC         
# MAGIC         # Only process string columns (likely to contain mixed decimals)
# MAGIC         if isinstance(field.dataType, StringType):
# MAGIC             
# MAGIC             # Replace comma with dot and attempt cast
# MAGIC             df = df.withColumn(
# MAGIC                 column_name,
# MAGIC                 regexp_replace(col(column_name), ",", ".").cast(DoubleType())
# MAGIC             )
# MAGIC     
# MAGIC     return df
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, round
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType

def round_numeric_columns_except_time(df):
    """
    Rounds all numeric columns (except 'time') to 3 decimal places.
    Columns with fewer decimals remain unchanged.
    Non-numeric columns are untouched.
    """
    
    for field in df.schema.fields:
        column_name = field.name
        
        # Skip time column
        if column_name.lower() == "time":
            continue
        
        # Apply only to numeric columns
        if isinstance(field.dataType, (DoubleType, FloatType)):
            df = df.withColumn(
                column_name,
                round(col(column_name), 3)
            )
    
    return df


# COMMAND ----------

from pyspark.sql.functions import col, avg, stddev, sum as spark_sum
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType

def remove_highly_correlated_features(df, threshold=0.999):

    # 1️⃣ Select numeric columns except time
    numeric_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (DoubleType, FloatType, IntegerType, LongType))
        and f.name.lower() != "time"
    ]

    # 2️⃣ Compute mean and stddev once
    stats = df.select(
        *[avg(c).alias(f"{c}_mean") for c in numeric_cols],
        *[stddev(c).alias(f"{c}_std") for c in numeric_cols]
    ).collect()[0]

    means = {c: stats[f"{c}_mean"] for c in numeric_cols}
    stds = {c: stats[f"{c}_std"] for c in numeric_cols}

    # 🚨 3️⃣ Remove zero-variance columns FIRST
    zero_variance_cols = [c for c in numeric_cols if stds[c] in (None, 0)]

    if zero_variance_cols:
        print("Dropping zero variance columns:", zero_variance_cols)
        df = df.drop(*zero_variance_cols)

    # Update numeric columns after drop
    numeric_cols = [c for c in numeric_cols if c not in zero_variance_cols]

    # 4️⃣ Build covariance expressions
    exprs = []
    pairs = []

    for i in range(len(numeric_cols)):
        for j in range(i):
            c1 = numeric_cols[i]
            c2 = numeric_cols[j]
            exprs.append(
                spark_sum((col(c1)-means[c1])*(col(c2)-means[c2]))
                .alias(f"{c1}__{c2}")
            )
            pairs.append((c1, c2))

    if not exprs:
        return df

    cov_values = df.select(exprs).collect()[0]

    # 5️⃣ Decide correlated columns
    to_drop = set()

    for idx, (c1, c2) in enumerate(pairs):
        cov = cov_values[idx]
        if cov is None:
            continue

        denom = stds[c1] * stds[c2]
        if denom == 0:
            continue

        corr = cov / denom

        if abs(corr) > threshold:
            to_drop.add(c1)

    df_reduced = df.drop(*to_drop)

    print(f"Removed {len(to_drop)} highly correlated columns")
    return df_reduced


# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType
# MAGIC
# MAGIC def remove_highly_correlated_features(df, threshold=0.95):
# MAGIC     """
# MAGIC     Removes highly correlated numeric columns using Spark SQL correlation.
# MAGIC     Works in restricted Databricks clusters.
# MAGIC     """
# MAGIC     
# MAGIC     # 1️⃣ Select numeric columns except time
# MAGIC     numeric_cols = [
# MAGIC         f.name for f in df.schema.fields
# MAGIC         if isinstance(f.dataType, (DoubleType, FloatType, IntegerType, LongType))
# MAGIC         and f.name.lower() != "time"
# MAGIC     ]
# MAGIC     
# MAGIC     to_drop = set()
# MAGIC     
# MAGIC     # 2️⃣ Compute pairwise correlations
# MAGIC     for i in range(len(numeric_cols)):
# MAGIC         col1 = numeric_cols[i]
# MAGIC         
# MAGIC         for j in range(i):
# MAGIC             col2 = numeric_cols[j]
# MAGIC             
# MAGIC             # Skip if already marked
# MAGIC             if col1 in to_drop:
# MAGIC                 continue
# MAGIC             
# MAGIC             corr_value = df.stat.corr(col1, col2)
# MAGIC             
# MAGIC             if corr_value is not None and abs(corr_value) > threshold:
# MAGIC                 to_drop.add(col1)
# MAGIC     
# MAGIC     # 3️⃣ Drop correlated columns
# MAGIC     df_reduced = df.drop(*to_drop)
# MAGIC     
# MAGIC     print(f"Removed {len(to_drop)} highly correlated columns")
# MAGIC     print("Dropped columns:", to_drop)
# MAGIC     
# MAGIC     return df_reduced
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Filters the input DataFrame to keep only columns listed in a CSV file.
# MAGIC     The CSV must have a header row; all rows below are considered allowed columns.
# MAGIC     Drops any column from df not in the reference list.
# MAGIC     Compatible with serverless compute (no RDD usage).

# COMMAND ----------

STAGEX = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/robot_data_tables/stageX"

# COMMAND ----------

# df_raw = spark.table(STAGE0_TABLE)
df_raw = read_data_from_tables_inabfss(spark,STAGE0_TABLE)
# display(df_raw.limit(5))
df = remove_quotes_all_columns(df_raw)
df = fix_broken_scientific_notation(df)
df = collapse_multiple_dots(df)
df = normalize_nan_like_strings(df)
df = normalize_empty_strings(df)
# df = apply_comma_to_dot_string_only(df)
df = convert_all_possible_numeric_columns(df)
df = round_numeric_columns_except_time(df)
write_df_to_abfss(df,STAGEX)
display(df.limit(5))



# COMMAND ----------

from pyspark.sql.functions import col

df = read_data_from_tables_inabfss(spark, STAGEX)

# Keep only active welding rows
df_active = df.filter(col("arcseam_isactive") == 1)

df_reduced = remove_highly_correlated_features(df_active)

display(df_reduced.limit(5))


# COMMAND ----------

df = read_data_from_tables_inabfss(spark,STAGEX)
# display(df.limit(5))
print(len(df.columns))

df = remove_highly_correlated_features(df)
display(df.limit(5))
df.printSchema()
print(len(df.columns))


# COMMAND ----------

# df = remove_highly_correlated_features(df)
# display(df.limit(5))

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