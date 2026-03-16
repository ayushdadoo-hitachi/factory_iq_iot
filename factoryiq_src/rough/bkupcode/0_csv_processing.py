# Databricks notebook source
# DBTITLE 1,Import all libraries
# Get all lines in order using row_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, regexp_replace, trim, to_timestamp, when, round as spark_round
# from pyspark.sql.types import FloatType, TimestampType
# from pyspark.sql import DataFrame
from pyspark.sql.types import *
# import re

# COMMAND ----------

# MAGIC %run ./common/data_utils

# COMMAND ----------

# MAGIC %run ./common/common_functions

# COMMAND ----------

# MAGIC %run ./global/global_functions

# COMMAND ----------

# path = "/Volumes/factoryiq-catalog/factoryiq-schema/data-volume/igm6_29fjan_4feb.csv"
path = "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/igm6_29fjan_4feb.csv"

# COMMAND ----------

required_columns_df = read_data_from_tables_inabfss(spark,REQUIRED_STAGE0_COLUMNS_TABLE_PATH)

# COMMAND ----------

display(required_columns_df)

# COMMAND ----------

# --- REPLACE your existing read_and_prepare(...) with this version ---
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re

def read_and_prepare(path: str):
    """
    Reads the semi-colon separated file where:
      - Row 1: metadata
      - Row 2: header (semicolon-delimited; may contain invalid chars)
      - Row 3: metadata
      - Row 4+: data rows
    Produces a DF with sanitized, unique column names that are safe for Delta.
    """
    # Read raw file as lines
    raw_df = spark.read.text(path)

    # Stable row order
    df_rn = raw_df.withColumn(
        "rn",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
    )

    # ----- Header extraction -----
    header_row = df_rn.filter(F.col("rn") == 2).select("value").first()[0]

    # Remove trailing semicolon if present (your original behavior)
    header_row = header_row.rstrip(";")

    # Split header fields on semicolon
    raw_cols = header_row.split(";")

    # ----- Sanitize + de-duplicate column names -----
    invalid = re.compile(r"[ ,;{}\(\)\n\t=]+")  # characters Delta complains about

    def clean(name: str) -> str:
        n = (name or "").strip()
        # common issue in your file: trailing commas at the end of the last field
        n = n.rstrip(",")
        # replace invalid chars with underscore, collapse multiples, trim edges
        n = invalid.sub("_", n)
        n = re.sub(r"_+", "_", n).strip("_")
        return n

    seen = {}
    cols = []
    for i, c in enumerate(raw_cols):
        base = clean(c)
        if not base:
            base = f"col_{i+1}"
        k = seen.get(base, 0)
        seen[base] = k + 1
        cols.append(base if k == 0 else f"{base}_{k+1}")

    # ----- Data rows only (from row 4) -----
    data_only = df_rn.filter(F.col("rn") >= 4).select("value")

    # Split each line by semicolon once and index safely
    arr = F.split(F.col("value"), ";")

    # Use the minimum of header length and widest data row
    max_fields = data_only.select(F.max(F.size(arr))).first()[0]
    limit = min(len(cols), max_fields)

    # Build the final dataframe with sanitized/unique column aliases
    select_exprs = [arr.getItem(i).alias(cols[i]) for i in range(limit)]
    final_df = data_only.select(*select_exprs)

    return final_df

# COMMAND ----------

from pyspark.sql import functions as F

def drop_unwanted_columns(df, required_columns_df):
    """
    Serverless-safe:
    Drops columns where Remove is populated.
    Column name is taken from Columns_in_IGM2 OR Columns_in_IGM6 (first non-null).
    """

    ref_df = (
        required_columns_df
        .withColumn(
            "col_to_drop",
            F.when(
                F.col("Columns_in_IGM2").isNotNull() & (F.trim(F.col("Columns_in_IGM2")) != ""),
                F.col("Columns_in_IGM2")
            ).otherwise(F.col("Columns_in_IGM6"))
        )
        .filter(F.col("Remove").isNotNull())
        .filter(F.trim(F.col("Remove")) != "")
        .filter(F.col("col_to_drop").isNotNull())
    )

    # Collect using DataFrame API only
    cols_to_drop = [row["col_to_drop"] for row in ref_df.select("col_to_drop").collect()]

    # Drop only existing columns
    existing_cols_to_drop = [c for c in cols_to_drop if c in df.columns]

    print(f"[INFO] Dropping {len(existing_cols_to_drop)} columns:")
    for c in existing_cols_to_drop:
        print(f"  - {c}")

    return df.drop(*existing_cols_to_drop)


# COMMAND ----------

from collections import Counter

def show_columns(df, title="DataFrame columns"):
    """
    Displays all columns with index and flags duplicates.
    """
    print(f"\n[DEBUG] {title}")
    print(f"Total columns: {len(df.columns)}\n")

    counts = Counter(df.columns)

    for i, c in enumerate(df.columns, start=1):
        flag = "  <-- DUPLICATE" if counts[c] > 1 else ""
        print(f"{i:03d}. {c}{flag}")

    dupes = [c for c, n in counts.items() if n > 1]
    if dupes:
        print("\n[WARNING] Duplicate column names detected:")
        for c in dupes:
            print(f"  - {c}")
    else:
        print("\n[OK] No duplicate column names detected")


# COMMAND ----------

from collections import defaultdict

def rename_duplicate_columns(df):
    """
    Renames duplicate columns by appending 2,3,4...
    Example:
      col, col, col  →  col, col2, col3
    """
    counter = defaultdict(int)
    new_cols = []

    for c in df.columns:
        counter[c] += 1
        if counter[c] == 1:
            new_cols.append(c)
        else:
            new_cols.append(f"{c}{counter[c]}")

    return df.toDF(*new_cols)


# COMMAND ----------

import re

def drop_min_max_cols(df):
    pattern = re.compile(r".*_(min|max)(_\d+)?$")
    cols_to_keep = [c for c in df.columns if not pattern.match(c)]
    return df.select(*cols_to_keep)




# COMMAND ----------


df_raw = read_and_prepare(path)
df_raw = normalize_columns(df_raw)
# df_raw.write.format("delta").mode("overwrite").save("/mnt/welddata/raw_delta")

# 🔍 optional but recommended
# show_columns(df_raw, "Before deduplication")

df_raw = rename_duplicate_columns(df_raw) # 🔥 fix duplicates
df_raw = drop_min_max_cols(df_raw)
# df_raw = drop_unwanted_columns(df_raw, required_columns_df)

# df_raw.write.format("delta").mode("overwrite").saveAsTable(STAGE0_TABLE)
write_df_to_abfss(df_raw,STAGE0_TABLE) 

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

df = read_data_from_tables_inabfss(spark,STAGE0_TABLE)
display(df.limit(35))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS workspace.welddata.stage0_igm6_29fjan_4feb;
# MAGIC DROP TABLE IF EXISTS `factoryiq-catalog`.`factoryiq-robotdata-schema`.stage0_igm6_29fjan_4feb;

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC %pip install pyyaml
# MAGIC dbutils.library.restartPython()