# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %run ./common/data_utils

# COMMAND ----------

# MAGIC %run ./common/common_functions

# COMMAND ----------

# MAGIC %run ./global/global_functions

# COMMAND ----------

# DBTITLE 1,Import all libraries
# Get all lines in order using row_number
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, regexp_replace, trim, to_timestamp, when, round as spark_round

# COMMAND ----------

df = read_data_from_tables_inabfss(spark,STAGE2_TABLE)
wps_ref_df = read_data_from_tables_inabfss(spark,WPS_TABLE_ACTUAL_PATH)
# display(wps_ref_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def add_time_diff(df, time_col="time", output_col="der_time_diff_sec"):

    # Define window spec
    w = Window.orderBy(time_col)

    # Compute time difference
    df = df.withColumn(
        output_col,
        F.col(time_col).cast("long") - F.lag(time_col).over(w).cast("long")
    )

    # Fill nulls (first row)
    df = df.fillna({output_col: 0})

    return df


# COMMAND ----------

def get_distinct_dates_from_time(pdf, time_col="time"):
    """
    Extract distinct dates from a timestamp column.

    Returns:
        List[datetime.date]
    """
    return (
        pd.to_datetime(pdf[time_col], errors="coerce")
        .dt.date
        .dropna()
        .unique()
        .tolist()
    )


# COMMAND ----------

df = add_time_diff(df)
df = extract_active_task_num(df)
# dates = get_distinct_dates_from_time(pdf)
df = add_der_machine_state(df)
df = add_welding_active(df)
df = add_step_major(df)
# display(df.limit(5))
# df.write.format("delta").mode("overwrite").saveAsTable(STAGE3_TABLE)
write_df_to_abfss(df,STAGE3_TABLE) 


# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

df = read_data_from_tables_inabfss(spark,STAGE3_TABLE)
display(df.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS STAGE3_TABLE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3_sept_igm6;