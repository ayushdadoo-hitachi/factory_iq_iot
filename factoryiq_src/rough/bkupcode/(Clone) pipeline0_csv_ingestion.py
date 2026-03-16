"""
Pipeline 0: CSV Ingestion

Reads raw CSV data from ADLS (or DBFS),
performs basic normalization,
and writes it as Delta to the Bronze layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.io_utils import read_csv, write_delta

def normalize_columns(df):
    """
    Standardize column names:
    - lowercase
    - replace spaces with underscore
    - remove special characters
    """
    for col in df.columns:
        new_col = (
            col.strip()
               .lower()
               .replace(" ", "_")
               .replace("-", "_")
               .replace(".", "_")
        )
        df = df.withColumnRenamed(col, new_col)
    return df


def run(
    spark: SparkSession,
    input_path: str,
    output_path: str
):
    """
    Main execution function for CSV ingestion pipeline.
    """

    print(f"📥 Reading raw CSV from: {input_path}")
    df = read_csv(spark, input_path)

    print("🧹 Normalizing column names...")
    df = normalize_columns(df)

    # print(f"💾 Writing Delta output to: {output_path}")
    # write_delta(df, output_path, mode="overwrite")

    print("✅ Pipeline 0 completed successfully.")




# def run(
#     spark: SparkSession,
#     input_path: str,
#     output_path: str
# ):
#     print(f"📥 Reading raw CSV from: {input_path}")
#     print(f"💾 Writing Delta output to: {output_path}")
    