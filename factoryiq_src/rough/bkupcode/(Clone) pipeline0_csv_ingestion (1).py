"""
Pipeline 0: Raw Robot File Ingestion (Stage 0 - Bronze)

Reads semi-structured robot CSV files from ADLS/DBFS where:
    - Row 1: metadata
    - Row 2: header
    - Row 3: metadata
    - Row 4+: data rows

Performs:
    - Header extraction
    - Column sanitization (Delta-safe)
    - Duplicate column handling

Writes clean output to Bronze (Delta format).
"""

from pyspark.sql import SparkSession

from core.ingestion_utils import read_and_prepare
from core.io_utils import write_delta


def run(
    spark: SparkSession,
    input_path: str,
    output_path: str
):
    """
    Stage 0 execution function.
    """

    print("--------------------------------------------------")
    print("🚀 Starting Stage 0 - Raw Ingestion")
    print("--------------------------------------------------")

    print(f"📥 Reading robot file from: {input_path}")
    df = read_and_prepare(spark, input_path)

    print(f"📊 Rows read: {df.count()}")
    print(f"📑 Columns detected: {len(df.columns)}")

    print(f"💾 Writing Bronze Delta table to: {output_path}")
    write_delta(df, output_path, mode="overwrite")

    print("✅ Stage 0 completed successfully.")
    print("--------------------------------------------------")