"""
Pipeline: Reference Data Load

Loads reference Excel and CSV files,
converts to Spark,
writes to Delta.
"""

from pyspark.sql import SparkSession
from core.io_utils import write_delta
from core.reference_utils import pandas_excel_to_spark
from core.ingestion_utils import read_and_prepare   # if needed
from core.io_utils import read_csv


def run(
    spark: SparkSession,
    joint_colors_pdf,
    required_stage0_pdf,
    wps_pdf,
    reference_csv_path: str,
    output_paths: dict
):

    print("🚀 Starting Reference Data Load")

    joint_colors_df = pandas_excel_to_spark(spark, joint_colors_pdf)
    required_stage0_columns_df = pandas_excel_to_spark(spark, required_stage0_pdf)
    wps_df_a = pandas_excel_to_spark(spark, wps_pdf)

    ref_df = read_csv(spark, reference_csv_path)

    writes = [
        (joint_colors_df, output_paths["joint_colors"]),
        (required_stage0_columns_df, output_paths["required_stage0"]),
        (wps_df_a, output_paths["wps"]),
        (ref_df, output_paths["required_weld_features"]),
    ]

    for df, out_path in writes:
        print(f"💾 Writing Delta (overwrite): {out_path}")
        write_delta(df, out_path)

    print("✅ Reference Data Load Completed")