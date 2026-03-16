# pipelines/ref_ingest.py

import importlib
from pyspark.sql import SparkSession

from core import cleaning_utils as cleaningutils; importlib.reload(cleaningutils)
import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)


def load_reference_tables(
    spark: SparkSession,
    *,
    joint_colors_excel_path: str,
    wps_excel_path: str,
    weld_features_csv_path: str,
    joint_colors_path: str,
    wps_path: str,
    weld_features_path: str
):
    logger.info("Loading reference: joint_colors")
    df = cleaningutils.excel_path_to_spark_df(spark, joint_colors_excel_path)
    df.write.format("delta").mode("overwrite").save(joint_colors_path)

    logger.info("Loading reference: WPS")
    df = cleaningutils.excel_path_to_spark_df(spark, wps_excel_path)
    df.write.format("delta").mode("overwrite").save(wps_path)

    logger.info("Loading reference: weld_features")
    df = spark.read.option("header", "true").csv(weld_features_csv_path)
    df.write.format("delta").mode("overwrite").save(weld_features_path)

    logger.info("Reference table ingestion completed.")