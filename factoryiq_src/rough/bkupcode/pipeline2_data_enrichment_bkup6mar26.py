"""
Stage 2 Data Enrichment Pipeline
- Reads Stage 3 table
- Applies segmentation
- Applies WPS mapping
- Applies movement features
- Writes Stage 4 table
"""
import importlib
import logging
from pyspark.sql import SparkSession, DataFrame

from core.io_utils import read_delta, write_delta

import enrichments.machine_state as ems
import enrichments.task_features as etf
import enrichments.segmentation as es
import enrichments.movement_features as emf
import enrichments.cleanup as ecu

importlib.reload(ems)
importlib.reload(etf)
importlib.reload(es)
importlib.reload(emf)
importlib.reload(ecu)

from enrichments.machine_state import add_der_machine_state, add_welding_active
from enrichments.segmentation import (
    add_atask_segment_id,
    add_mstepnumber_segment_id,
    add_d_activetask_f,
    add_d_step_major_f,
)
from enrichments.wps_mapping import add_wps_joint_id
from enrichments.task_features import extract_active_task_num, add_step_major
from enrichments.cleanup import drop_intermediate_columns

from enrichments.movement_features import (
    compute_der_euc_distance_spark,
    compute_der_bigmove_spark,
    add_der_euc_seg_id
)

logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)


def run_pipeline2(
    spark: SparkSession,
    input_path: str,
    wps_ref_path: str,
    tps500current: str,
    techdevisrunning: str,
    output_path: str,
) -> DataFrame:
    
    logger.info("--------------------------------------------------")
    logger.info("Starting Stage 2 – Data Enrichment")
    logger.info("--------------------------------------------------")

    logger.info("📥 Reading Stage 1 data")
    df = read_delta(spark, input_path)

    logger.info("📥 Reading WPS reference")
    wps_ref_df = read_delta(spark, wps_ref_path)

    logger.info("📥 extracting active tasks")
    df = extract_active_task_num(df)

    logger.info("📥 extracting step major number")
    df = add_step_major(df)

    logger.info("📥 adding machine state")
    df = add_der_machine_state(df,tps500current,techdevisrunning)
    df = add_welding_active(df)

    logger.info("⚙ Applying segmentation")
    df = add_atask_segment_id(df)
    df = add_mstepnumber_segment_id(df)
    df = add_d_activetask_f(df)
    df = add_d_step_major_f(df)

    logger.info("🔗 Applying WPS mapping")
    df = add_wps_joint_id(df, wps_ref_df)

    logger.info("📐 Computing movement features")
    df = compute_der_euc_distance_spark(df)
    df = compute_der_bigmove_spark(df, threshold=150.0)
    df = add_der_euc_seg_id(df)

    logger.info("📐 dropping intermideiate columns")
    df = drop_intermediate_columns(df)

    logger.info("📤 Writing Stage 2")
    write_delta(df, output_path)

    logger.info("🎉 Stage 2 completed")

    return df