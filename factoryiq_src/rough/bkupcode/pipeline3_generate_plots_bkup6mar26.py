"""
Pipeline 3 – TCP 3D Plot Generation

Responsibilities:
- Read Stage5 enriched Delta table
- Identify active task segments
- Convert each segment to Pandas (controlled scope)
- Generate 3D TCP plots
- Persist images to ADLS

NO enrichment logic here.
"""
import importlib
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


import visualization.tcp_plot as vtp
import visualization.joint_color as vjc

importlib.reload(vtp)
importlib.reload(vjc)

from core.io_utils import read_delta
from visualization.tcp_plot import plot_tcp_3d_refined
from visualization.zoom import compute_zoom_bounds
from visualization.joint_color import load_joint_color_map
from visualization.joint_annotations import label_joints_minimal
from visualization.joint_ribbon import add_joint_color_ribbon_vertical

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run_pipeline3_generate_plots(
    spark: SparkSession,
    input_path: str,
    image_output_basepath: str,
    joint_colors_path: str,
    robot_name: str,
    welding_active_col: str = "arcseam_isactive",
    activetask_col: str = "d_activetask_f"
):
    """
    Orchestrates generation of TCP 3D plots per active task.
    """

    logger.info("--------------------------------------------------")
    logger.info("Starting Stage 3 – Plot Generation")
    logger.info("--------------------------------------------------")

    logger.info("📥 Reading Stage2 enriched data")
    df = read_delta(spark, input_path)

    logger.info("🔍 Extracting active task list")

    JOINT_COLOR_MAP = load_joint_color_map(spark, joint_colors_path)

    logger.info(JOINT_COLOR_MAP)

    active_task_list = [
        int(row[activetask_col])
        for row in (
            df.filter(F.col(activetask_col) != 0)
              .select(activetask_col)
              .distinct()
              .orderBy(activetask_col)
              .collect()
        )
    ]

    logger.info(f"Found {len(active_task_list)} active task segments")

    no_data_tasks = []

    for task_id in active_task_list:

        logger.info(f"Processing active task: {task_id}")

        task_df = (
            df.filter(F.col(activetask_col) == task_id)
              .filter(F.col(welding_active_col) == 1)
        )

        if not task_df.take(1):
            no_data_tasks.append(task_id)
            continue

        # Convert only this partition to Pandas
        pdf = task_df.toPandas()

        if pdf.empty:
            no_data_tasks.append(task_id)
            continue

        zoom_bounds = compute_zoom_bounds(pdf)

        output_name = f"tcp_plot_{robot_name}_{task_id}.png"

        plot_tcp_3d_refined(
            pdf=pdf,
            joint_col="wps_joint_id",   # adjust to your column
            joint_color_map=JOINT_COLOR_MAP,
            title="3D TCP plot by joint ID",
            output_name=output_name,
            zoom_bounds=zoom_bounds,
            image_output_basepath=image_output_basepath,
        )

    if no_data_tasks:
        logger.warning(f"No data to plot for tasks: {no_data_tasks}")

    logger.info("🎉 Pipeline 3 completed successfully")