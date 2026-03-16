from core.io_utils import read_delta

import logging
logger = logging.getLogger(__name__)

def load_joint_color_map(spark, joint_colors_table_path: str) -> dict:
    """
    Loads joint color reference table and returns:
    { joint_id: "#hexcolor" }
    """

    jc_df = read_delta(spark, joint_colors_table_path)

    joint_color_pdf = (
        jc_df
        .select("joint_id", "color")
        .dropna()
        .toPandas()
    )

    color_map = {
        int(row["joint_id"]): f"#{row['color']}"
        for _, row in joint_color_pdf.iterrows()
    }

    logger.info(f"[INFO] Loaded {len(color_map)} joint colors")

    return color_map