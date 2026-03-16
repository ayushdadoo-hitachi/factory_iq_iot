# Databricks notebook source
import yaml
import os

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
import re

# COMMAND ----------

def normalize_columns(df: DataFrame) -> DataFrame:
    def normalize(col_name: str) -> str:
        # Lowercase
        col = col_name.lower()
        # Replace space, hyphen, dot, slash with underscore
        col = re.sub(r"[ \-\.\/]+", "_", col)
        # Remove multiple underscores
        col = re.sub(r"_+", "_", col)
        # Strip leading/trailing underscores
        col = col.strip("_")
        return col

    # Rename all columns in one go
    new_cols = [normalize(c) for c in df.columns]
    return df.toDF(*new_cols)

# COMMAND ----------

def plot_labels(ax, pdf_clean):
    for j in pdf_clean[joint_col_name].dropna().unique():
        # print("joint_id: ", j)
        joint_points = pdf_clean.loc[
            pdf_clean[joint_col_name] == j
        ]

        if joint_points.empty:
            continue

        x_c = pd.to_numeric(joint_points[TCP_X_COL], errors="coerce").mean()
        y_c = pd.to_numeric(joint_points[TCP_Y_COL], errors="coerce").mean()
        z_c = pd.to_numeric(joint_points[TCP_Z_COL], errors="coerce").mean()

        if np.isnan(x_c) or np.isnan(y_c) or np.isnan(z_c):
            continue

        ax.text(
            float(x_c),
            float(y_c),
            float(z_c) + float(JOINT_LABEL_Z_OFFSET),
            str(int(j)),
            color=JOINT_LABEL_COLOR,
            fontsize=JOINT_LABEL_FONT_SIZE,
            ha=JOINT_LABEL_HORIZONTAL,
            va=JOINT_LABEL_VERTICAL
        )

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def compute_der_euc_distance_spark(df_spark):
    w = Window.partitionBy("d_activetask_f").orderBy("time")
    df_spark = df_spark.withColumn("dx", F.col("tcp_x") - F.lag("tcp_x").over(w))
    df_spark = df_spark.withColumn("dy", F.col("tcp_y") - F.lag("tcp_y").over(w))
    df_spark = df_spark.withColumn("dz", F.col("tcp_z") - F.lag("tcp_z").over(w))
    df_spark = df_spark.fillna({"dx":0, "dy":0, "dz":0})
    df_spark = df_spark.withColumn("der_euc_distance", F.sqrt(F.col("dx")**2 + F.col("dy")**2 + F.col("dz")**2))
    return df_spark.drop("dx", "dy", "dz")


# COMMAND ----------

def compute_der_bigmove_spark(df_spark, dist_threshold):
    return df_spark.withColumn("der_bigmove", F.col("der_euc_distance") > F.lit(dist_threshold))

# COMMAND ----------

def label_joints_minimal(ax, pdf):
    # -------- Step 1: Clean joint column (NO row loss) --------
    pdf_clean = pdf.copy()

    pdf_clean[joint_col_name] = (
        pdf_clean[joint_col_name]
        .replace([np.inf, -np.inf], np.nan)
        .astype("Int64")   # nullable int, keeps NaNs
    )

    # persist_clean_joints_to_delta(pdf_clean)
    # plot_labels(ax, pdf_clean)
    plot_labels_with_arrow_leaders(ax, pdf_clean)

    # -------- Step 3: Plot labels --------
    


# COMMAND ----------

def add_der_euc_seg_id(
    df,
    flag_col="der_bigmove",
    output_col="der_euc_seg_id"
):
    """
    Creates Euclidean-distance-based segment id per active task.
    """

    w = (
        Window
        .partitionBy("d_activetask_f")
        .orderBy("time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    return df.withColumn(
        output_col,
        F.sum(
            F.when(F.col(flag_col) == True, 1).otherwise(0)
        ).over(w)
    )


# COMMAND ----------

# MAGIC %skip
# MAGIC def split_continuous_segments(pdf, dist_threshold=200.0):
# MAGIC     """
# MAGIC     Split TCP trajectory into continuous segments based on Euclidean distance.
# MAGIC     """
# MAGIC     coords = pdf[[TCP_X_COL, TCP_Y_COL, TCP_Z_COL]].values
# MAGIC     # print(coords)
# MAGIC     if len(coords) < 2:
# MAGIC         return [pdf]
# MAGIC     
# MAGIC     dists = np.linalg.norm(coords[1:] - coords[:-1], axis=1)
# MAGIC     # print(dists)
# MAGIC     break_idxs = np.where(dists > dist_threshold)[0] + 1
# MAGIC
# MAGIC     segments = []
# MAGIC     start = 0
# MAGIC     for idx in break_idxs:
# MAGIC         segments.append(pdf.iloc[start:idx])
# MAGIC         start = idx
# MAGIC     segments.append(pdf.iloc[start:])
# MAGIC     return segments

# COMMAND ----------

def apply_zoom_and_scale_3d(ax, zoom_bounds):
    ax.set_proj_type("ortho")  # remove perspective distortion

    if not zoom_bounds:
        return

    ax.set_xlim(zoom_bounds["x"])
    ax.set_ylim(zoom_bounds["y"])
    ax.set_zlim(zoom_bounds["z"])

    dx = zoom_bounds["x"][1] - zoom_bounds["x"][0]
    dy = zoom_bounds["y"][1] - zoom_bounds["y"][0]
    dz = zoom_bounds["z"][1] - zoom_bounds["z"][0]

    ax.set_box_aspect((dx, dy, dz))


# COMMAND ----------

def date_to_string():
    start_dt = datetime.strptime(str(DATA_START_TIME), "%Y-%m-%d %H:%M:%S")
    date_str = start_dt.strftime("%Y%m%d")
    return date_str

# COMMAND ----------

# MAGIC %skip
# MAGIC %pip install pyyaml