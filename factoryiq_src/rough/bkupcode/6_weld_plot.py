# Databricks notebook source
# MAGIC %run ./common/data_utils

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np

# COMMAND ----------

# print(IMAGE_BASEPATH)

# COMMAND ----------


from datetime import datetime

start_dt = datetime.strptime(str(DATA_START_TIME), "%Y-%m-%d %H:%M:%S")
date_str = start_dt.strftime("%Y%m%d")

tablename = f"workspace.welddata.stage3_filtered_temp_july_igm2_{date_str}"
# tablename = f"{STAGE3_WINDOW_TABLE}{'_'}{date_str}"
# df = spark.table("workspace.welddata.stage3_may_igm2")
# df = stage3_time_window(df)
# df.write.format("delta").mode("overwrite").saveAsTable(tablename)




# COMMAND ----------

pdf = spark.table(tablename).toPandas()
# output_suffix = IMAGE_WPS_SUFFIX
joint_type = WPS_JOINT
joint_col_name = f"{joint_type}_id"

# COMMAND ----------

image_output = f"tcp_plot_{joint_type}_{date_str}.png"

# COMMAND ----------

# MAGIC %skip
# MAGIC pdf = spark.table(STAGE3_WINDOW_TABLE).toPandas()
# MAGIC # output_suffix = IMAGE_WPS_SUFFIX
# MAGIC joint_type = WPS_JOINT
# MAGIC joint_col_name = f"{joint_type}_id"
# MAGIC

# COMMAND ----------

# Load joint color reference table
joint_color_pdf = (
    spark.table(JOINT_COLORS_TABLE)  # <-- your table
         .select("joint_id", "color")
         .dropna()
         .toPandas()
)

# Build lookup dict: {joint_id: "#hex"}
JOINT_COLOR_MAP = {
    int(row["joint_id"]): f"#{row['color']}"
    for _, row in joint_color_pdf.iterrows()
}


# COMMAND ----------

def persist_clean_joints_to_delta(pdf_clean):
    # -------- Step 2: Persist FULL DF (MANDATORY) --------
    spark.createDataFrame(pdf_clean) \
        .write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(STAGE3_WINDOW_CLEANJOINT_TABLE)

    print(
        f"✅ Persisted {len(pdf_clean)} rows to "
        f"{STAGE3_WINDOW_CLEANJOINT_TABLE}"
    )


# COMMAND ----------

def plot_labels(ax, pdf_clean):
    for j in pdf_clean[joint_col_name].dropna().unique():

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

import numpy as np
import pandas as pd

def label_joints_minimal(ax, pdf):
    # -------- Step 1: Clean joint column (NO row loss) --------
    pdf_clean = pdf.copy()

    pdf_clean[joint_col_name] = (
        pdf_clean[joint_col_name]
        .replace([np.inf, -np.inf], np.nan)
        .astype("Int64")   # nullable int, keeps NaNs
    )

    # persist_clean_joints_to_delta(pdf_clean)
    plot_labels(ax, pdf_clean)

    # -------- Step 3: Plot labels --------
    


# COMMAND ----------

def build_joint_color_map(joint_ids):
    colors = np.vstack([
        cm.tab20(np.linspace(0, 1, 20)),
        cm.tab20b(np.linspace(0, 1, 20)),
        cm.tab20c(np.linspace(0, 1, 20))
    ])

    def rgb_to_hex(rgb):
        return "#{:02x}{:02x}{:02x}".format(
            int(rgb[0]*255),
            int(rgb[1]*255),
            int(rgb[2]*255)
        )

    return {
        j: rgb_to_hex(colors[i][:3])
        for i, j in enumerate(sorted(joint_ids))
    }


# COMMAND ----------

def split_continuous_segments(pdf, dist_threshold=200.0):
    """
    Split TCP trajectory into continuous segments based on Euclidean distance.
    """
    coords = pdf[[TCP_X_COL, TCP_Y_COL, TCP_Z_COL]].values
    if len(coords) < 2:
        return [pdf]
    
    dists = np.linalg.norm(coords[1:] - coords[:-1], axis=1)
    break_idxs = np.where(dists > dist_threshold)[0] + 1

    segments = []
    start = 0
    for idx in break_idxs:
        segments.append(pdf.iloc[start:idx])
        start = idx
    segments.append(pdf.iloc[start:])
    return segments

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

def render_3d_plot_databricks(
    fig,
    filename=image_output,
    # base_path="/Volumes/workspace/sourcedata/sourcedatavolume",
    base_path=IMAGE_BASEPATH,
    dpi=150
):
    """
    Databricks Free Edition safe renderer using Volumes.
    """

    output_path = f"{base_path}/{filename}"

    fig.savefig(
        output_path,
        dpi=dpi,
        bbox_inches="tight",
        pad_inches=0.1
    )
    plt.close(fig)

    # displayHTML(f"""
    #     <img src="files/workspace/sourcedata/sourcedatavolume/{filename}"
    #          style="width:100%; height:auto; border:1px solid #ddd;">
    # """
    # )


# COMMAND ----------

def plot_tcp_3d_refined(pdf, joint_col, title, output_name,
    outlier_mask=None,            # backward compatible (ignored)
    zoom_bounds=None,
    point_size_main=12,
    line_width=2.2,
    dist_threshold=200.0,
    # dist_threshold=10000.0,
    elev=20,
    azim=-120,
    figsize=(32, 20)
):

    pdf = pdf.sort_values(TIME_COL).reset_index(drop=True)
    # No dynamic color map anymore
    color_map = JOINT_COLOR_MAP
    # print("inside1")

    segments = split_continuous_segments(pdf, dist_threshold)

    # 🔑 DO NOT push DPI too high (Databricks limit)
    fig = plt.figure(figsize=figsize, dpi=120)
    ax = fig.add_subplot(111, projection="3d")

    # Fill canvas properly
    ax.set_position([0.04, 0.04, 0.92, 0.92])
    # print("inside2")
    apply_zoom_and_scale_3d(ax, zoom_bounds)
    # print("inside3")
    for seg in segments:
        if len(seg) < 2:
            continue

        ax.plot(
            seg[TCP_X_COL], seg[TCP_Y_COL], seg[TCP_Z_COL],
            color="black", linewidth=line_width, alpha=0.7
        )

        ax.scatter(
        seg[TCP_X_COL], seg[TCP_Y_COL], seg[TCP_Z_COL],
        c=[color_map.get(j, "#d3d3d3") for j in seg[joint_col]],
        s=point_size_main,
        alpha=0.9
)

    # print("inside4")
    # Labels — keep readable
    ax.set_xlabel("TCP X (mm)", fontsize=10, labelpad=10)
    ax.set_ylabel("TCP Y (mm)", fontsize=10, labelpad=12)
    ax.set_zlabel("TCP Z (mm)", fontsize=10, labelpad=8)
    ax.set_title(title, fontsize=14, pad=18)

    ax.view_init(elev=elev, azim=azim)
    ax.tick_params(labelsize=8)

    label_joints_minimal(ax, pdf)
    # print("inside5")
    render_3d_plot_databricks(fig, filename=output_name, dpi=150)
    # print("inside6")
    return color_map


# COMMAND ----------

def compute_zoom_bounds(
    pdf,
    lower: float = 0.05,
    upper: float = 0.95
):
    """
    Percentile-based zoom bounds for 3D axes.
    """
    return {
        "x": (pdf[TCP_X_COL].quantile(lower), pdf[TCP_X_COL].quantile(upper)),
        "y": (pdf[TCP_Y_COL].quantile(lower), pdf[TCP_Y_COL].quantile(upper)),
        "z": (pdf[TCP_Z_COL].quantile(lower), pdf[TCP_Z_COL].quantile(upper)),
    }


# COMMAND ----------

# plot_pdf = df_with_dummy_joints[df_with_dummy_joints["dummy_joint"] > 0]
plot_pdf = pdf[pdf["der_welding_active"] == 1]

# plot_pdf = pdf


zoom_bounds = compute_zoom_bounds(plot_pdf, 0.05, 0.95)
print(zoom_bounds)

# COMMAND ----------

# MAGIC %skip
# MAGIC print(len(plot_pdf))
# MAGIC
# MAGIC print(
# MAGIC     plot_pdf[[TCP_X_COL, TCP_Y_COL, TCP_Z_COL]]
# MAGIC     .replace([np.inf, -np.inf], np.nan)
# MAGIC     .isna()
# MAGIC     .all()
# MAGIC )
# MAGIC

# COMMAND ----------

color_map = plot_tcp_3d_refined(
    plot_pdf,
    joint_col=joint_col_name,
    title=PLOT_DESC,
    output_name=image_output,
    zoom_bounds=zoom_bounds,
    dist_threshold=250
)


# COMMAND ----------

# MAGIC %skip
# MAGIC display(
# MAGIC     pd.DataFrame(
# MAGIC         [(int(k), v) for k, v in color_map.items()],
# MAGIC         columns=["joint_id", "hex_color"]
# MAGIC     ).sort_values("joint_id")
# MAGIC )

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3_clean_joint;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3;