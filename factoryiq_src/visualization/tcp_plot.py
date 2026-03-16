
"""
TCP 3D Plot Rendering (Joint-Aware Version)

Supports:
- Segmented plotting via der_euc_seg_id
- Joint-based coloring
- Zoom bounds
- Databricks image persistence
"""

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import logging
import importlib

import visualization.joint_annotations as vja
importlib.reload(vja)


from visualization.render import render_3d_plot_databricks
from visualization.joint_annotations import label_joints_minimal
from visualization.joint_ribbon import add_joint_color_ribbon_vertical

logger = logging.getLogger(__name__)


def plot_tcp_3d_refined(
    pdf,
    joint_col: str,
    joint_color_map: dict,
    title: str,
    output_name: str,
    zoom_bounds: dict,
    image_output_basepath: str,
    figsize=(20, 14),
    elev=20,
    azim=-120,
):
    """
    Generate 3D TCP plot with:
    - Segmented plotting
    - Joint-based coloring
    - In-space joint labels
    - Vertical color ribbon legend
    """

    logger.info(f"Generating plot: {output_name}")

    # --------------------------------------------------
    # Create figure + axis
    # --------------------------------------------------
    fig = plt.figure(figsize=figsize, dpi=120)
    ax = fig.add_subplot(111, projection="3d")

    # --------------------------------------------------
    # Sort by time
    # --------------------------------------------------
    pdf = pdf.sort_values("time").reset_index(drop=True)

    # --------------------------------------------------
    # Safety check
    # --------------------------------------------------
    if joint_col not in pdf.columns:
        raise ValueError(f"{joint_col} not found in dataframe")

    # --------------------------------------------------
    # SEGMENTED PLOTTING
    # --------------------------------------------------
    if "der_euc_seg_id" in pdf.columns:

        for _, seg in pdf.groupby("der_euc_seg_id"):

            if len(seg) < 2:
                continue

            for i in range(len(seg) - 1):

                joint_id = seg.iloc[i][joint_col]

                ax.plot(
                    seg["tcp_x"].iloc[i:i+2],
                    seg["tcp_y"].iloc[i:i+2],
                    seg["tcp_z"].iloc[i:i+2],
                    color=joint_color_map.get(joint_id, "#d3d3d3"),
                    linewidth=1.8,
                    alpha=0.9
                )
    else:
        # Fallback if segmentation missing
        ax.plot(
            pdf["tcp_x"],
            pdf["tcp_y"],
            pdf["tcp_z"],
            linewidth=1.8,
            alpha=0.9
        )

    # --------------------------------------------------
    # Joint labels inside 3D space
    # --------------------------------------------------
    label_joints_minimal(ax, pdf, joint_col)

    # Required before placing ribbon outside axes
    fig.canvas.draw()

    # --------------------------------------------------
    # Vertical joint color ribbon
    # --------------------------------------------------
    add_joint_color_ribbon_vertical(
        ax,
        pdf,
        joint_col,
        joint_color_map,
        fontsize=13,
        box_size=(0.018, 0.009),
        label_offset=0.006,
        row_gap=0.008,
        x_align="axes_right+pad",
        right_pad=0.035,
        y_center="axes_mid",
        bg_alpha=0.18,
        zorder=80,
        clear_previous=True
    )

    # --------------------------------------------------
    # Zoom bounds
    # --------------------------------------------------
    if zoom_bounds:
        ax.set_xlim(zoom_bounds["x"])
        ax.set_ylim(zoom_bounds["y"])
        ax.set_zlim(zoom_bounds["z"])

    # --------------------------------------------------
    # Axis labels & view
    # --------------------------------------------------
    ax.set_xlabel("TCP X (mm)", fontsize=10, labelpad=10)
    ax.set_ylabel("TCP Y (mm)", fontsize=10, labelpad=12)
    ax.set_zlabel("TCP Z (mm)", fontsize=10, labelpad=8)
    ax.set_title(title, fontsize=14, pad=18)

    ax.view_init(elev=elev, azim=azim)
    ax.tick_params(labelsize=8)

    # --------------------------------------------------
    # Save image
    # --------------------------------------------------
    render_3d_plot_databricks(
        fig=fig,
        filename=output_name,
        image_output_basepath=image_output_basepath
    )

    plt.close(fig)