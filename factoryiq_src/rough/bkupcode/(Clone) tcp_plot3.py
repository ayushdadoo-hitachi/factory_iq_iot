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

from visualization.render import render_3d_plot_databricks

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
    Generate 3D TCP plot with joint-based coloring.

    Parameters
    ----------
    pdf : pandas.DataFrame
        Task-level dataframe (already filtered)
    joint_col : str
        Column containing joint IDs
    joint_color_map : dict
        {joint_id: "#hexcolor"}
    """

    logger.info(f"Generating plot: {output_name}")

    fig = plt.figure(figsize=figsize, dpi=120)
    ax = Axes3D(fig)
    fig.add_axes(ax)

    # -----------------------------
    # Sort by time
    # -----------------------------
    pdf = pdf.sort_values("time").reset_index(drop=True)

    # -----------------------------
    # Safety check
    # -----------------------------
    if joint_col not in pdf.columns:
        raise ValueError(f"{joint_col} not found in dataframe")

    # -----------------------------
    # SEGMENTED PLOTTING
    # -----------------------------
    if "der_euc_seg_id" in pdf.columns:

        segments = [seg for _, seg in pdf.groupby("der_euc_seg_id")]

        for seg in segments:

            if len(seg) < 2:
                continue

            # Plot each small segment with joint-based color
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
        # Fallback (single color if segmentation missing)
        ax.plot(
            pdf["tcp_x"],
            pdf["tcp_y"],
            pdf["tcp_z"],
            linewidth=1.8,
            alpha=0.9
        )

    # -----------------------------
    # Zoom bounds
    # -----------------------------
    if zoom_bounds:
        ax.set_xlim(zoom_bounds["x"])
        ax.set_ylim(zoom_bounds["y"])
        ax.set_zlim(zoom_bounds["z"])

    ax.set_xlabel("TCP X (mm)")
    ax.set_ylabel("TCP Y (mm)")
    ax.set_zlabel("TCP Z (mm)")
    ax.set_title(title)

    ax.view_init(elev=elev, azim=azim)

    # -----------------------------
    # Save image
    # -----------------------------
    render_3d_plot_databricks(
        fig=fig,
        filename=output_name,
        image_output_basepath=image_output_basepath
    )

    plt.close(fig)