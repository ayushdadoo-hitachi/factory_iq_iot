import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import logging

from visualization.render import render_3d_plot_databricks

logger = logging.getLogger(__name__)


def plot_tcp_3d_refined(
    pdf,
    title: str,
    output_name: str,
    zoom_bounds: dict,
    image_output_basepath: str,
    figsize=(20, 14),
    elev=20,
    azim=-120,
):

    logger.info(f"Generating plot: {output_name}")

    fig = plt.figure(figsize=figsize, dpi=120)
    ax = Axes3D(fig)
    fig.add_axes(ax)

    pdf = pdf.sort_values("time").reset_index(drop=True)

    ax.plot(
        pdf["tcp_x"],
        pdf["tcp_y"],
        pdf["tcp_z"],
        linewidth=1.8,
        alpha=0.9
    )

    if zoom_bounds:
        ax.set_xlim(zoom_bounds["x"])
        ax.set_ylim(zoom_bounds["y"])
        ax.set_zlim(zoom_bounds["z"])

    ax.set_xlabel("TCP X (mm)")
    ax.set_ylabel("TCP Y (mm)")
    ax.set_zlabel("TCP Z (mm)")
    ax.set_title(title)

    ax.view_init(elev=elev, azim=azim)

    render_3d_plot_databricks(
        fig=fig,
        filename=output_name,
        image_output_basepath=image_output_basepath
    )

    plt.close(fig)