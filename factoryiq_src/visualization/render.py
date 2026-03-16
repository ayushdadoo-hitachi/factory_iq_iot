import logging

logger = logging.getLogger(__name__)


def render_3d_plot_databricks(fig, filename, image_output_basepath, dpi=150):

    output_path = f"{image_output_basepath}/{filename}"

    logger.info(f"Saving image to {output_path}")

    fig.savefig(
        output_path,
        dpi=dpi,
        bbox_inches="tight",
        pad_inches=0.1
    )