import numpy as np

TCP_X_COL = "tcp_x"
TCP_Y_COL = "tcp_y"
TCP_Z_COL = "tcp_z"


def compute_zoom_bounds(pdf, lower=0.05, upper=0.95):
    return {
        "x": (pdf[TCP_X_COL].quantile(lower), pdf[TCP_X_COL].quantile(upper)),
        "y": (pdf[TCP_Y_COL].quantile(lower), pdf[TCP_Y_COL].quantile(upper)),
        "z": (pdf[TCP_Z_COL].quantile(lower), pdf[TCP_Z_COL].quantile(upper)),
    }