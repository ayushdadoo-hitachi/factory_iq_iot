
import logging
import numpy as np
logger = logging.getLogger(__name__)

# =========================
# Joint label configuration
# =========================

JOINT_LABEL_XY_OFFSET   = 40    # mm (sideways offset)
JOINT_LABEL_Z_OFFSET    = 15    # mm (vertical lift)

JOINT_LABEL_FONT_SIZE   = 8
JOINT_LABEL_COLOR       = "black"

ARROW_COLOR             = "black"
ARROW_LINEWIDTH         = 1.6
ARROW_HEAD_SIZE         = 12    # mutation_scale
ARROW_ALPHA             = 0.9

TCP_X_COL = "tcp_x"
TCP_Y_COL = "tcp_y"
TCP_Z_COL = "tcp_z"


def compute_leader_offset(joint_points, offset_xy=JOINT_LABEL_XY_OFFSET):
    dx = (
        joint_points[TCP_X_COL].iloc[-1]
        - joint_points[TCP_X_COL].iloc[0]
    )
    dy = (
        joint_points[TCP_Y_COL].iloc[-1]
        - joint_points[TCP_Y_COL].iloc[0]
    )

    norm = np.sqrt(dx * dx + dy * dy) + 1e-6

    # perpendicular direction
    ox = -dy / norm * offset_xy
    oy =  dx / norm * offset_xy

    return ox, oy


def plot_labels_with_leaders(ax, pdf_clean, joint_col):
    for j in pdf_clean[joint_col].dropna().unique():

        joint_points = pdf_clean[pdf_clean[joint_col] == j]
        if joint_points.empty:
            continue

        # --- Use midpoint instead of mean ---
        mid_idx = len(joint_points) // 2
        row = joint_points.iloc[mid_idx]

        x_c = float(row[TCP_X_COL])
        y_c = float(row[TCP_Y_COL])
        z_c = float(row[TCP_Z_COL])

        if np.isnan(x_c) or np.isnan(y_c) or np.isnan(z_c):
            continue

        # --- Compute offset ---
        ox, oy = compute_leader_offset(
            joint_points,
            offset_xy=JOINT_LABEL_XY_OFFSET
        )

        z_label = z_c + JOINT_LABEL_Z_OFFSET

        # --- Draw leader line ---
        ax.plot(
            [x_c, x_c + ox],
            [y_c, y_c + oy],
            [z_c, z_label],
            color="black",
            linewidth=0.8,
            alpha=0.8
        )

        # --- Draw label ---
        ax.text(
            x_c + ox,
            y_c + oy,
            z_label,
            str(int(j)),
            color="black",
            fontsize=8,
            ha="center",
            va="bottom"
        )

def label_joints_minimal(ax, pdf, joint_col):

    logger.info(f"inside label joints minimal")
    # -------- Step 1: Clean joint column (NO row loss) --------
    pdf_clean = pdf.copy()

    # pdf_clean[joint_col_name] = (
    #     pdf_clean[joint_col_name]
    #     .replace([np.inf, -np.inf], np.nan)
    #     .astype("Int64")   # nullable int, keeps NaNs
    # )

    pdf_clean[joint_col] = (
    pdf_clean[joint_col]
    .replace([np.inf, -np.inf], np.nan)
    .astype("Int64")
)

    # persist_clean_joints_to_delta(pdf_clean)
    # plot_labels(ax, pdf_clean)
    plot_labels_with_leaders(ax, pdf_clean,joint_col)
    # plot_labels_with_arrow_leaders(ax, pdf_clean)

    # -------- Step 3: Plot labels --------
    


