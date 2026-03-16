from matplotlib.patches import Rectangle

def add_joint_color_ribbon_vertical(
    ax,
    pdf,
    joint_col,
    color_map,
    fontsize=13,               # slightly bigger labels
    box_size=(0.018, 0.009),   # width, height in *figure fraction* (shorter height)
    label_offset=0.006,        # horizontal gap between color box and label
    row_gap=0.010,             # vertical gap between items
    x_align='axes_right+pad',  # place at the right of the axes; or pass a float (fig x)
    right_pad=0.010,           # padding from axes' right edge (figure fraction)
    y_center='axes_mid',       # vertically centered; or pass a float (fig y)
    bg_alpha=0.18,             # faint white background strip (0 to disable)
    zorder=60,
    tag="joint_ribbon_vertical",
    clear_previous=True
):
    """
    Draw a single-column (vertical) joint-color legend ribbon, centered vertically.
    Positions use figure coordinates for stability with 3D views.
    """

    # --- collect joints
    joints = pdf[joint_col].dropna().unique().tolist()
    joints = sorted(int(j) for j in joints)
    if not joints:
        return

    fig = ax.get_figure()
    trans = fig.transFigure

    # ---- clear previous instance if requested
    if clear_previous and hasattr(fig, "_ribbon_artists") and tag in fig._ribbon_artists:
        for artist in fig._ribbon_artists[tag]:
            try: artist.remove()
            except: pass
        fig._ribbon_artists[tag] = []
    added = []

    # Ensure renderer for text height metrics (not strictly required for vertical layout,
    # but keeps things consistent across backends).
    try:
        renderer = fig.canvas.get_renderer()
    except Exception:
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()

    # --- layout metrics
    box_w, box_h = box_size
    n = len(joints)

    # Total legend height
    total_h = n * box_h + (n - 1) * row_gap

    # Axes bbox in figure coords
    axbox = ax.get_position()

    # X placement
    if x_align == 'axes_right+pad':
        x = axbox.x1 + right_pad
    else:
        x = float(x_align)  # figure fraction

    # Y center
    if y_center == 'axes_mid':
        y_mid = 0.5 * (axbox.y0 + axbox.y1)
    else:
        y_mid = float(y_center)

    # Top-left of the legend block
    y_top = y_mid + total_h / 2.0

    # Optional background strip
    if bg_alpha and bg_alpha > 0:
        bg_pad_x = 0.006
        bg_pad_y = 0.006
        bg_rect = Rectangle(
            (x - bg_pad_x, y_top - total_h - bg_pad_y),
            box_w + label_offset + 0.035 + 2 * bg_pad_x,    # 0.035 ~ room for text
            total_h + 2 * bg_pad_y,
            transform=trans,
            facecolor="white", edgecolor="none",
            alpha=bg_alpha, zorder=zorder - 1
        )
        fig.patches.append(bg_rect)
        added.append(bg_rect)

    # Draw items (top -> bottom)
    y_cursor = y_top
    for j in joints:
        # color box (left), label to its right
        rect = Rectangle(
            (x, y_cursor - box_h), box_w, box_h,
            transform=trans, facecolor=color_map.get(j, "#d3d3d3"),
            edgecolor="none", zorder=zorder
        )
        fig.patches.append(rect)
        added.append(rect)

        txt = fig.text(
            x + box_w + label_offset, y_cursor - box_h / 2.0, str(j),
            transform=trans, fontsize=fontsize, color="black",
            ha="left", va="center", zorder=zorder + 1
        )
        added.append(txt)

        y_cursor -= (box_h + row_gap)

    # remember what we drew so we can clear next time
    if not hasattr(fig, "_ribbon_artists"):
        fig._ribbon_artists = {}
    fig._ribbon_artists[tag] = added
