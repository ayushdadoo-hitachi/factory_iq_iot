def build_paths(cfg: dict) -> dict:

    ref_base = cfg["reference_data"]["base_path"].rstrip("/")

    robot_base = cfg["robot_data"]["base_path"]

    

    tables = cfg["tables"]

    return {
        "STAGE0_TABLE": f"{ref_base}/robot_data/robot_data_tables/{tables['stage0']}",
        "STAGE1_TABLE": f"{ref_base}/robot_data/robot_data_tables/{tables['stage1']}",
        "STAGE2_TABLE": f"{ref_base}/robot_data/robot_data_tables/{tables['stage2']}",
        "STAGE3_TABLE": f"{ref_base}/robot_data/robot_data_tables/{tables['stage3']}",
        "REF_REQUIRED_WELD_FEATURES": f"{ref_base}/ref_tables/{cfg['ref_tables']['required_weld_features']}",
        "REF_WPS_FEATURES": f"{ref_base}/ref_tables/{cfg['ref_tables']['wps']}",
        "JOINT_COLORS": f"{ref_base}/ref_tables/{cfg['ref_tables']['joint_colors']}",
        "IMAGE_BASEPATH": f"{cfg['output']['image_path']}"
    }


def build_weldcoldnames(cfg: dict) -> dict:

    return {
        "TECHDEV_ISRUNNING_COL": cfg["welddata_columns"]["techdev_isrunning_col"],
        "TPS500_CURRENT_COL": cfg["welddata_columns"]["tps500i_current_col"]
    }


def ref_data(cfg: dict) -> dict:

    return {
        "joint_colors_path": cfg["reference_data"]["joint_colors"],
        "ref_path_stage0_required_columns": cfg["reference_data"]["stage0_required_columns"],
        "wps_path_a": cfg["reference_data"]["wps_actual_excel"],
        "reference_path": cfg["reference_data"]["required_weld_features"]
    }
