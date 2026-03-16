# core/fs_utils.py
from typing import List, Optional
import importlib

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)

def list_csvs(dbu, path: str, limit: Optional[int] = None) -> List[str]:
    items = dbu.fs.ls(path)
    csvs = sorted([it.path for it in items if it.path.lower().endswith(".csv")])
    if limit:
        csvs = csvs[:limit]
    return csvs

def archive_one(dbu, src_path: str, archive_root: str, by_date: bool) -> None:
    from datetime import datetime
    if by_date:
        dt = datetime.utcnow().strftime("%Y-%m-%d")
        dest_dir = f"{archive_root.rstrip('/')}/dt={dt}"
    else:
        dest_dir = archive_root.rstrip("/")
    dbu.fs.mkdirs(dest_dir)

    fname = src_path.split("/")[-1]
    dest_path = f"{dest_dir}/{fname}"

    try:
        dbu.fs.rm(dest_path)
    except Exception:
        pass
    dbu.fs.mv(src_path, dest_path)