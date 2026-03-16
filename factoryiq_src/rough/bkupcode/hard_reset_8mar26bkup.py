# Databricks notebook source
# ------------------------------------------
# 9️⃣ Hard Reset - only for development
# ------------------------------------------

# COMMAND ----------

import importlib
from config import config_paths as config_paths; importlib.reload(config_paths)

import core.logging_config as logging_conf; importlib.reload(logging_conf)
logger = logging_conf.get_module_logger(__name__)  

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Safety: preview actions without executing destructive steps
DRY_RUN                 = False
ONLY_LATEST_ARCHIVE_DT  = True

# =========================
# Helpers
# =========================
def _path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def _rm_dir(path: str):
    if _path_exists(path):
        if DRY_RUN:
            print(f"[DRY_RUN] Would delete: {path}")
        else:
            print(f"Deleting: {path}")
            dbutils.fs.rm(path, recurse=True)
    else:
        print(f"[skip] Not found: {path}")

def _list_dirs(path: str):
    try:
        return [x for x in dbutils.fs.ls(path) if x.isDir()]
    except Exception:
        return []

def _list_files(path: str, suffix: str = None):
    try:
        files = [x for x in dbutils.fs.ls(path) if not x.isDir()]
    except Exception:
        return []
    if suffix:
        files = [x for x in files if x.path.lower().endswith(suffix.lower())]
    return files

def _get_latest_dt_partition(archive_root: str):
    # expects subfolders like .../archive/dt=YYYY-MM-DD
    dts = []
    for d in _list_dirs(archive_root):
        name = d.name.rstrip("/").lower()
        if name.startswith("dt="):
            dts.append((name[3:], d.path))
    if not dts:
        return None
    dts.sort(key=lambda x: x[0])  # YYYY-MM-DD lexicographic works
    return dts[-1][1]             # path of latest dt partition

def _move_all_csv(src_dir: str, dest_dir: str) -> int:
    if not _path_exists(src_dir):
        print(f"[skip] Source dir not found: {src_dir}")
        return 0
    if not DRY_RUN:
        dbutils.fs.mkdirs(dest_dir)
    csvs = _list_files(src_dir, suffix=".csv")
    moved = 0
    for f in csvs:
        dest_path = f"{dest_dir.rstrip('/')}/{f.name}"
        if DRY_RUN:
            print(f"[DRY_RUN] Would move {f.path} -> {dest_path}")
        else:
            try:
                # Overwrite if exists
                try:
                    dbutils.fs.rm(dest_path)
                except Exception:
                    pass
                dbutils.fs.mv(f.path, dest_path)
                print(f"  moved back: {f.name}")
                moved += 1
            except Exception as e:
                print(f"  [warn] failed to move {f.path}: {e}")
    return moved

def _rewind_archive_to_inflow(archive_root: str, inflow: str, latest_only: bool) -> int:
    total = 0
    if latest_only:
        latest = _get_latest_dt_partition(archive_root)
        if latest:
            print(f"Rewinding from latest archive partition: {latest}")
            total += _move_all_csv(latest, inflow)
        else:
            print("[info] No dt= partitions found in archive (nothing to move)")
    else:
        parts = [d.path for d in _list_dirs(archive_root) if d.name.lower().startswith("dt=")]
        if not parts:
            print("[info] No dt= partitions found in archive (nothing to move)")
        for p in parts:
            print(f"Rewinding from: {p}")
            total += _move_all_csv(p, inflow)
    return total

def _print_summary():
    print("\n--- SUMMARY (post-purge) ---")
    for p in [config_paths.archive_dir, config_paths.outputs_dir, config_paths.tables_root, config_paths.inflow_dir]:
        print(f"{p} -> {'exists' if _path_exists(p) else 'missing'}")

# =========================
# EXECUTION
# =========================
print("=== DEV PURGE + REWIND START ===")
print(f"Robot ID       : {config_paths.ROBOT_ID}")
print(f"Account/Cont   : {config_paths.ACCOUNT}/{config_paths.CONTAINER}")
print(f"Tables Root    : {config_paths.TABLE_ROOT}")
print(f"DRY_RUN        : {DRY_RUN}")
print(f"ONLY_LATEST_DT : {ONLY_LATEST_ARCHIVE_DT}")
print("-------------------------------")

# 1) REWIND: move archived CSVs back to inflow
print("\n[1] Rewind archived CSVs -> inflow")
moved_total = _rewind_archive_to_inflow(config_paths.archive_dir, config_paths.inflow_dir, ONLY_LATEST_ARCHIVE_DT)
print(f"   ✔ Moved {moved_total} file(s) back to inflow")

# 2) Delete ARCHIVE
print("\n[2] Delete ARCHIVE")
_rm_dir(config_paths.archive_dir)

# 3) Delete OUTPUT FILES
print("\n[3] Delete OUTPUT FILES")
_rm_dir(config_paths.outputs_dir)

# 4) Delete TABLE ROOT (Delta layers)
print("\n[4] Delete TABLE ROOT (Delta layers)")
_rm_dir(config_paths.tables_root)

# Ensure inflow exists
if not _path_exists(config_paths.inflow_dir) and not DRY_RUN:
    dbutils.fs.mkdirs(config_paths.inflow_dir)
    print(f"[info] inflow recreated: {config_paths.inflow_dir}")

_print_summary()
print("\n=== DEV PURGE + REWIND DONE ===")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC # Databricks notebook cell — DEV PURGE + REWIND (rewind archived CSVs back to inflow, then purge)
# MAGIC # Actions (in order):
# MAGIC #   1) Move archived CSVs back to inflow (rewind)
# MAGIC #   2) Delete ARCHIVE folder
# MAGIC #   3) Delete OUTPUT FILES folder
# MAGIC #   4) Delete TABLE ROOT (Delta layers: bronze, manifest, silver, etc.)
# MAGIC #
# MAGIC # Leaves:
# MAGIC #   - inflow/ intact (so you can re-ingest immediately)
# MAGIC #
# MAGIC # Requirements:
# MAGIC #   from databricks.sdk.runtime import dbutils
# MAGIC
# MAGIC from databricks.sdk.runtime import dbutils
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC # =========================
# MAGIC # ---- PARAMETERS ----------
# MAGIC # =========================
# MAGIC ACCOUNT     = "hrlfactoryiqsa"
# MAGIC CONTAINER   = "weld-data"
# MAGIC ROBOT_ID    = "igm6_29fjan_4feb"
# MAGIC
# MAGIC # Choose your table root name: "lake" (recommended) or "robot_data_tables" (legacy)
# MAGIC TABLE_ROOT  = "lake"   # change to "robot_data_tables" if you keep the old name
# MAGIC
# MAGIC # Safety: preview actions without executing destructive steps
# MAGIC DRY_RUN                 = False
# MAGIC
# MAGIC # When rewinding (moving CSVs back from archive to inflow):
# MAGIC # - if True: only move from the latest dt=YYYY-MM-DD partition
# MAGIC # - if False: move from all dt partitions
# MAGIC ONLY_LATEST_ARCHIVE_DT  = True
# MAGIC
# MAGIC # =========================
# MAGIC # Derived paths
# MAGIC # =========================
# MAGIC base = f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.windows.net/{ROBOT_ID}/robot_data"
# MAGIC
# MAGIC inflow_dir   = f"{base}/inflow/"
# MAGIC archive_dir  = f"{base}/archive/"
# MAGIC outputs_dir  = f"{base}/output_files/"
# MAGIC tables_root  = f"{base}/{TABLE_ROOT}/"   # parent of bronze/silver/manifest etc.
# MAGIC
# MAGIC # If you keep named folders under the table root, they’ll be included when deleting the root
# MAGIC bronze_path   = f"{tables_root}bronze_{ROBOT_ID}"
# MAGIC manifest_path = f"{bronze_path}_manifest"
# MAGIC silver_path   = f"{tables_root}silver_{ROBOT_ID}"
# MAGIC
# MAGIC # =========================
# MAGIC # Helpers
# MAGIC # =========================
# MAGIC def _path_exists(path: str) -> bool:
# MAGIC     try:
# MAGIC         dbutils.fs.ls(path)
# MAGIC         return True
# MAGIC     except Exception:
# MAGIC         return False
# MAGIC
# MAGIC def _rm_dir(path: str):
# MAGIC     if _path_exists(path):
# MAGIC         if DRY_RUN:
# MAGIC             print(f"[DRY_RUN] Would delete: {path}")
# MAGIC         else:
# MAGIC             print(f"Deleting: {path}")
# MAGIC             dbutils.fs.rm(path, recurse=True)
# MAGIC     else:
# MAGIC         print(f"[skip] Not found: {path}")
# MAGIC
# MAGIC def _list_dirs(path: str):
# MAGIC     try:
# MAGIC         return [x for x in dbutils.fs.ls(path) if x.isDir()]
# MAGIC     except Exception:
# MAGIC         return []
# MAGIC
# MAGIC def _list_files(path: str, suffix: str = None):
# MAGIC     try:
# MAGIC         files = [x for x in dbutils.fs.ls(path) if not x.isDir()]
# MAGIC     except Exception:
# MAGIC         return []
# MAGIC     if suffix:
# MAGIC         files = [x for x in files if x.path.lower().endswith(suffix.lower())]
# MAGIC     return files
# MAGIC
# MAGIC def _get_latest_dt_partition(archive_root: str):
# MAGIC     # expects subfolders like .../archive/dt=YYYY-MM-DD
# MAGIC     dts = []
# MAGIC     for d in _list_dirs(archive_root):
# MAGIC         name = d.name.rstrip("/").lower()
# MAGIC         if name.startswith("dt="):
# MAGIC             dts.append((name[3:], d.path))
# MAGIC     if not dts:
# MAGIC         return None
# MAGIC     dts.sort(key=lambda x: x[0])  # YYYY-MM-DD lexicographic works
# MAGIC     return dts[-1][1]             # path of latest dt partition
# MAGIC
# MAGIC def _move_all_csv(src_dir: str, dest_dir: str) -> int:
# MAGIC     if not _path_exists(src_dir):
# MAGIC         print(f"[skip] Source dir not found: {src_dir}")
# MAGIC         return 0
# MAGIC     if not DRY_RUN:
# MAGIC         dbutils.fs.mkdirs(dest_dir)
# MAGIC     csvs = _list_files(src_dir, suffix=".csv")
# MAGIC     moved = 0
# MAGIC     for f in csvs:
# MAGIC         dest_path = f"{dest_dir.rstrip('/')}/{f.name}"
# MAGIC         if DRY_RUN:
# MAGIC             print(f"[DRY_RUN] Would move {f.path} -> {dest_path}")
# MAGIC         else:
# MAGIC             try:
# MAGIC                 # Overwrite if exists
# MAGIC                 try:
# MAGIC                     dbutils.fs.rm(dest_path)
# MAGIC                 except Exception:
# MAGIC                     pass
# MAGIC                 dbutils.fs.mv(f.path, dest_path)
# MAGIC                 print(f"  moved back: {f.name}")
# MAGIC                 moved += 1
# MAGIC             except Exception as e:
# MAGIC                 print(f"  [warn] failed to move {f.path}: {e}")
# MAGIC     return moved
# MAGIC
# MAGIC def _rewind_archive_to_inflow(archive_root: str, inflow: str, latest_only: bool) -> int:
# MAGIC     total = 0
# MAGIC     if latest_only:
# MAGIC         latest = _get_latest_dt_partition(archive_root)
# MAGIC         if latest:
# MAGIC             print(f"Rewinding from latest archive partition: {latest}")
# MAGIC             total += _move_all_csv(latest, inflow)
# MAGIC         else:
# MAGIC             print("[info] No dt= partitions found in archive (nothing to move)")
# MAGIC     else:
# MAGIC         parts = [d.path for d in _list_dirs(archive_root) if d.name.lower().startswith("dt=")]
# MAGIC         if not parts:
# MAGIC             print("[info] No dt= partitions found in archive (nothing to move)")
# MAGIC         for p in parts:
# MAGIC             print(f"Rewinding from: {p}")
# MAGIC             total += _move_all_csv(p, inflow)
# MAGIC     return total
# MAGIC
# MAGIC def _print_summary():
# MAGIC     print("\n--- SUMMARY (post-purge) ---")
# MAGIC     for p in [archive_dir, outputs_dir, tables_root, inflow_dir]:
# MAGIC         print(f"{p} -> {'exists' if _path_exists(p) else 'missing'}")
# MAGIC
# MAGIC # =========================
# MAGIC # EXECUTION
# MAGIC # =========================
# MAGIC print("=== DEV PURGE + REWIND START ===")
# MAGIC print(f"Robot ID       : {ROBOT_ID}")
# MAGIC print(f"Account/Cont   : {ACCOUNT}/{CONTAINER}")
# MAGIC print(f"Tables Root    : {TABLE_ROOT}")
# MAGIC print(f"DRY_RUN        : {DRY_RUN}")
# MAGIC print(f"ONLY_LATEST_DT : {ONLY_LATEST_ARCHIVE_DT}")
# MAGIC print("-------------------------------")
# MAGIC
# MAGIC # 1) REWIND: move archived CSVs back to inflow
# MAGIC print("\n[1] Rewind archived CSVs -> inflow")
# MAGIC moved_total = _rewind_archive_to_inflow(archive_dir, inflow_dir, ONLY_LATEST_ARCHIVE_DT)
# MAGIC print(f"   ✔ Moved {moved_total} file(s) back to inflow")
# MAGIC
# MAGIC # 2) Delete ARCHIVE
# MAGIC print("\n[2] Delete ARCHIVE")
# MAGIC _rm_dir(archive_dir)
# MAGIC
# MAGIC # 3) Delete OUTPUT FILES
# MAGIC print("\n[3] Delete OUTPUT FILES")
# MAGIC _rm_dir(outputs_dir)
# MAGIC
# MAGIC # 4) Delete TABLE ROOT (Delta layers)
# MAGIC print("\n[4] Delete TABLE ROOT (Delta layers)")
# MAGIC _rm_dir(tables_root)
# MAGIC
# MAGIC # Ensure inflow exists
# MAGIC if not _path_exists(inflow_dir) and not DRY_RUN:
# MAGIC     dbutils.fs.mkdirs(inflow_dir)
# MAGIC     print(f"[info] inflow recreated: {inflow_dir}")
# MAGIC
# MAGIC _print_summary()
# MAGIC print("\n=== DEV PURGE + REWIND DONE ===")