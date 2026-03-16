# Databricks notebook source
# print(IMAGE_BASEPATH)

# COMMAND ----------

dbutils.widgets.text("source", "/Volumes/factoryiq_iot/weld_images/images", "Source Folder")
dbutils.widgets.text("dest", "abfss://weld-data@hrlfactoryiqsa.dfs.core.windows.net/robot_data/weld_path_images", "Destination Folder")
dbutils.widgets.dropdown("dry_run", "false", ["true","false"], "Dry Run")

source = dbutils.widgets.get("source")
dest   = dbutils.widgets.get("dest")
dry_run = dbutils.widgets.get("dry_run") == "true"

files = dbutils.fs.ls(source)

print(f"Found {len(files)} items to copy.\n")

for f in files:
    target_path = dest + "/" + f.name
    print(f"{'DRY RUN:' if dry_run else 'COPYING:'} {f.path} -> {target_path}")
    if not dry_run:
        dbutils.fs.cp(f.path, target_path, recurse=True)

# COMMAND ----------

# Optional: return a message to the caller notebook
dbutils.notebook.exit("Notebook execution stopped intentionally")

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3_clean_joint;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS workspace.welddata.stage3;