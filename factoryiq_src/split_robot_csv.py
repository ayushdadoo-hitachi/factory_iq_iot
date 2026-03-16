# Databricks notebook source
# -------------------------------------------------------------
# 9️⃣ Generate small csv file - only for development simulation
# -------------------------------------------------------------

# COMMAND ----------

import importlib
from config import config_paths as config_paths
importlib.reload(config_paths)

# COMMAND ----------

# Databricks notebook cell: Split large robot CSV into 10k-row chunk CSVs
# Keeps the original file structure:
#   row 1: metadata
#   row 2: header
#   row 3: metadata
#   row 4+: data rows (semicolon-delimited)
#
# It writes one CSV per chunk: chunk_00000.csv, chunk_00001.csv, ...

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime, uuid

rows_per_file   = 10_000

# Basic validation to catch placeholder typos
if "<" in config_paths.input_csv_path or ">" in config_paths.input_csv_path or "<" in config_paths.splitfiles_dir or ">" in config_paths.splitfiles_dir:
    raise ValueError("Replace placeholder paths with real ABFSS paths (no <...> allowed).")

# Make sure the output base exists
dbutils.fs.mkdirs(config_paths.splitfiles_dir)

# =========================
# 1) Read raw file as TEXT and number rows
# =========================
raw_df = spark.read.text(config_paths.input_csv_path)
w = Window.orderBy(F.monotonically_increasing_id())
df_rn = raw_df.withColumn("rn", F.row_number().over(w))

# Extract the three preamble lines
row1 = df_rn.filter(F.col("rn") == 1).select("value").first()
row2 = df_rn.filter(F.col("rn") == 2).select("value").first()
row3 = df_rn.filter(F.col("rn") == 3).select("value").first()

if row2 is None or not row2["value"]:
    raise ValueError("Could not find header on row 2. Check the input CSV format.")

meta1_line   = row1["value"] if row1 and row1["value"] is not None else ""
header_line  = row2["value"]
meta3_line   = row3["value"] if row3 and row3["value"] is not None else ""

# Data rows (row 4+), kept as TEXT lines
data_only = df_rn.filter(F.col("rn") >= 4).select("value")
total_rows = data_only.count()
print(f"Total data rows (excluding the first 3 lines): {total_rows}")

if total_rows == 0:
    raise ValueError("No data rows found after row 3. Verify the input file format.")

# =========================
# 2) Assign chunk_id per 10k rows, preserving original row order
#    We reuse the original 'rn' to keep line order stable.
# =========================
data_with_chunk = (
    df_rn
    .filter(F.col("rn") >= 4)
    .select("rn", "value")
    .withColumn("chunk_id", ((F.col("rn") - 4) / rows_per_file).cast("long"))
)

chunk_ids = [r["chunk_id"] for r in data_with_chunk.select("chunk_id").distinct().collect()]
chunk_ids.sort()
print(f"Total chunks to write: {len(chunk_ids)} (each ~{rows_per_file} rows, last may be smaller)")

# =========================
# 3) For each chunk, write a SINGLE CSV file that includes:
#    line1 = original row1 metadata
#    line2 = original header line
#    line3 = original row3 metadata (or blank if absent)
#    line4+ = this chunk's data lines
# =========================
for cid in chunk_ids:
    temp_dir = f"{config_paths.splitfiles_dir}/_chunk_{cid:05d}"
    out_file = f"{config_paths.splitfiles_dir}/chunk_{cid:05d}.csv"

    # Create a small 3-line DataFrame for the preamble
    preamble = spark.createDataFrame(
        [(meta1_line,), (header_line,), (meta3_line,)],
        ["value"]
    )

    # The chunk's data lines (as text)
    chunk_lines = (
        data_with_chunk
        .filter(F.col("chunk_id") == cid)
        .orderBy(F.col("rn"))           # preserve original order
        .select("value")
    )

    # Combine preamble + data and write as a single text file
    combined = preamble.unionByName(chunk_lines)

    # Write to a temp folder as a single part file
    (combined
        .coalesce(1)
        .write
        .mode("overwrite")
        .text(temp_dir))

    # Find the single part file and rename it to chunk_<id>.csv
    part_files = [f.path for f in dbutils.fs.ls(temp_dir) if f.name.startswith("part-")]
    if not part_files:
        raise RuntimeError(f"No part file found in {temp_dir}")
    dbutils.fs.mv(part_files[0], out_file)
    dbutils.fs.rm(temp_dir, recurse=True)

    print(f"✔ Wrote {out_file}")

print("✅ Split complete.")