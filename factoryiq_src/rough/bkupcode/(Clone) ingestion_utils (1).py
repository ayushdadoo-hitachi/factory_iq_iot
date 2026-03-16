from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
import logging

logger = logging.getLogger(__name__)


def read_and_prepare(spark, path: str):

    logger.info(f"Starting read_and_prepare for file: {path}")

    # -------------------------------
    # 1️⃣ Read raw file as text
    # -------------------------------
    logger.info("Reading raw text file")

    raw_df = spark.read.text(path)

    df_rn = raw_df.withColumn(
        "rn",
        F.row_number().over(
            Window.orderBy(F.monotonically_increasing_id())
        )
    )

    logger.info("Row numbering completed")

    # -------------------------------
    # 2️⃣ Extract header (row 2)
    # -------------------------------
    logger.info("Extracting header row")

    header_row = (
        df_rn
        .filter(F.col("rn") == 2)
        .select("value")
        .first()[0]
    )

    header_row = header_row.rstrip(";")
    raw_cols = header_row.split(";")

    logger.info(f"Number of raw header columns found: {len(raw_cols)}")

    # -------------------------------
    # 3️⃣ Sanitize column names
    # -------------------------------
    logger.info("Sanitizing column names")

    invalid = re.compile(r"[ ,;{}\(\)\n\t=]+")

    def clean(name: str) -> str:
        n = (name or "").strip()
        n = n.rstrip(",")
        n = invalid.sub("_", n)
        n = re.sub(r"_+", "_", n).strip("_")
        return n

    seen = {}
    cols = []

    for i, col in enumerate(raw_cols):
        base = clean(col)

        if not base:
            base = f"col_{i+1}"

        count = seen.get(base, 0)
        seen[base] = count + 1

        if count == 0:
            cols.append(base)
        else:
            cols.append(f"{base}_{count+1}")

    logger.info(f"Sanitized columns count: {len(cols)}")

    # -------------------------------
    # 4️⃣ Extract data rows (row 4+)
    # -------------------------------
    logger.info("Extracting data rows starting from row 4")

    data_only = (
        df_rn
        .filter(F.col("rn") >= 4)
        .select("value")
    )

    arr = F.split(F.col("value"), ";")

    logger.info("Calculating maximum number of fields in data rows")

    max_fields = (
        data_only
        .select(F.max(F.size(arr)))
        .first()[0]
    )

    if max_fields is None:
        logger.error("No data rows found in file")
        raise ValueError("No data rows found in file.")

    logger.info(f"Maximum fields detected in data rows: {max_fields}")

    limit = min(len(cols), max_fields)

    logger.info(f"Using {limit} columns for final dataframe")

    # -------------------------------
    # 5️⃣ Build final dataframe
    # -------------------------------
    select_exprs = [
        arr.getItem(i).alias(cols[i])
        for i in range(limit)
    ]

    final_df = data_only.select(*select_exprs)

    logger.info("Final dataframe successfully created")

    return final_df