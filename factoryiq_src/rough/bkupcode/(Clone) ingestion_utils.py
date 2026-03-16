from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re


def read_and_prepare(spark, path: str):
    """
    Reads a semi-colon separated robot file where:
      - Row 1: metadata
      - Row 2: header (semicolon-delimited; may contain invalid chars)
      - Row 3: metadata
      - Row 4+: data rows

    Returns:
        Spark DataFrame with sanitized, unique, Delta-safe column names.
    """

    # -------------------------------
    # 1️⃣ Read raw file as text
    # -------------------------------
    raw_df = spark.read.text(path)

    df_rn = raw_df.withColumn(
        "rn",
        F.row_number().over(
            Window.orderBy(F.monotonically_increasing_id())
        )
    )

    # -------------------------------
    # 2️⃣ Extract header (row 2)
    # -------------------------------
    header_row = (
        df_rn
        .filter(F.col("rn") == 2)
        .select("value")
        .first()[0]
    )

    header_row = header_row.rstrip(";")
    raw_cols = header_row.split(";")

    # -------------------------------
    # 3️⃣ Sanitize column names
    # -------------------------------
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

    # -------------------------------
    # 4️⃣ Extract data rows (row 4+)
    # -------------------------------
    data_only = (
        df_rn
        .filter(F.col("rn") >= 4)
        .select("value")
    )

    arr = F.split(F.col("value"), ";")

    # Determine widest row safely
    max_fields = (
        data_only
        .select(F.max(F.size(arr)))
        .first()[0]
    )

    if max_fields is None:
        raise ValueError("No data rows found in file.")

    limit = min(len(cols), max_fields)

    # -------------------------------
    # 5️⃣ Build final dataframe
    # -------------------------------
    select_exprs = [
        arr.getItem(i).alias(cols[i])
        for i in range(limit)
    ]

    final_df = data_only.select(*select_exprs)

    return final_df