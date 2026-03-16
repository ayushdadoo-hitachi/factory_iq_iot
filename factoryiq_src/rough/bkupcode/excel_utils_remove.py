import pandas as pd
from pyspark.sql.types import *
from databricks.sdk.runtime import dbutils  # REQUIRED


def sanitize_excel_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    clean = pdf.copy()

    for col in clean.columns:
        dtype = clean[col].dtype

        if dtype in ["int64", "float64"]:
            continue

        clean[col] = (
            clean[col]
            .astype(str)
            .replace(
                {
                    "nan": None,
                    "NaN": None,
                    "None": None,
                    "": None
                }
            )
        )

    return clean


def pandas_to_spark_schema(pdf: pd.DataFrame) -> StructType:
    fields = []

    for col, dtype in pdf.dtypes.items():
        if dtype == "int64":
            fields.append(StructField(col, LongType(), True))
        elif dtype == "float64":
            fields.append(StructField(col, DoubleType(), True))
        else:
            fields.append(StructField(col, StringType(), True))

    return StructType(fields)


def read_excel_from_abfss(path: str):
    local_path = "/tmp/_temp_excel.xlsx"
    dbutils.fs.cp(path, f"file:{local_path}")
    return pd.read_excel(local_path)


def excel_path_to_spark_df(spark, path: str):
    pdf = read_excel_from_abfss(path)
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    return spark.createDataFrame(pdf, schema=schema)