import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import DataFrame


def sanitize_excel_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    clean = pdf.copy()

    for col in clean.columns:
        dtype = clean[col].dtype

        if dtype in ["int64", "float64"]:
            continue

        clean[col] = (
            clean[col]
            .astype(str)
            .replace({
                "nan": None,
                "NaN": None,
                "None": None,
                "": None
            })
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


def pandas_excel_to_spark(spark, pdf: pd.DataFrame) -> DataFrame:
    pdf = sanitize_excel_pdf(pdf)
    schema = pandas_to_spark_schema(pdf)
    return spark.createDataFrame(pdf, schema=schema)