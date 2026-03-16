# hello_databricks.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    print("=== Hello Databricks ===")
    print("Spark Version:", spark.version)

    df = spark.createDataFrame(
        [("Ayush", 1), ("Pilot", 2)],
        ["name", "id"]
    )
    print("Row count:", df.count())
    df.show(truncate=False)

    # Write a tiny Delta (optional, for proof)
    out = "/tmp/hello_databricks_delta"
    df.write.format("delta").mode("overwrite").save(out)
    print("Wrote delta to:", out)