def read_delta(spark, path: str):
    return spark.read.format("delta").load(path)

def write_delta(df, path: str, mode="overwrite"):
    df.write.format("delta").mode(mode).save(path)

def read_csv(spark, path: str):
    return spark.read.option("header", "true").csv(path)
