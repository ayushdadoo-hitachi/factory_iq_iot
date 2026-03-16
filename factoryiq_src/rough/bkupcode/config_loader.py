import yaml


def load_config(spark, config_path: str) -> dict:
    """
    Loads YAML config from ADLS/DBFS using Spark.
    """
    df = spark.read.text(config_path)
    cfg_text = "\n".join(r.value for r in df.collect())
    return yaml.safe_load(cfg_text)
