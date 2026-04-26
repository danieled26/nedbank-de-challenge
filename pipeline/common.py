import json
import os
from datetime import datetime, timezone
from pathlib import Path


import yaml
from pyspark.sql import SparkSession, functions as F
# from delta import configure_spark_with_delta_pip


RUN_STATE_PATH = "/tmp/nedbank_pipeline_run_state.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def load_config() -> dict:
    runtime_path = "/data/config/pipeline_config.yaml"
    fallback_path = "/app/config/pipeline_config.yaml"

    path = runtime_path if os.path.exists(runtime_path) else fallback_path
    return load_yaml(path)


def load_dq_rules() -> dict:
    runtime_path = "/data/config/dq_rules.yaml"
    fallback_path = "/app/config/dq_rules.yaml"

    path = runtime_path if os.path.exists(runtime_path) else fallback_path
    if not os.path.exists(path):
        return {}
    return load_yaml(path)


def get_or_create_run_state() -> dict:
    if os.path.exists(RUN_STATE_PATH):
        with open(RUN_STATE_PATH, "r") as f:
            return json.load(f)

    state = {
        "run_timestamp": utc_now_iso(),
        "pipeline_run_id": datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S"),
        "stage": "auto",
        "source_record_counts": {},
        "dq_metrics": {},
    }

    save_run_state(state)
    return state


def save_run_state(state: dict) -> None:
    with open(RUN_STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def create_spark() -> SparkSession:
    cfg = load_config()
    spark_cfg = cfg.get("spark", {})
    delta_jars = ",".join([
    "/root/.ivy2/jars/io.delta_delta-spark_2.12-3.1.0.jar",
    "/root/.ivy2/jars/io.delta_delta-storage-3.1.0.jar",
    ])  

    builder = (
        SparkSession.builder
        .master(spark_cfg.get("master", "local[2]"))
        .appName(spark_cfg.get("app_name", "nedbank-de-pipeline"))
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "512m"))
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "1g"))
        .config("spark.default.parallelism", str(spark_cfg.get("default_parallelism", 2)))
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 4)))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.local.dir", spark_cfg.get("local_dir", "/tmp"))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.blockManager.port", "0")
        .config("spark.driver.port", "0")
        .config("spark.jars", delta_jars)
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        )

    # spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def write_delta(df, path: str, mode: str = "overwrite") -> None:
    ensure_dir(os.path.dirname(path))
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("compression", "uncompressed")
        .save(path)
    )

def table_path(root: str, name: str) -> str:
    return os.path.join(root.rstrip("/"), name)


def add_ingestion_columns(df, source_name: str, source_file: str):
    state = get_or_create_run_state()
    return (
        df.withColumn("ingestion_timestamp", F.to_timestamp(F.lit(state["run_timestamp"])))
          .withColumn("_source_name", F.lit(source_name))
          .withColumn("_source_file", F.lit(source_file))
          .withColumn("_pipeline_run_id", F.lit(state["pipeline_run_id"]))
    )