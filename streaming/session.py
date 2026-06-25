"""Spark session factory shared by all streaming jobs (Delta + Kafka wired in)."""
import logging

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

def get_spark(app_name: str = "pitchflow") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=[KAFKA_PACKAGE]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session ready: %s", app_name)
    return spark
