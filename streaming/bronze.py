"""Bronze: read the Kafka match-event stream and land raw events into Delta."""
import logging
from pyspark.sql import functions as F
from streaming.config import BRONZE_PATH, CHECKPOINT_DIR, KAFKA_BROKER, MATCH_TOPIC
from streaming.session import get_spark

logger = logging.getLogger(__name__)

def run() -> None:
    spark = get_spark("pitchflow-bronze")
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", MATCH_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("event_json"),
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    query = (
        bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bronze")
        .option("path", BRONZE_PATH)
        .trigger(processingTime="2 seconds")
        .start()
    )
    logger.info("Bronze stream started -> %s (Ctrl+C to stop)", BRONZE_PATH)
    query.awaitTermination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
