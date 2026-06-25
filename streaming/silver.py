"""Silver: parse Bronze raw JSON into typed, deduplicated event rows."""
import logging
from pyspark.sql import DataFrame, functions as F, types as T
from streaming.config import BRONZE_PATH, CHECKPOINT_DIR, SILVER_PATH
from streaming.session import get_spark

logger = logging.getLogger(__name__)
APP_ID = "pitchflow-silver"

_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("index", T.IntegerType()),
    T.StructField("period", T.IntegerType()),
    T.StructField("minute", T.IntegerType()),
    T.StructField("second", T.IntegerType()),
    T.StructField("type", T.StructType([T.StructField("name", T.StringType())])),
    T.StructField("team", T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("name", T.StringType()),
    ])),
    T.StructField("player", T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("name", T.StringType()),
    ])),
    T.StructField("location", T.ArrayType(T.DoubleType())),
    T.StructField("shot", T.StructType([
        T.StructField("statsbomb_xg", T.DoubleType()),
        T.StructField("outcome", T.StructType([T.StructField("name", T.StringType())])),
    ])),
])

def parse_and_flatten(df: DataFrame, kafka_ts_col: str = "kafka_timestamp") -> DataFrame:
    parsed = df.withColumn("_e", F.from_json(F.col("event_json"), _SCHEMA))
    return parsed.select(
        F.col("_e.id").alias("event_id"),
        F.col("_e.index").alias("event_index"),
        F.col("_e.period").alias("period"),
        F.col("_e.minute").alias("minute"),
        F.col("_e.second").alias("second"),
        (F.col("_e.minute") * 60 + F.col("_e.second")).alias("match_second"),
        F.col("_e.type.name").alias("event_type"),
        F.col("_e.team.id").alias("team_id"),
        F.col("_e.team.name").alias("team_name"),
        F.col("_e.player.id").alias("player_id"),
        F.col("_e.player.name").alias("player_name"),
        F.element_at(F.col("_e.location"), 1).alias("loc_x"),
        F.element_at(F.col("_e.location"), 2).alias("loc_y"),
        F.coalesce(F.col("_e.shot.statsbomb_xg"), F.lit(0.0)).alias("xg"),
        F.col("_e.shot.outcome.name").alias("shot_outcome"),
        F.col(kafka_ts_col).alias("event_time"),
    ).filter(F.col("event_id").isNotNull() & (F.col("period") <= 4))

def run() -> None:
    spark = get_spark("pitchflow-silver")
    bronze = spark.readStream.format("delta").load(BRONZE_PATH)
    silver_parsed = parse_and_flatten(bronze)
    silver_deduped = (
        silver_parsed
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        (batch_df.write
            .format("delta").mode("append")
            .option("txnAppId", APP_ID)
            .option("txnVersion", batch_id)
            .save(SILVER_PATH))
        logger.info("Silver batch %d: wrote %d rows", batch_id, batch_df.count())

    query = (
        silver_deduped.writeStream
        .outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver")
        .trigger(processingTime="5 seconds")
        .start()
    )
    logger.info("Silver stream started -> %s (Ctrl+C to stop)", SILVER_PATH)
    query.awaitTermination()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
