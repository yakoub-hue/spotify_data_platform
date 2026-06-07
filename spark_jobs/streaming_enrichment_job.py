import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
POSTGRES_URL = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")

CHECKPOINT_PATH = "s3a://spotify-checkpoints/streaming_enrichment"
PARQUET_OUT_PATH = "s3a://spotify-parquet/enriched"

POSTGRES_PROPS = {
    "user": "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

LISTENING_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("source_peer", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("completed", BooleanType(), True),
    StructField("event_source", StringType(), True),
])

P2P_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("peer_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("chunk_size_kb", IntegerType(), True),
    StructField("target_peer", StringType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("geo_country", StringType(), True),
    StructField("device_type", StringType(), True),
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-enrichment")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def read_kafka_json(spark, topic, schema):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(F.from_json(F.col("json_value"), schema).alias("data"))
        .select("data.*")
    )

def load_catalog(spark):
    tracks = (
        spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)
        .select(
            F.col("id").alias("catalog_track_id"),
            F.col("title").alias("track_title"),
            F.col("artist_id").alias("catalog_artist_id"),
            F.col("genre").alias("genre"),
        )
    )

    artists = (
        spark.read.jdbc(POSTGRES_URL, "artists", properties=POSTGRES_PROPS)
        .select(
            F.col("id").alias("catalog_artist_id"),
            F.col("name").alias("artist_name"),
            F.col("country").alias("artist_country"),
        )
    )

    return tracks.join(artists, "catalog_artist_id", "inner")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage du job streaming_enrichment...")

    listening_df = (
        read_kafka_json(spark, "listening_events", LISTENING_SCHEMA)
        .withColumn("event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
        .withWatermark("event_time", "2 minutes")
    )

    p2p_df = (
        read_kafka_json(spark, "p2p_network_events", P2P_SCHEMA)
        .withColumn("p2p_event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
        .withWatermark("p2p_event_time", "2 minutes")
    )

    catalog_df = load_catalog(spark)

    enriched_catalog_df = (
        listening_df.alias("l")
        .join(
            catalog_df.alias("c"),
            F.col("l.track_id") == F.col("c.catalog_track_id"),
            "inner"
        )
    )

    enriched_df = (
        enriched_catalog_df.alias("e")
        .join(
            p2p_df.alias("p"),
            (F.col("e.track_id") == F.col("p.track_id")) &
            (F.col("e.event_time") >= F.col("p.p2p_event_time") - F.expr("INTERVAL 2 MINUTES")) &
            (F.col("e.event_time") <= F.col("p.p2p_event_time") + F.expr("INTERVAL 2 MINUTES")),
            "leftOuter"
        )
        .dropDuplicates(["event_id"])
        .select(
            F.col("e.event_id"),
            F.col("e.user_id"),
            F.col("e.track_id"),
            F.col("e.timestamp"),
            F.col("e.event_time"),
            F.col("e.track_title"),
            F.col("e.artist_name"),
            F.col("e.genre"),
            F.col("e.artist_country"),
            F.col("e.duration_ms"),
            F.col("e.device_type"),
            F.col("e.geo_country"),
            F.col("e.completed"),
            F.col("e.event_source"),
            F.col("p.event_type").alias("p2p_event_type"),
            F.col("p.peer_id").alias("p2p_peer_id"),
            F.col("p.latency_ms").alias("p2p_latency_ms"),
            F.date_format(F.col("e.event_time"), "yyyy-MM-dd").alias("date"),
            F.date_format(F.col("e.event_time"), "HH").alias("hour"),
        )
    )

    kafka_query = (
        enriched_df
        .selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "enriched_events")
        .option("checkpointLocation", CHECKPOINT_PATH + "/kafka")
        .start()
    )

    parquet_query = (
        enriched_df
        .writeStream
        .format("parquet")
        .option("checkpointLocation", CHECKPOINT_PATH + "/parquet")
        .partitionBy("date", "hour")
        .start(PARQUET_OUT_PATH)
    )

    print("Écriture active vers Kafka enriched_events et MinIO Parquet.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
