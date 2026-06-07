import os
import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)
 
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
POSTGRES_URL = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
CHECKPOINT_PATH = "s3a://spotify-checkpoints/fraud_detection_v2"

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
    StructField("status", StringType(), True),
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
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

def write_alerts(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[Batch {batch_id}] aucune fraude détectée")
        return

    print(f"[Batch {batch_id}] fraudes détectées : {batch_df.count()}")
    batch_df.show(20, truncate=False)

    postgres_df = batch_df.select(
        F.expr("CAST(user_id AS STRING)").alias("user_id"),
        F.expr("CAST(peer_id AS STRING)").alias("peer_id"),
        "fraud_type",
        "suspicion_score",
        F.expr("CAST(evidence AS STRING)").alias("evidence"),
        "window_start",
        "window_end"
    )

    postgres_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "fraud_detections_text") \
        .option("user", "spotify") \
        .option("password", "spotify") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"[Batch {batch_id}] alertes écrites dans Kafka + PostgreSQL")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage fraud_detection_job...")

    listening_df = (
        read_kafka_json(spark, "listening_events", LISTENING_SCHEMA)
        .withColumn("event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
    )

    p2p_df = (
        read_kafka_json(spark, "p2p_network_events", P2P_SCHEMA)
        .withColumn("p2p_event_time", F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", "")))
    )

    rule1_df = (
        listening_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "10 minutes"), "user_id")
        .agg(F.count("*").alias("listen_count"))
        .filter(F.col("listen_count") > 10)
        .select(
            F.col("user_id"),
            F.lit(None).cast("string").alias("peer_id"),
            F.lit("burst_listening").alias("fraud_type"),
            F.least(F.lit(1.0), F.col("listen_count") / F.lit(100.0)).alias("suspicion_score"),
            F.to_json(F.struct("listen_count")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    rule2_df = (
        listening_df
        .withWatermark("event_time", "1 hour")
        .groupBy(F.window("event_time", "1 hour"), "user_id")
        .agg(
            F.count("*").alias("listen_count"),
            F.avg("duration_ms").alias("avg_duration_ms")
        )
        .filter((F.col("listen_count") > 3) & (F.col("avg_duration_ms") < 5000))
        .select(
            F.col("user_id"),
            F.lit(None).cast("string").alias("peer_id"),
            F.lit("short_duration_bot").alias("fraud_type"),
            F.lit(0.9).alias("suspicion_score"),
            F.to_json(F.struct("listen_count", "avg_duration_ms")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    rule3_df = (
        p2p_df
        .filter(F.col("event_type") == "chunk_transfer")
        .withWatermark("p2p_event_time", "15 minutes")
        .groupBy(F.window("p2p_event_time", "15 minutes"), "peer_id")
        .agg(
            F.count("*").alias("total_transfers"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_transfers")
        )
        .filter((F.col("total_transfers") > 2) & ((F.col("failed_transfers") / F.col("total_transfers")) > 0.5))
        .select(
            F.lit(None).cast("string").alias("user_id"),
            F.col("peer_id"),
            F.lit("p2p_failure_rate").alias("fraud_type"),
            (F.col("failed_transfers") / F.col("total_transfers")).alias("suspicion_score"),
            F.to_json(F.struct("total_transfers", "failed_transfers")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    alerts_df = rule1_df.unionByName(rule2_df).unionByName(rule3_df)

    postgres_query = (
        alerts_df.writeStream
        .outputMode("update")
        .foreachBatch(write_alerts)
        .option("checkpointLocation", CHECKPOINT_PATH + "/postgres")
        .start()
    )

    kafka_query = (
        alerts_df
        .selectExpr(
            "CAST(COALESCE(user_id, peer_id) AS STRING) AS key",
            "to_json(struct(*)) AS value"
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "fraud_alerts")
        .option("checkpointLocation", CHECKPOINT_PATH + "/kafka")
        .start()
    )

    print("Fraud detection active → Kafka fraud_alerts + PostgreSQL fraud_detections_text")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()