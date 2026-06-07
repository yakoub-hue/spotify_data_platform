"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py

TODO :
    [ ] Implémenter la lecture du topic Kafka avec readStream
    [ ] Désérialiser les messages JSON avec le bon schéma
    [ ] Implémenter les fenêtres tumbling de 5 minutes
    [ ] Implémenter les sliding windows pour les genres (15 min / 5 min)
    [ ] Configurer le checkpoint sur MinIO
    [ ] Écrire les résultats dans PostgreSQL et Redis
"""

import os
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.

    TODO : vérifier que les packages kafka et postgresql sont disponibles
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming.

    TODO :
        1. Utiliser spark.readStream.format("kafka")
        2. Configurer kafka.bootstrap.servers, subscribe, startingOffsets
        3. Caster la colonne "value" (bytes) en string
        4. Parser le JSON avec from_json() et LISTENING_EVENT_SCHEMA
        5. Caster la colonne "timestamp" (string ISO) en TimestampType
        6. Renommer en "event_time" pour les fenêtres temporelles

    Returns:
        DataFrame streaming avec colonnes typées
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.isolation.level", "read_committed")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(
            F.from_json(
                F.col("json_value"),
                LISTENING_EVENT_SCHEMA
            ).alias("event")
        )
        .select("event.*")
        .withColumn(
            "event_time",
            F.to_timestamp("timestamp")
        )
    )

    return parsed_df


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes.

    TODO :
        1. groupBy(window("event_time", "5 minutes"), "track_id")
        2. agg(count("*").alias("stream_count"), countDistinct("user_id").alias("unique_listeners"))
        3. Output mode : "update" (on met à jour au fur et à mesure)
        4. Écrire dans PostgreSQL table realtime_top_tracks

    Hint : pour écrire dans PostgreSQL depuis Spark Streaming,
    utiliser foreachBatch() et df.write.jdbc() dans le batch.
    """
    events_df_watermarked = events_df.withWatermark("event_time", "10 minutes")

    grouped_df = (
            events_df_watermarked
            .filter(F.col("track_id").isNotNull())
            .filter(F.col("event_time").isNotNull())
            .groupBy(F.window("event_time", "5 minutes"), "track_id")
            .agg(
                F.count("*").alias("stream_count"),
                F.approx_count_distinct("user_id").alias("unique_listeners")
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("track_id"),
                F.col("stream_count"),
                F.col("unique_listeners")
            )
        )

    def write_to_postgres(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                print(f"[Batch {batch_id}] Aucun événement.")
                return

            staging_table = f"staging_realtime_top_tracks_{batch_id}"

            batch_df.write \
                .format("jdbc") \
                .option("url", POSTGRES_URL) \
                .option("dbtable", staging_table) \
                .option("user", POSTGRES_PROPS["user"]) \
                .option("password", POSTGRES_PROPS["password"]) \
                .option("driver", POSTGRES_PROPS["driver"]) \
                .mode("overwrite") \
                .save()

            spark = batch_df.sparkSession
            jvm = spark._jvm

            upsert_sql = f"""
                INSERT INTO realtime_top_tracks
                    (window_start, window_end, track_id, stream_count, unique_listeners, updated_at)
                SELECT
                    window_start,
                    window_end,
                    CAST(track_id AS UUID),
                    stream_count,
                    unique_listeners,
                    NOW()
                FROM {staging_table}
                ON CONFLICT (window_start, track_id)
                DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    stream_count = EXCLUDED.stream_count,
                    unique_listeners = EXCLUDED.unique_listeners,
                    updated_at = NOW();
            """

            drop_sql = f"DROP TABLE IF EXISTS {staging_table};"

            conn = jvm.java.sql.DriverManager.getConnection(
                POSTGRES_URL,
                POSTGRES_PROPS["user"],
                POSTGRES_PROPS["password"]
            )

            try:
                stmt = conn.createStatement()
                stmt.execute(upsert_sql)
                stmt.execute(drop_sql)
                stmt.close()
                print(f"[Batch {batch_id}] Données écrites dans realtime_top_tracks.")
            finally:
                conn.close()

    return (
            grouped_df.writeStream
            .outputMode("update")
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", CHECKPOINT_PATH + "/top_tracks")
            .trigger(processingTime="30 seconds")
            .start()
        )


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min glissant toutes les 5 min).

    TODO :
        1. Joindre events_df avec catalog_df (stream-static join sur track_id)
           pour récupérer le genre du morceau
        2. groupBy(window("event_time", "15 minutes", "5 minutes"), "genre")
        3. agg(countDistinct("user_id").alias("unique_listeners"))
        4. Écrire dans Redis (clé "genre_listeners:live") via foreachBatch
           Utiliser redis-py dans le batch

    Hint : charger le catalogue PostgreSQL comme DataFrame statique avec spark.read.jdbc()
    """
    raise NotImplementedError("TODO : implémenter compute_genre_listeners_sliding()")


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    events_df = read_kafka_stream(spark)
    late_events_df = events_df.filter(
        F.col("event_time") < F.current_timestamp() - F.expr("INTERVAL 10 minutes")
    )

    query_late_events = (
        late_events_df
        .selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "late_listening_events")
        .option("checkpointLocation", CHECKPOINT_PATH + "/late_events")
        .start()
    )

    query_top_tracks = compute_top_tracks_tumbling(events_df)

    spark.streams.awaitAnyTermination()



if __name__ == "__main__":
    main()