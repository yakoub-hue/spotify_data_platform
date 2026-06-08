"""
DAG : late_events_reprocessing
==================================
Consomme le topic Kafka late_listening_events pour réintégrer les événements tardifs
qui ont été routés et ignorés par Spark Streaming. Réinitialise les agrégats batch.
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## late_events_reprocessing

### Rôle
Consomme et réintègre les événements d'écoute tardifs du topic Kafka late_listening_events.
Recalcule les statistiques quotidiennes (daily_streams) pour les dates affectées.

### Architecture Lambda
- Speed Layer : Spark détecte les late events (>10 min watermark)
- Batch Layer : Airflow recalcule les agrégats
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
KAFKA_BOOTSTRAP = "kafka-1:9092,kafka-2:9094,kafka-3:9096"
TOPIC = "late_listening_events"


with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements d'écoute tardifs",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-3", "late-events", "lambda"],
    doc_md=DAG_DOC,
) as dag:

    # ----------------------------
    # 1. CONSUME + INSERT
    # ----------------------------
    @task(task_id="consume_and_insert_late_events")
    def consume_and_insert_late_events() -> list:
        logger = logging.getLogger(__name__)
        from confluent_kafka import Consumer, KafkaError

        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "late_events_airflow_consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        consumer = Consumer(conf)
        consumer.subscribe([TOPIC])

        logger.info(f"Consommation du topic {TOPIC}")

        events = []
        max_messages = 500
        timeout_seconds = 5

        start_time = datetime.now()

        while len(events) < max_messages:
            msg = consumer.poll(1.0)

            if msg is None:
                if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(msg.error())
                break

            try:
                events.append(json.loads(msg.value().decode("utf-8")))
            except Exception as e:
                logger.error(f"JSON error: {e}")

        consumer.close()

        logger.info(f"{len(events)} late events reçus")

        if not events:
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT id FROM tracks")
        valid_tracks = set(str(r[0]) for r in cur.fetchall())

        affected_dates = set()
        inserted = 0

        try:
            for event in events:
                event_id = event.get("event_id")
                user_id = event.get("user_id")
                track_id = event.get("track_id")
                source_peer = event.get("source_peer")
                ts = event.get("timestamp")

                if not all([event_id, user_id, track_id, ts]):
                    continue

                if track_id not in valid_tracks:
                    continue

                clean_ts = datetime.fromisoformat(ts.replace("Z", ""))

                try:
                    cur.execute("""
                        INSERT INTO listening_events (
                            id, user_id, track_id, source_peer_id,
                            timestamp, duration_ms, device_type,
                            geo_country, completed, event_source
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (id) DO NOTHING
                    """, (
                        event_id,
                        user_id,
                        track_id,
                        source_peer,
                        clean_ts,
                        event.get("duration_ms", 0),
                        event.get("device_type"),
                        event.get("geo_country"),
                        event.get("completed", False),
                        event.get("event_source", "late_reprocessing"),
                    ))

                    inserted += 1
                    affected_dates.add(clean_ts.date().strftime("%Y-%m-%d"))

                except Exception as e:
                    logger.warning(f"Insert error {event_id}: {e}")

            conn.commit()
            logger.info(f"{inserted} events insérés")

            return list(affected_dates)

        finally:
            cur.close()
            conn.close()

    # ----------------------------
    # 2. RECOMPUTE AGGREGATES
    # ----------------------------
    @task(task_id="recalculate_aggregates")
    def recalculate_aggregates(affected_dates: list):
        logger = logging.getLogger(__name__)

        if not affected_dates:
            logger.info("Aucune date affectée")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        for date in affected_dates:
            logger.info(f"Recalcul {date}")

            query = """
                INSERT INTO daily_streams (
                    track_id, date, total_streams,
                    unique_listeners, total_duration_ms,
                    countries, updated_at
                )
                SELECT
                    track_id,
                    DATE(timestamp),
                    COUNT(*),
                    COUNT(DISTINCT user_id),
                    COALESCE(SUM(duration_ms), 0),
                    ARRAY_AGG(DISTINCT geo_country),
                    NOW()
                FROM listening_events
                WHERE DATE(timestamp) = %s
                  AND completed = TRUE
                GROUP BY track_id, DATE(timestamp)
                ON CONFLICT (track_id, date)
                DO UPDATE SET
                    total_streams = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries = EXCLUDED.countries,
                    updated_at = NOW();
            """

            hook.run(query, parameters=(date,))

        logger.info("Recalcul terminé")

    # ----------------------------
    # ORCHESTRATION PROPRE
    # ----------------------------
    late_events = consume_and_insert_late_events()
    recalculate_aggregates(late_events)