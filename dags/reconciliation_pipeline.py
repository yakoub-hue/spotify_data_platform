"""
DAG : reconciliation_pipeline
Compare les agrégats batch daily_streams avec les agrégats streaming realtime_top_tracks.
Calcule un taux de divergence par track et stocke le résultat dans reconciliation_report.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "spotify_postgres"


DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}


DAG_DOC = """
## reconciliation_pipeline

Ce DAG compare les données batch de `daily_streams` avec les données temps réel de `realtime_top_tracks`.

Il calcule pour chaque `track_id` :

- le nombre de streams batch
- le nombre de streams streaming
- le taux de divergence
- une alerte si la divergence dépasse 5 %
"""


with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Réconciliation Batch vs Streaming",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "reconciliation"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="init_reconciliation_table")
    def init_reconciliation_table():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        hook.run("""
            CREATE TABLE IF NOT EXISTS reconciliation_report (
                id SERIAL PRIMARY KEY,
                track_id UUID,
                batch_streams BIGINT,
                realtime_streams BIGINT,
                divergence_pct DOUBLE PRECISION,
                alert BOOLEAN,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

        logging.info("✅ Table reconciliation_report initialisée.")

    @task(task_id="run_reconciliation")
    def run_reconciliation(**context):
        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Tes données batch sont visibles au 2026-06-02.
        # On force cette date pour valider l'issue avec les données existantes.
        reco_date = datetime(2026, 6, 2).date()

        logger.info(f"Début de la réconciliation pour la date : {reco_date}")

        query = """
            WITH batch_data AS (
                SELECT
                    track_id,
                    total_streams AS batch_streams
                FROM daily_streams
                WHERE date = %s
            ),
            realtime_data AS (
                SELECT
                    track_id,
                    SUM(stream_count) AS realtime_streams
                FROM realtime_top_tracks
                WHERE CAST(window_start AS DATE) = %s
                GROUP BY track_id
            )
            SELECT
                COALESCE(b.track_id, r.track_id) AS track_id,
                COALESCE(b.batch_streams, 0) AS batch_streams,
                COALESCE(r.realtime_streams, 0) AS realtime_streams
            FROM batch_data b
            FULL OUTER JOIN realtime_data r
                ON b.track_id = r.track_id;
        """

        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            cur.execute(query, (reco_date, reco_date))
            rows = cur.fetchall()

            logger.info(f"{len(rows)} tracks trouvés pour la réconciliation.")

            inserts = []
            alerts = []

            for track_id, batch_streams, realtime_streams in rows:
                batch_streams = int(batch_streams or 0)
                realtime_streams = int(realtime_streams or 0)

                max_value = max(batch_streams, realtime_streams)

                if max_value == 0:
                    divergence_pct = 0.0
                else:
                    divergence_pct = abs(batch_streams - realtime_streams) / max_value

                alert = divergence_pct > 0.05

                inserts.append((
                    track_id,
                    batch_streams,
                    realtime_streams,
                    divergence_pct,
                    alert
                ))

                if alert:
                    alerts.append(
                        f"⚠️ Track {track_id} divergence={divergence_pct:.2%} "
                        f"batch={batch_streams}, realtime={realtime_streams}"
                    )

            if inserts:
                cur.execute("DELETE FROM reconciliation_report;")

                insert_query = """
                    INSERT INTO reconciliation_report
                        (track_id, batch_streams, realtime_streams, divergence_pct, alert)
                    VALUES (%s, %s, %s, %s, %s);
                """

                cur.executemany(insert_query, inserts)
                conn.commit()

                logger.info(f"✅ {len(inserts)} lignes insérées dans reconciliation_report.")

            if alerts:
                logger.warning(f"⚠️ {len(alerts)} alertes détectées avec divergence > 5%.")
                for alert_msg in alerts[:20]:
                    logger.warning(alert_msg)
            else:
                logger.info("✅ Aucune divergence supérieure à 5% détectée.")
            return {
                "reconciled_tracks": len(inserts),
                "alerts_triggered": len(alerts),
            }

        finally:
            cur.close()
            conn.close()

    init_reconciliation_table() >> run_reconciliation()