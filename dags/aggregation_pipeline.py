"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL

TODO :
    [ ] Implémenter compute_top_tracks()
    [ ] Implémenter compute_artist_stats()
    [ ] Implémenter compute_p2p_metrics()
    [ ] Implémenter update_aggregates()
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...

### TODO
Compléter les 4 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"

def _target_date(context) -> str:
    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf and dag_run.conf.get("date"):
        return dag_run.conf["date"]

    return context["data_interval_start"].strftime("%Y-%M-%D")


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )


    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution.

        TODO :
            1. Récupérer execution_date depuis context["data_interval_start"]
            2. Requête SQL :
               SELECT track_id,
                      COUNT(*) as total_streams,
                      COUNT(DISTINCT user_id) as unique_listeners,
                      SUM(duration_ms) as total_duration_ms,
                      ARRAY_AGG(DISTINCT geo_country) as countries
               FROM listening_events
               WHERE DATE(timestamp) = %(date)s AND completed = TRUE
               GROUP BY track_id
               ORDER BY total_streams DESC
               LIMIT 50
            3. Retourner la liste des agrégats
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        date = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            """
            SELECT
                track_id,
                COUNT(*) AS total_streams,
                COUNT(DISTINCT user_id) AS unique_listeners,
                COALESCE(SUM(duration_ms), 0) AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country)
                    FILTER (WHERE geo_country IS NOT NULL) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
            """,
            parameters={"date": date},
        )

        top_tracks = [
            {
                "track_id": str(row[0]),
                "total_streams": int(row[1]),
                "unique_listeners": int(row[2]),
                "total_duration_ms": int(row[3]),
                "countries": list(row[4]) if row[4] else [],
            }
            for row in rows
        ]

        print(f"Top tracks calculés pour {date} : {len(top_tracks)}")
        return top_tracks

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.

        TODO :
            1. Jointure listening_events × tracks × artists
            2. GROUP BY artist_id, date
            3. Métriques : total_streams, unique_listeners, top_track_id
            4. Retourner la liste des stats artistes
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        date = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            """
            WITH track_counts AS (
                SELECT
                    t.artist_id,
                    le.track_id,
                    COUNT(*) AS streams
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %(date)s
                  AND le.completed = TRUE
                GROUP BY t.artist_id, le.track_id
            ),
            artist_totals AS (
                SELECT
                    t.artist_id,
                    COUNT(*) AS total_streams,
                    COUNT(DISTINCT le.user_id) AS unique_listeners
                FROM listening_events le
                JOIN tracks t ON le.track_id = t.id
                WHERE DATE(le.timestamp) = %(date)s
                  AND le.completed = TRUE
                GROUP BY t.artist_id
            ),
            top_per_artist AS (
                SELECT DISTINCT ON (artist_id)
                    artist_id,
                    track_id AS top_track_id
                FROM track_counts
                ORDER BY artist_id, streams DESC
            )
            SELECT
                a.artist_id,
                a.total_streams,
                a.unique_listeners,
                tp.top_track_id
            FROM artist_totals a
            LEFT JOIN top_per_artist tp ON a.artist_id = tp.artist_id
            ORDER BY a.total_streams DESC
            """,
            parameters={"date": date},
        )

        artist_stats = [
            {
                "artist_id": str(row[0]),
                "total_streams": int(row[1]),
                "unique_listeners": int(row[2]),
                "top_track_id": str(row[3]) if row[3] else None,
            }
            for row in rows
        ]

        print(f"Stats artistes calculées pour {date} : {len(artist_stats)}")
        return artist_stats
    
    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.

        TODO :
            1. Taux de cache_hit (event_source='cache' / total)
            2. Latence moyenne des transferts P2P
            3. Nombre de peers actifs uniques
            4. Distribution des écoutes par device_type et geo_country
            5. Retourner un dict de métriques
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        date = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        source_rows = hook.get_records(
            """
            SELECT event_source, COUNT(*)
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY event_source
            """,
            parameters={"date": date},
        )

        device_rows = hook.get_records(
            """
            SELECT device_type, COUNT(*)
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY device_type
            """,
            parameters={"date": date},
        )

        country_rows = hook.get_records(
            """
            SELECT geo_country, COUNT(*)
            FROM listening_events
            WHERE DATE(timestamp) = %(date)s
            GROUP BY geo_country
            """,
            parameters={"date": date},
        )

        source_distribution = {
            str(row[0]): int(row[1]) for row in source_rows if row[0] is not None
        }

        total_sources = sum(source_distribution.values())
        cache_hits = source_distribution.get("cache", 0)

        cache_hit_rate = round(cache_hits / total_sources, 4) if total_sources else 0.0

        metrics = {
            "date": date,
            "cache_hit_rate": cache_hit_rate,
            "event_source_distribution": source_distribution,
            "device_distribution": {
                str(row[0]): int(row[1]) for row in device_rows if row[0] is not None
            },
            "country_distribution": {
                str(row[0]): int(row[1]) for row in country_rows if row[0] is not None
            },
        }

        print(f"Métriques P2P calculées pour {date}")
        print(metrics)

        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.

        TODO :
            1. UPSERT dans daily_streams :
               INSERT INTO daily_streams (track_id, date, total_streams, ...)
               VALUES ... ON CONFLICT (track_id, date) DO UPDATE SET ...
            2. UPSERT dans artist_stats
            3. Logger les stats : "Top track: {title} avec {N} streams"
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        date = _target_date(context)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        for track in top_tracks:
            cursor.execute(
                """
                INSERT INTO daily_streams (
                    track_id,
                    date,
                    total_streams,
                    unique_listeners,
                    total_duration_ms,
                    countries,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries = EXCLUDED.countries,
                    updated_at = NOW()
                """,
                (
                    track["track_id"],
                    date,
                    track["total_streams"],
                    track["unique_listeners"],
                    track["total_duration_ms"],
                    track["countries"],
                ),
            )

        for artist in artist_stats:
            cursor.execute(
                """
                INSERT INTO artist_stats (
                    artist_id,
                    date,
                    total_streams,
                    unique_listeners,
                    top_track_id,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id = EXCLUDED.top_track_id,
                    updated_at = NOW()
                """,
                (
                    artist["artist_id"],
                    date,
                    artist["total_streams"],
                    artist["unique_listeners"],
                    artist["top_track_id"],
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Agrégats écrits pour {date}")
        print(f"Tracks écrites : {len(top_tracks)}")
        print(f"Artistes écrits : {len(artist_stats)}")
        print(f"Métriques P2P : {p2p_metrics}")

        return {
            "date": date,
            "tracks_written": len(top_tracks),
            "artists_written": len(artist_stats),
            "p2p_metrics": p2p_metrics,
        }

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
 
