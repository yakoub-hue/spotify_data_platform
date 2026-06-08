"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task



DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger
logger = logging.getLogger(__name__)

with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """
        Récupère les événements en attente de retraitement.

        TODO :
            1. Utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            2. Requête :
               SELECT id, payload, error_type, retry_count, original_topic
               FROM dead_letter_events
               WHERE status = 'pending'
                 AND retry_count < %(max_retries)s
               ORDER BY created_at ASC
               LIMIT %(batch_size)s
            3. Retourner la liste des events à retraiter
            4. Logger : "X événements pending trouvés"
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records(
            """
            SELECT
                id,
                payload,
                error_type,
                COALESCE(retry_count, 0) AS retry_count,
                original_topic,
                created_at
            FROM dead_letter_events
            WHERE COALESCE(status, 'pending') = 'pending'
              AND COALESCE(retry_count, 0) < %(max_retries)s
            ORDER BY created_at ASC
            LIMIT %(batch_size)s
            """,
            parameters={
                "max_retries": MAX_RETRIES,
                "batch_size": BATCH_SIZE,
            },
        )

        events = [
            {
                "dlq_id": str(row[0]),
                "payload": row[1],
                "error_type": row[2],
                "retry_count": int(row[3]),
                "original_topic": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
            }
            for row in rows
        ]

        logger.info(f"Événements DLQ pending trouvés : {len(events)}")
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """
        Tente de corriger et réinjecter chaque événement défectueux.

        TODO :
            1. Pour chaque event, parser le payload JSON
            2. Tenter la validation des champs obligatoires
            3. Tenter la correction si possible :
               - user_id manquant → impossible à corriger → abandoned
               - timestamp invalide → utiliser created_at comme fallback
               - track_id inconnu → vérifier dans tracks, si absent → abandoned
            4. Si valide : préparer pour réinsertion dans listening_events
            5. Retourner {"reprocessed": [...], "failed": [...]}
        """
        import json
        from datetime import datetime
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not pending_events:
            print("Aucun événement DLQ à retraiter")
            return {"reprocessed": [], "failed": []}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        reprocessed = []
        failed = []

        for item in pending_events:
            dlq_id = item["dlq_id"]

            try:
                payload = item["payload"]

                if isinstance(payload, str):
                    event = json.loads(payload)
                elif isinstance(payload, dict):
                    event = payload
                else:
                    raise ValueError("payload_format_invalid")

                required_fields = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

                missing_fields = [
                    field for field in required_fields
                    if field not in event or event[field] in [None, ""]
                ]

                if missing_fields:
                    logger.warning(
                     f"Event {dlq_id}: champs manquants {missing_fields}"
                    )
                    failed.append({
                        "dlq_id": dlq_id,
                        "reason": f"missing_fields:{','.join(missing_fields)}",
                    })
                    continue

                try:
                    duration_ms = int(event["duration_ms"])
                    if duration_ms <= 0:
                        raise ValueError()
                except Exception:
                    logger.warning(
                        f"Event {dlq_id}: duration_ms invalide"
                    )
                    failed.append({
                        "dlq_id": dlq_id,
                        "reason": "invalid_duration_ms",
                    })
                    continue

                try:
                    datetime.fromisoformat(str(event["timestamp"]).replace("Z", "+00:00"))
                except Exception:
                    if item.get("created_at"):
                        event["timestamp"] = item["created_at"]
                    else:
                        logger.warning(
                            f"Event {dlq_id}: timestamp invalide"
                        )
                        failed.append({
                            "dlq_id": dlq_id,
                            "reason": "invalid_timestamp",
                        })
                        continue

                cur.execute(
                    """
                    SELECT id
                    FROM tracks
                    WHERE id = %s::uuid
                    """,
                    (event["track_id"],),
                )

                if cur.fetchone() is None:
                    logger.warning(
                        f"Event {dlq_id}: track_id '{event['track_id']}' inconnu"
                    )
                    failed.append({
                        "dlq_id": dlq_id,
                        "reason": "unknown_track",
                    })
                    continue

                reprocessed.append({
                    "dlq_id": dlq_id,
                    "event": event,
                })

            except Exception as e:
                failed.append({
                    "dlq_id": dlq_id,
                    "reason": str(e),
                })

        cur.close()
        conn.close()

        logger.info(f"DLQ retraitables : {len(reprocessed)}")
        logger.info(f"DLQ échoués : {len(failed)}")

        return {
            "reprocessed": reprocessed,
            "failed": failed,
        }

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """
        Met à jour le statut des événements dans dead_letter_events.

        TODO :
            1. Pour les events retraités avec succès :
               - INSERT dans listening_events
               - UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW()
            2. Pour les events échoués :
               - UPDATE dead_letter_events
                 SET retry_count = retry_count + 1,
                     last_retry_at = NOW(),
                     status = CASE WHEN retry_count + 1 >= 3 THEN 'abandoned' ELSE 'pending' END
            3. Logger le bilan : "X retraités, Y abandonnés, Z encore en pending"
            4. Retourner les stats
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        reprocessed = results.get("reprocessed", [])
        failed = results.get("failed", [])

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        inserted = 0
        reprocessed_count = 0
        failed_count = 0
        abandoned_count = 0

        for item in reprocessed:
            dlq_id = item["dlq_id"]
            event = item["event"]

            cur.execute(
                """
                INSERT INTO listening_events (
                    id,
                    user_id,
                    track_id,
                    timestamp,
                    duration_ms,
                    device_type,
                    geo_country,
                    completed,
                    event_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    event["event_id"],
                    event["user_id"],
                    event["track_id"],
                    event["timestamp"],
                    event["duration_ms"],
                    event.get("device_type"),
                    event.get("geo_country"),
                    event.get("completed", False),
                    event.get("event_source", "dlq_reprocessed"),
                ),
            )

            if cur.rowcount > 0:
                inserted += 1

            cur.execute(
                """
                UPDATE dead_letter_events
                SET status = 'reprocessed',
                    resolved_at = NOW(),
                    last_retry_at = NOW()
                WHERE id = %s
                """,
                (dlq_id,),
            )

            reprocessed_count += 1

        for item in failed:
            dlq_id = item["dlq_id"]

            cur.execute(
                """
                UPDATE dead_letter_events
                SET retry_count = COALESCE(retry_count, 0) + 1,
                    last_retry_at = NOW(),
                    status = CASE
                        WHEN COALESCE(retry_count, 0) + 1 >= %s
                        THEN 'abandoned'
                        ELSE 'pending'
                    END
                WHERE id = %s
                RETURNING status
                """,
                (MAX_RETRIES, dlq_id),
            )

            new_status = cur.fetchone()[0]

            failed_count += 1
            if new_status == "abandoned":
                abandoned_count += 1

        conn.commit()
        cur.close()
        conn.close()
        

        pending_count = failed_count - abandoned_count

        logger.info("📊 Bilan DLQ retraitement :")
        logger.info(f"✅ Retraités : {reprocessed_count}")
        logger.info(f"⚠️ Abandonnés : {abandoned_count}")
        logger.info(f"ℹ️ Encore pending : {pending_count}")
        stats = {
            "inserted_into_listening_events": inserted,
            "reprocessed": reprocessed_count,
            "failed": failed_count,
            "abandoned": abandoned_count,
        }

        logger.info(f"Bilan DLQ : {stats}")
        return stats

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
