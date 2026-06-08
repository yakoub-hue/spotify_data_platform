"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [ ] Implémenter build_user_track_matrix()
    [ ] Implémenter compute_recommendations()
    [ ] Implémenter store_recommendations()
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user × track des écoutes des 7 derniers jours.

        TODO :
            1. Requête SQL :
               SELECT user_id, track_id, COUNT(*) as play_count
               FROM listening_events
               WHERE timestamp >= NOW() - INTERVAL '7 days'
                 AND completed = TRUE
               GROUP BY user_id, track_id
            2. Construire un dict {user_id: {track_id: play_count}}
            3. Ne garder que les utilisateurs avec >= 3 écoutes distinctes
            4. Retourner la matrice + la liste des users actifs

        Hint : pandas pivot_table peut aider pour construire la matrice.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records("""
            SELECT
                user_id,
                track_id,
                COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            AND completed = TRUE
            GROUP BY user_id, track_id
        """)

        matrix = {}

        for user_id, track_id, play_count in rows:
            user_id = str(user_id)
            track_id = str(track_id)

            if user_id not in matrix:
                matrix[user_id] = {}

            matrix[user_id][track_id] = int(play_count)

        # garder seulement les users avec au moins 3 tracks distincts
        matrix = {
            user_id: tracks
            for user_id, tracks in matrix.items()
            if len(tracks) >= 3
        }

        print(f"Utilisateurs actifs retenus : {len(matrix)}")

        return {
            "matrix": matrix,
            "users": list(matrix.keys()),
        }

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Calcule les recommandations par similarité cosinus.

        TODO :
            1. Convertir la matrice en numpy array ou DataFrame sparse
            2. Calculer la similarité cosinus entre utilisateurs
               (sklearn.metrics.pairwise.cosine_similarity)
            3. Pour chaque user : trouver ses TOP_N voisins les plus similaires
            4. Recommander les tracks que ses voisins ont aimés mais qu'il n'a pas écoutés
            5. Retourner {user_id: [track_id_1, track_id_2, ...]} (top TOP_N_RECO)

        Hint : scipy.sparse.csr_matrix pour gérer les grandes matrices efficacement.
        """
        import math
        from collections import defaultdict

        matrix = matrix_data.get("matrix", {})
        users = matrix_data.get("users", [])

        if not matrix or len(users) < 2:
            print("Pas assez d'utilisateurs pour générer des recommandations")
            return {}

        def cosine_similarity(profile_a: dict, profile_b: dict) -> float:
            common_tracks = set(profile_a.keys()) & set(profile_b.keys())

            if not common_tracks:
                return 0.0

            dot_product = sum(profile_a[t] * profile_b[t] for t in common_tracks)
            norm_a = math.sqrt(sum(v * v for v in profile_a.values()))
            norm_b = math.sqrt(sum(v * v for v in profile_b.values()))

            if norm_a == 0 or norm_b == 0:
                return 0.0

            return dot_product / (norm_a * norm_b)

        recommendations = {}

        for user_id in users:
            user_profile = matrix[user_id]
            user_tracks = set(user_profile.keys())

            similarities = []

            for other_user_id in users:
                if other_user_id == user_id:
                    continue

                score = cosine_similarity(user_profile, matrix[other_user_id])

                if score > 0:
                    similarities.append((other_user_id, score))

            similarities.sort(key=lambda x: x[1], reverse=True)
            nearest_neighbors = similarities[:10]

            candidate_scores = defaultdict(float)

            for neighbor_id, similarity_score in nearest_neighbors:
                neighbor_profile = matrix[neighbor_id]

                for track_id, play_count in neighbor_profile.items():
                    if track_id not in user_tracks:
                        candidate_scores[track_id] += similarity_score * play_count

            ranked_tracks = sorted(
                candidate_scores.items(),
                key=lambda x: x[1],
                reverse=True
            )

            recommendations[user_id] = [
                {
                    "track_id": track_id,
                    "score": round(score, 6),
                }
                for track_id, score in ranked_tracks[:TOP_N_RECO]
            ]

        recommendations = {
            user_id: recos
            for user_id, recos in recommendations.items()
            if recos
        }

        print(f"Recommandations générées pour {len(recommendations)} utilisateurs")

        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis et PostgreSQL.

        TODO :
            1. Redis : pour chaque user_id :
               redis.setex(f'reco:{user_id}', RECO_TTL_SECONDS, json.dumps(track_ids))
            2. PostgreSQL : UPSERT dans recommendations
               INSERT INTO recommendations (user_id, track_id, score, generated_at)
               VALUES ... ON CONFLICT (user_id, track_id) DO UPDATE SET score=..., generated_at=NOW()
            3. Retourner {"users_with_recos": N, "total_recommendations": M}
        """
        import json
        import redis
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not recommendations:
            print("Aucune recommandation à stocker")
            return {
                "users_with_recos": 0,
                "total_recommendations": 0,
            }

        redis_client = redis.from_url(REDIS_URL, decode_responses=True)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        users_with_recos = 0
        total_recommendations = 0

        for user_id, recos in recommendations.items():
            track_ids = [reco["track_id"] for reco in recos]

            redis_client.setex(
                f"reco:{user_id}",
                RECO_TTL_SECONDS,
                json.dumps(track_ids)
            )

            users_with_recos += 1

            for reco in recos:
                cur.execute("""
                    INSERT INTO recommendations (
                        user_id,
                        track_id,
                        score,
                        generated_at
                    )
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, track_id) DO UPDATE SET
                        score = EXCLUDED.score,
                        generated_at = NOW()
                """, (
                    user_id,
                    reco["track_id"],
                    reco["score"],
                ))

                total_recommendations += 1

        conn.commit()
        cur.close()
        conn.close()

        print(f"Utilisateurs avec recommandations : {users_with_recos}")
        print(f"Recommandations totales : {total_recommendations}")

        return {
            "users_with_recos": users_with_recos,
            "total_recommendations": total_recommendations,
        }

    # ── Orchestration ─────────────────────────────────────────
    matrix        = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
