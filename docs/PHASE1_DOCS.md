# Validation Issue #1 - Setup Docker Compose et vérification de la stack

## Airflow UI

![Airflow UI](../screenshots/Airflow_UI.png)

## docker compose ps

![docker services](../screenshots/Docker_services.png)

## Conclusion

L'environnement docker est opérationnel et conforme aux critères de validation de l'Issue #1.

# Validation Issue #2 - Schéma PostgreSQL et modèle de données SPOTIFY

## ERD + Data model

[ERD & Data model](docs/DATA_MODEL.md)

## Architecture

[Architecture](docs/ARCHITECTURE.md)

## Conclusion

Le schéma PostgreSQL et le modèle de données SPOTIFY ont été mis en place et sont conformes aux critères de validation de l'Issue #2.

# Validation Issue #3 - Data Generator : catalogue musical avec Faker

## Tests

![Tests](../screenshots/Tests_Success.png)

## Conclusion

Le Data Generator pour le catalogue musical utilisant Faker a été implémenté et est conforme aux critères de validation de l’Issue #3.


# Validation Issue #4 - DAG catalog_ingestion_pipeline

# DAGRun vert

![DAGrun](../screenshots/DAGrun_Airflow.png)

# pytest passed

![pytest](../screenshots/pytest.png)

## Conclusion

Le DAG catalog_ingestion_pipeline a été mis en place et est conforme aux critères de validation de l’Issue #4.

# Validation Issue #5 - Simulateur P2P : compléter et lancer

# Evenets JSON

![events json](../screenshots/events_json.png)

## Conclusion

le simulateur a été complété et lancé avec succès, conforme aux critères de validation de l’Issue #5.

# Validation Issue #6 - DAG streaming_events_pipeline

# DAGrun

![DAGrun](../screenshots/DAGrun_streaming.png)

# MINIO Parquet

![Minio parquet page 1](../screenshots/minio_parquet_1.jpeg)
![Minio parquet page 2](../screenshots/minio_parquet_2.jpeg)

# Count events

![Count events](../screenshots/events_postgres.jpeg)

## Conclusion

le DAG a été implémenté et exécuté correctement, conforme aux critères de validation de l’Issue #6.

# Validation Issue #7 - DAG aggregation_pipeline + stockage MinIO

# Daily streams

![daily streams](../screenshots/daily_streams.jpeg)

## Conclusion

le pipeline d’agrégation ainsi que le stockage dans MinIO ont été mis en place et validés issue #7.

# Validation Issue #8 - DAG recommendation_pipeline

# Track IDs

![Track IDs](../screenshots/track_id.jpeg)

## Conclusion

le pipeline de recommandation a été développé et intégré avec succès, conforme aux critères de validation de l’Issue #8.

# Validation Issue #9 - DAG dlq_reprocessing_pipeline

# Statut transition

![Statut transition](../screenshots/statut_transition.jpeg)

## Conclusion

le DAG de reprocessing de la DLQ a été implémenté et fonctionne conformément aux critères de validation de l’Issue #9.

# Validation Issue #10 - Tests pytest + README + doc_md

# Tests

![Tests](../screenshots/tests.png)

# README

[README](../README.md)

## Conclusion

les tests, le README et la documentation ont été ajoutés et validés selon les exigences de l’Issue #10.
