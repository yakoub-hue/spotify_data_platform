# Spotify Data Platform

## Spotify Data Platform — Phase 1

Plateforme de data engineering simulant un service de streaming musical avec ingestion catalogue, événements d’écoute, agrégations, recommandations et retraitement DLQ.

### Architecture Phase 1

```text
Data Generator → MinIO → Airflow DAGs → PostgreSQL
P2P Simulator → Redis → streaming_events_pipeline → PostgreSQL / MinIO
PostgreSQL → aggregation_pipeline → daily_streams / artist_stats / realtime_top_tracks
PostgreSQL → recommendation_pipeline → Redis / recommendations
dead_letter_events → dlq_reprocessing_pipeline → listening_events / status update
```

### Fonctionnalités implémentées

- Setup Docker Compose
- Schéma PostgreSQL complet
- Générateur de données Faker
- DAG catalog_ingestion_pipeline
- Simulateur P2P avec Redis
- DAG streaming_events_pipeline
- DAG aggregation_pipeline
- DAG recommendation_pipeline
- DAG dlq_reprocessing_pipeline
- Tests unitaires et tests de structure DAGs
- Documentation doc_md sur les DAGs

### Lancer le projet
```Bash
docker compose up -d
```

Interfaces: 

- Airflow : http://localhost:8080
- MinIO   : http://localhost:9001
- Postgres: localhost:5432
- Redis   : localhost:6379

### Lancer le simulateur P2P
```Bash
python3 src/p2p_simulator/simulator.py
```

### Lancer les tests

Tests unitaires :
```Bash
python3 -m pytest tests/unit/test_transformations.py -v
```
Tests de structure Airflow, à lancer dans le conteneur Airflow :
```Bash
docker exec -it spotify-m1-airflow-worker-1 bash

pytest tests/structure/test_dag_structure.py -v
```

Résultats obtenus :
- 18 passed: tests unitaires
- 16 passed: tests structure DAGs16 passed — tests structure DAGs

### DAGs Phase 1

- catalog_ingestion_pipeline
- streaming_events_pipeline
- aggregation_pipeline
- recommendation_pipeline
- dlq_reprocessing_pipeline

### Validation Phase 1

- Les 5 DAGs sont importés par Airflow.
- Le simulateur P2P publie des événements dans Redis.
- Les événements valides sont stockés dans PostgreSQL et MinIO.
- Les événements invalides sont routés vers la DLQ.
- Les agrégations et recommandations sont générées.
- La DLQ est retraitée avec gestion des statuts pending, reprocessed et abandoned.

## Spotify Data Platform — Phase 2

Cette phase introduit une architecture orientée streaming basée sur Kafka, Spark Structured Streaming et l’architecture Lambda afin de traiter les événements en temps réel, détecter les fraudes et gérer les événements tardifs.

### Architecture Phase 2
P2P Simulator → Kafka → Spark Structured Streaming → PostgreSQL / MinIO

   Kafka Topics :
     - listening_events
     - p2p_network_events
     - enriched_events
     - fraud_alerts
     - late_listening_events

    Spark Jobs :
     - streaming_enrichment_job
     - fraud_detection_job
     - late_events_router

PostgreSQL / MinIO → Airflow DAGs → Reconciliation & Late Events Processing
### Fonctionnalités implémentées
  - Setup Kafka et Spark Structured Streaming
  - Publication des événements dans Kafka
  - Gestion des topics Kafka dédiés
  - Exactly Once Processing
  - Streaming Enrichment des événements
  - Jointure avec le catalogue PostgreSQL
  - Jointure avec les événements P2P
  - Déduplication des événements
  - Détection de fraude en temps réel
  - Gestion des événements tardifs
  - Routage des événements tardifs vers Kafka
  - DAG reconciliation_pipeline
  - DAG late_events_reprocessing_pipeline
### Kafka Topics
  - listening_events
  - p2p_network_events
  - enriched_events
  - fraud_alerts
  - late_listening_events
### Jobs Spark Phase 2
  - streaming_enrichment_job.py
  - fraud_detection_job.py
  - late_events_router.py
### DAGs Phase 2
  - reconciliation_pipeline
  - late_events_reprocessing_pipeline

### Fonctionnalités temps réel
#### Streaming Enrichment

Les événements d'écoute sont enrichis en temps réel avec :
  - Titre du morceau
  - Artiste
  - Genre
  - Pays de l'artiste

Les événements P2P sont également corrélés avec les écoutes afin d'ajouter du contexte réseau aux analyses.

#### Fraud Detection

Détection automatique des comportements suspects :

  - Burst Listening
  - Short Duration Bot
  - P2P Failure Rate

Les alertes générées sont publiées dans Kafka et stockées dans PostgreSQL.

#### Exactly Once Processing
Producteur Kafka idempotent
Transactions Kafka activées
Consumer Spark configuré avec read_committed
Prévention des doublons lors des redémarrages

#### Late Events Processing
Détection des événements tardifs
Routage dans le topic late_listening_events
Retraitement via Airflow
Réinjection dans listening_events
Recalcul des agrégats impactés

#### Reconciliation Pipeline
Comparaison entre :
  - Batch Layer (daily_streams)
  - Speed Layer (realtime_top_tracks)

Le pipeline :
  - Calcule le taux de divergence
  - Génère un rapport de réconciliation
  - Détecte les écarts supérieurs à 5 %

### Validation Phase 2
  - Kafka opérationnel
  - Spark Structured Streaming opérationnel
  - Topics Kafka créés et alimentés
  - Exactly Once Processing validé
  - Enrichissement temps réel validé
  - Détection de fraude validée
  - Réconciliation Batch / Streaming validée
  - Gestion des événements tardifs validée
  - Réinjection des événements tardifs validée
  - Recalcul des agrégats validé

### Résultat global
La plateforme couvre désormais :

#### Batch Layer
  - Ingestion catalogue
  - Agrégations
  - Recommandations
  - DLQ

#### Speed Layer
  - Kafka Streaming
  - Spark Structured Streaming
  - Enrichissement temps réel
  - Détection de fraude
  - Gestion des événements tardifs

#### Serving Layer
  - PostgreSQL
  - Redis
  - MinIO

L’architecture Lambda est désormais opérationnelle avec traitement batch, streaming temps réel, détection de fraude, réconciliation des données et retraitement des événements tardifs.
