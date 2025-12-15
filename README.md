# Projet SmartTech : Streaming de Données IoT

## Vue d'ensemble

Ce projet implémente des pipelines de streaming pour traiter des données IoT en temps réel avec Apache Spark Structured Streaming, Kafka et Delta Lake.

## Architecture

- **Bronze Layer** : Données brutes ingérées depuis des fichiers JSON
- **Silver Layer** : Données nettoyées et normalisées depuis Kafka
- **Gold Layer** : Données agrégées pour l'analyse (à venir)

## Prérequis

- Docker et Docker Compose installés
- WSL Ubuntu (pour les commandes Linux)
- Windows 10/11

## Structure du projet

```
MyStream/
├── data/                  # Données d'entrée (JSON)
├── delta/                 # Tables Delta Lake
│   ├── bronze/           # Niveau Bronze
│   └── silver/           # Niveau Silver
├── checkpoints/          # Checkpoints Spark pour la tolérance aux pannes
├── notebooks/            # Notebooks Jupyter
│   ├── 01_pipeline_bronze.ipynb
│   └── 02_pipeline_silver.ipynb
├── scripts/              # Scripts utilitaires
│   └── kafka_producer.py
├── docker-compose.yml    # Configuration Docker
├── Dockerfile            # Image Spark personnalisée
├── requirements.txt      # Dépendances Python
└── roadmap.md           # Roadmap du projet
```

## Installation et démarrage

### 1. Démarrer les services Spark

```bash
docker-compose up -d
```

### 2. Vérifier que les services sont démarrés

```bash
docker-compose ps
```

### 3. Accéder à l'interface Spark

- Spark Master UI : http://localhost:8080

### 4. Démarrer Jupyter Notebook

```bash
docker exec -it spark-master jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root
```

Puis accéder à : http://localhost:8888

## Utilisation

### Pipeline Bronze (JSON → Delta Lake)

1. Placer les fichiers JSON dans le dossier `data/`
2. Ouvrir le notebook `notebooks/01_pipeline_bronze.ipynb`
3. Exécuter les cellules dans l'ordre

### Pipeline Silver (Kafka → Delta Lake)

1. Démarrer Kafka (voir section Kafka)
2. Lancer le simulateur de capteurs : `python scripts/kafka_producer.py`
3. Ouvrir le notebook `notebooks/02_pipeline_silver.ipynb`
4. Exécuter les cellules dans l'ordre

## Configuration Kafka

Pour la Phase 2.2, vous devrez installer et configurer Kafka séparément ou utiliser un conteneur Kafka.

## Documentation

Voir `roadmap.md` pour le détail des étapes du projet.

## Auteur

Projet SmartTech - Streaming de Données IoT

