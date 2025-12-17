# Projet SmartTech : Streaming de Données IoT

## Vue d'ensemble

Ce projet implémente des pipelines de streaming pour traiter des données IoT en temps réel avec Apache Spark Structured Streaming, Kafka et Delta Lake.

## Architecture Médaillon

- **Bronze Layer** : Données brutes ingérées depuis des fichiers JSON
  - Ingestion minimale, pas de filtrage, toutes les données conservées
- **Silver Layer** : Données nettoyées et normalisées depuis Kafka
  - Nettoyage approfondi, validation, filtrage, colonnes calculées
- **Gold Layer** : Données agrégées pour l'analyse
  - Agrégations temporelles (horaires, quotidiennes), statistiques, KPIs

## Prérequis

- Docker et Docker Compose installés
- WSL Ubuntu (pour les commandes Linux)
- Windows 10/11

## Structure du projet

```
MyStream/
├── data/                  # Données d'entrée (JSON)
├── delta/                 # Tables Delta Lake
│   ├── bronze/           # Niveau Bronze (données brutes)
│   ├── silver/           # Niveau Silver (données nettoyées)
│   └── gold/             # Niveau Gold (données agrégées)
├── checkpoints/          # Checkpoints Spark pour la tolérance aux pannes
│   ├── bronze/          # Checkpoints Bronze
│   ├── silver/          # Checkpoints Silver
│   └── gold/            # Checkpoints Gold
├── notebooks/            # Notebooks Jupyter
│   ├── 01_pipeline_bronze.ipynb   # Bronze : Données brutes
│   ├── 02_pipeline_silver.ipynb   # Silver : Données nettoyées
│   └── 03_pipeline_gold.ipynb     # Gold : Données agrégées
├── scripts/              # Scripts utilitaires
│   ├── generate_sensor_data.py      # Générateur de données JSON (Bronze)
│   ├── kafka_sensor_producer.py     # Producer Kafka (Silver)
│   ├── create_kafka_topic.sh        # Création du topic Kafka
│   ├── clean_delta.sh               # Nettoyage Delta Lake
│   └── ...
├── docker-compose.yml    # Configuration Docker
├── Dockerfile            # Image Spark personnalisée
├── requirements.txt      # Dépendances Python
└── roadmap.md           # Roadmap du projet
```

## Installation et démarrage

### 1. Démarrer les services Spark

**Première installation :**
```bash
# Avec WSL Ubuntu
chmod +x scripts/setup.sh
./scripts/setup.sh

# Ou avec PowerShell
.\scripts\rebuild.ps1
```

**Rebuild complet (après modifications) :**
```bash
# Avec WSL Ubuntu
chmod +x scripts/rebuild.sh
./scripts/rebuild.sh

# Ou avec PowerShell
.\scripts\rebuild.ps1
```

**Démarrage simple (si l'image existe déjà) :**
```bash
docker-compose up -d
```

### 2. Vérifier que les services sont démarrés

```bash
docker-compose ps
```

### 3. Accéder à l'interface Spark

- Spark Master UI : http://localhost:8080

### 4. Accéder à Jupyter Notebook

Jupyter Notebook démarre automatiquement avec le conteneur `spark-jupyter`.

**Accès direct** : http://localhost:8888 (aucun token requis)

**Si besoin de redémarrer manuellement** :
```bash
docker exec -it spark-jupyter jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --notebook-dir=/opt/spark/notebooks
```

Ou utiliser le script :
```bash
chmod +x scripts/start_jupyter.sh
./scripts/start_jupyter.sh
```

## Utilisation

### Pipeline Bronze (JSON → Delta Lake)

1. Placer les fichiers JSON dans le dossier `data/`
2. Ouvrir le notebook `notebooks/01_pipeline_bronze.ipynb`
3. Exécuter les cellules dans l'ordre

### Pipeline Silver (Kafka → Delta Lake) - Données Nettoyées

**Prérequis** : Kafka et Zookeeper sont déjà inclus dans `docker-compose.yml` et démarrent automatiquement.

1. **Créer le topic Kafka** (première fois uniquement) :
   ```bash
   chmod +x scripts/create_kafka_topic.sh
   ./scripts/create_kafka_topic.sh
   ```
   
   Ou manuellement :
   ```bash
   docker exec -it kafka kafka-topics \
       --create \
       --topic sensor-data-iot \
       --bootstrap-server localhost:9092 \
       --partitions 3 \
       --replication-factor 1
   ```

2. **Lancer le simulateur de capteurs** (dans un terminal séparé) :
   ```bash
   # Depuis votre machine
   python scripts/kafka_sensor_producer.py
   
   # Ou depuis le conteneur Docker
   docker exec -it spark-jupyter python3 /opt/spark/scripts/kafka_sensor_producer.py
   ```
   
   Le script produit des messages dans le topic `sensor-data-iot` avec :
   - 10 capteurs par défaut
   - 100 messages par capteur
   - Intervalle de 1 seconde entre les messages
   
   Vous pouvez personnaliser :
   ```bash
   python scripts/kafka_sensor_producer.py [num_sensors] [messages_per_sensor] [interval_seconds]
   ```

3. **Ouvrir le notebook** `notebooks/02_pipeline_silver.ipynb` dans Jupyter

4. **Exécuter les cellules dans l'ordre** :
   - Configuration Spark avec support Kafka
   - Vérification de la connectivité Kafka
   - Lecture du flux Kafka
   - Transformations et normalisation (nettoyage approfondi)
   - Filtrage et validation
   - Écriture dans Delta Lake Silver

### Pipeline Gold (Silver → Delta Lake) - Données Agrégées

**Prérequis** : Les données Silver doivent exister (exécuter d'abord le notebook Silver).

1. **Ouvrir le notebook** `notebooks/03_pipeline_gold.ipynb` dans Jupyter

2. **Exécuter les cellules dans l'ordre** :
   - Configuration Spark
   - Vérification des données Silver
   - Lecture du flux Silver (streaming)
   - Agrégations temporelles (fenêtres horaires)
   - Statistiques par capteur
   - Métriques métier (KPIs quotidiens)
   - Écriture dans Delta Lake Gold

**Types d'agrégations Gold** :
- **Agrégations horaires** : Moyennes, min, max par bâtiment et par heure
- **Statistiques capteurs** : Stats par capteur sur fenêtres glissantes
- **KPIs quotidiens** : Consommation totale, taux d'anomalies, capteurs actifs

## Configuration Kafka

Kafka est configuré dans `docker-compose.yml` avec :
- **Zookeeper** : Port 2181
- **Kafka Broker** : Port 9092
- **Topic par défaut** : `sensor-data-iot` (3 partitions, réplication factor 1)

**Variables d'environnement** (dans `.env`) :
- `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
- `KAFKA_TOPIC_IOT=sensor-data-iot`
- `KAFKA_CONSUMER_GROUP=spark-streaming-consumer`

**Vérifier que Kafka fonctionne** :
```bash
# Lister les topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Consulter les messages (test)
docker exec -it kafka kafka-console-consumer \
    --topic sensor-data-iot \
    --from-beginning \
    --bootstrap-server localhost:9092
```

## Documentation

Voir `roadmap.md` pour le détail des étapes du projet.

## Auteur

Projet SmartTech - Streaming de Données IoT

