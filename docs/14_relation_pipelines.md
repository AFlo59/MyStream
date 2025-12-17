# Relation entre les Pipelines Bronze, Silver et Gold

## Question : Les pipelines sont-elles liées ?

**Réponse courte** : **Non, elles sont indépendantes** dans l'implémentation actuelle, mais elles suivent l'architecture Médaillon.

## Architecture Actuelle

### Pipeline 1 : Bronze (JSON → Delta Bronze)

**Source** : Fichiers JSON dans `data/`
**Destination** : Delta Lake Bronze (`/opt/spark/delta/bronze`)
**Rôle** : Ingestion minimale de données brutes

```
JSON Files (data/)
    ↓
Pipeline Bronze (01_pipeline_bronze.ipynb)
    ↓
Delta Lake Bronze (données brutes)
```

### Pipeline 2 : Silver (Kafka → Delta Silver)

**Source** : Topic Kafka (`sensor-data-iot`)
**Destination** : Delta Lake Silver (`/opt/spark/delta/silver`)
**Rôle** : Nettoyage approfondi et validation

```
Kafka Topic (sensor-data-iot)
    ↓ (alimenté par kafka_sensor_producer.py)
Pipeline Silver (02_pipeline_silver.ipynb)
    ↓
Delta Lake Silver (données nettoyées)
```

### Pipeline 3 : Gold (Silver → Delta Gold)

**Source** : Delta Lake Silver (streaming)
**Destination** : Delta Lake Gold (`/opt/spark/delta/gold`)
**Rôle** : Agrégations et analytics

```
Delta Lake Silver
    ↓
Pipeline Gold (03_pipeline_gold.ipynb)
    ↓
Delta Lake Gold (données agrégées)
```

## Pourquoi sont-elles indépendantes ?

### 1. Sources de données différentes

- **Bronze** : Lit depuis des fichiers JSON (Phase 2.1 de la roadmap)
- **Silver** : Lit depuis Kafka (Phase 2.2 de la roadmap)
- **Gold** : Lit depuis Delta Silver (streaming)

### 2. Cas d'usage différents

- **Bronze** : Démonstration de l'ingestion depuis fichiers
- **Silver** : Démonstration de l'intégration Kafka + nettoyage
- **Gold** : Démonstration des agrégations temporelles

### 3. Architecture pédagogique

Cette séparation permet de :
- Comprendre chaque étape indépendamment
- Tester chaque pipeline séparément
- Démontrer différents concepts (fichiers, Kafka, streaming)

## Relation Logique (Architecture Médaillon)

Bien qu'elles soient indépendantes dans l'implémentation, elles suivent la logique Médaillon :

```
BRONZE (brut)
    ↓ (conceptuellement)
SILVER (nettoyé)
    ↓ (conceptuellement)
GOLD (agrégé)
```

### Dans un environnement de production réel

En production, on aurait typiquement :

```
Sources (IoT Devices)
    ↓
Kafka (message broker)
    ↓
BRONZE (ingestion depuis Kafka)
    ↓
SILVER (nettoyage depuis Bronze)
    ↓
GOLD (agrégations depuis Silver)
```

## Flux de Données Actuel

### Flux 1 : Bronze (Fichiers → Delta Bronze)

```
1. Génération de fichiers JSON (generate_sensor_data.py)
2. Pipeline Bronze lit les fichiers
3. Ingestion minimale (normalisation schéma, métadonnées)
4. Écriture dans Delta Bronze
```

### Flux 2 : Silver (Kafka → Delta Silver)

```
1. Producteur Kafka lit les patterns depuis data/ (optionnel)
2. Producteur génère des messages similaires
3. Messages produits dans Kafka topic
4. Pipeline Silver consomme depuis Kafka
5. Nettoyage approfondi et validation
6. Écriture dans Delta Silver
```

### Flux 3 : Gold (Silver → Delta Gold)

```
1. Pipeline Gold lit depuis Delta Silver (streaming)
2. Agrégations temporelles (fenêtres horaires)
3. Statistiques par dimension (bâtiment, capteur)
4. KPIs quotidiens
5. Écriture dans Delta Gold
```

## Points Importants

### 1. Pas de dépendance directe

- La pipeline Silver **ne lit pas** depuis Bronze
- La pipeline Gold **lit depuis** Silver (c'est la seule dépendance)
- Bronze et Silver sont **complètement indépendantes**

### 2. Producteur Kafka

Le producteur Kafka (`kafka_sensor_producer.py`) :
- Peut lire les patterns depuis les fichiers JSON de Bronze
- Génère des données similaires mais indépendantes
- Alimente directement Kafka (pas via Bronze)

### 3. Réutilisation des données

Les fichiers JSON dans `data/` sont utilisés par :
- **Bronze** : Pour l'ingestion initiale
- **Producteur Kafka** : Pour extraire les patterns et générer des données similaires

## Avantages de cette Architecture

### 1. Flexibilité

- Chaque pipeline peut être testée indépendamment
- Pas besoin d'exécuter Bronze pour tester Silver
- Possibilité de remplacer les sources facilement

### 2. Pédagogie

- Comprendre chaque concept séparément
- Voir différents types de sources (fichiers, Kafka)
- Apprendre les différentes transformations

### 3. Démonstration

- Montrer l'ingestion depuis fichiers (Bronze)
- Montrer l'intégration Kafka (Silver)
- Montrer les agrégations (Gold)

## Évolution Possible

Si vous voulez créer une dépendance réelle :

### Option 1 : Silver lit depuis Bronze

```python
# Dans 02_pipeline_silver.ipynb
silver_stream = spark.readStream \
    .format("delta") \
    .option("path", DELTA_BRONZE_PATH) \
    .load()
```

### Option 2 : Producteur lit depuis Bronze

```python
# Dans kafka_sensor_producer.py
bronze_df = spark.read.format("delta").load(DELTA_BRONZE_PATH)
# Extraire les patterns depuis Bronze
```

## Résumé

| Pipeline | Source | Destination | Dépend de |
|----------|--------|-------------|-----------|
| **Bronze** | Fichiers JSON | Delta Bronze | Aucune |
| **Silver** | Kafka Topic | Delta Silver | Aucune (mais utilise les patterns des JSON) |
| **Gold** | Delta Silver | Delta Gold | **Silver** |

**Conclusion** : Les pipelines Bronze et Silver sont **indépendantes** mais suivent la même logique Médaillon. Seule Gold dépend directement de Silver.
