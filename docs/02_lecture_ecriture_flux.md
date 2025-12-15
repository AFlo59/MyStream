# Lecture et écritie de flux

## Sources supportées

Structured Streaming supporte plusieurs types de sources pour lire des données en streaming.

### Sources de fichiers

#### Format JSON
```python
df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .load("/path/to/data")
```

**Caractéristiques** :
- Surveille un répertoire pour de nouveaux fichiers
- Traite les fichiers par ordre de modification
- Option `maxFilesPerTrigger` : contrôle le nombre de fichiers par micro-batch

**Cas d'usage SmartTech** : Idéal pour la Phase 2.1 où les données JSON arrivent dans un dossier

#### Autres formats de fichiers
- **Parquet** : Format colonnaire optimisé
- **CSV** : Format texte simple
- **ORC** : Format colonnaire optimisé
- **Text** : Fichiers texte bruts

### Sources réseau

#### Socket
```python
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

**Cas d'usage** : Tests et développement rapide

### Sources de messagerie

#### Kafka (pour Phase 2.2)
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("subscribe", "topic1") \
    .load()
```

**Avantages** :
- Débit élevé
- Tolérance aux pannes
- Scalabilité horizontale
- Gestion des offsets automatique

**Cas d'usage SmartTech** : Phase 2.2 pour consommer les données IoT depuis Kafka

### Schéma des données

#### Schéma défini explicitement
```python
from pyspark.sql.types import *

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = spark.readStream.schema(schema).json("/path")
```

**Avantages** :
- Validation des données dès la lecture
- Performance améliorée (pas d'inférence de schéma)
- Contrôle strict des types

#### Inférence de schéma (non recommandé en production)
```python
df = spark.readStream.json("/path")  # Inférence automatique
```

## Sinks supportés

Les sinks sont les destinations où les données traitées sont écrites.

### Delta Lake (recommandé pour SmartTech)

```python
df.writeStream \
    .format("delta") \
    .option("path", "/delta/bronze") \
    .option("checkpointLocation", "/checkpoints") \
    .start()
```

**Avantages** :
- Transactions ACID
- Time travel (accès aux versions historiques)
- Optimisations automatiques (Z-ordering, compaction)
- Support des mises à jour et suppressions

**Architecture Médaillon** :
- **Bronze** : Données brutes ingérées
- **Silver** : Données nettoyées et validées
- **Gold** : Données agrégées pour l'analyse

### Fichiers

```python
df.writeStream \
    .format("parquet") \
    .option("path", "/output") \
    .option("checkpointLocation", "/checkpoints") \
    .start()
```

**Formats supportés** : Parquet, JSON, CSV, ORC, Text

### Console (pour le débogage)

```python
df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()
```

**Cas d'usage** : Tests et développement

### Memory (pour les tests)

```python
df.writeStream \
    .format("memory") \
    .queryName("sensor_data") \
    .outputMode("append") \
    .start()
```

## Options de configuration

### Options communes pour la lecture

| Option | Description | Exemple |
|--------|-------------|---------|
| `maxFilesPerTrigger` | Nombre max de fichiers par micro-batch | `1`, `10`, `unlimited` |
| `latestFirst` | Traiter les fichiers les plus récents en premier | `true`, `false` |
| `maxFileAge` | Âge maximum des fichiers à traiter | `1d`, `2h` |

### Options communes pour l'écriture

| Option | Description | Exemple |
|--------|-------------|---------|
| `checkpointLocation` | Chemin pour les checkpoints (obligatoire) | `/checkpoints/bronze` |
| `path` | Chemin de destination | `/delta/bronze` |
| `partitionBy` | Colonnes pour le partitionnement | `["building_id", "sensor_type"]` |

## Schéma des données Kafka

Lors de la lecture depuis Kafka, chaque ligne contient :

| Colonne | Type | Description |
|---------|------|-------------|
| `key` | binary | Clé du message Kafka |
| `value` | binary | Valeur du message (souvent JSON) |
| `topic` | string | Nom du topic Kafka |
| `partition` | int | Partition du message |
| `offset` | long | Offset du message |
| `timestamp` | timestamp | Timestamp du message |
| `timestampType` | int | Type de timestamp |
| `headers` | array | Headers du message (optionnel) |

**Important** : Il faut parser la colonne `value` pour extraire les données JSON.

## Cas d'usage SmartTech

### Phase 2.1 : JSON → Delta Lake Bronze
- **Source** : Fichiers JSON dans un répertoire
- **Sink** : Delta Lake Bronze
- **Avantage** : Simple à mettre en place, idéal pour démarrer

### Phase 2.2 : Kafka → Delta Lake Silver
- **Source** : Kafka topic `sensor-data-iot`
- **Sink** : Delta Lake Silver
- **Avantage** : Débit élevé, tolérance aux pannes, scalabilité

## Sources

- [Apache Spark - Data Sources](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#data-sources)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/streaming-structured-streaming-kafka-integration.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Databricks - Streaming Data Sources](https://docs.databricks.com/structured-streaming/data-sources.html)

