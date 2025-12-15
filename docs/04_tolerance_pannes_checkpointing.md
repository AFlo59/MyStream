# Tolérance aux pannes et checkpointing

## Vue d'ensemble

La tolérance aux pannes est cruciale dans les systèmes de streaming pour garantir qu'aucune donnée ne soit perdue en cas d'échec. Structured Streaming utilise le **checkpointing** et les **Write-Ahead Logs (WAL)** pour assurer des garanties exactly-once.

## Mécanisme de checkpointing

### Définition

Le checkpointing consiste à sauvegarder régulièrement l'état de la requête de streaming, incluant :
- Les métadonnées de la source (offsets Kafka, fichiers traités, etc.)
- L'état des agrégations en cours
- Les informations de progression

### Configuration

```python
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .option("path", "/delta/bronze") \
    .start()
```

**Important** : Le `checkpointLocation` est **obligatoire** pour garantir la tolérance aux pannes.

### Structure des checkpoints

Le répertoire de checkpoint contient :
```
checkpoints/
├── sources/          # Métadonnées des sources
├── sinks/            # Métadonnées des sinks
├── state/            # État des agrégations
└── offsets/          # Offsets des sources (Kafka, fichiers, etc.)
```

## Garanties de traitement

### Exactly-once (avec checkpoint)

**Définition** : Chaque enregistrement est traité exactement une fois, même en cas de panne.

**Comment ça marche** :
1. Les offsets sont sauvegardés dans le checkpoint **avant** le traitement
2. En cas de redémarrage, Spark reprend depuis le dernier offset sauvegardé
3. Les écritures sont idempotentes (Delta Lake garantit cela)

**Cas d'usage SmartTech** : **Recommandé pour la production**

```python
# Configuration exactly-once
query = bronze_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .start()
```

### At-least-once (sans checkpoint)

**Définition** : Chaque enregistrement est traité au moins une fois, possiblement plusieurs fois.

**Risque** : Duplication des données en cas de panne.

**Cas d'usage** : Tests et développement uniquement.

## Write-Ahead Logs (WAL)

### Principe

Les WAL enregistrent toutes les opérations d'écriture **avant** qu'elles ne soient appliquées au sink final. Cela permet de :
- Garantir la durabilité des écritures
- Permettre la récupération après panne
- Assurer la cohérence

### Implémentation dans Spark

Spark utilise automatiquement les WAL pour :
- Les sources de fichiers
- Les sources Kafka (via les offsets)
- Les écritures vers Delta Lake

## Récupération après panne

### Scénario de panne

1. **Panne du driver Spark** : Le driver crash pendant le traitement
2. **Redémarrage** : Le driver redémarre
3. **Récupération** : Spark lit le checkpoint et reprend depuis le dernier état sauvegardé

### Exemple de récupération

```python
# Première exécution
query = bronze_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .start()

# ... panne du driver ...

# Redémarrage - Spark reprend automatiquement depuis le checkpoint
query_restart = bronze_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .start()  # Reprend depuis le dernier offset traité !
```

**Important** : Le schéma et la configuration doivent rester identiques.

## Bonnes pratiques

### 1. Chemin de checkpoint dédié

```python
# ✅ Bon : Chemin dédié par pipeline
CHECKPOINT_BRONZE = "/checkpoints/bronze"
CHECKPOINT_SILVER = "/checkpoints/silver"

# ❌ Mauvais : Chemin partagé
CHECKPOINT_SHARED = "/checkpoints"  # Peut causer des conflits
```

### 2. Sauvegarde des checkpoints

Les checkpoints sont critiques ! Recommandations :
- **Backup régulier** : Sauvegarder le répertoire de checkpoint
- **Stockage fiable** : Utiliser un système de fichiers distribué (HDFS, S3, etc.)
- **Versioning** : Conserver plusieurs versions en cas de corruption

### 3. Gestion des changements de schéma

**Problème** : Si le schéma change, le checkpoint peut devenir incompatible.

**Solution** :
- Utiliser `mergeSchema=True` pour Delta Lake
- Ou supprimer le checkpoint (⚠️ perte de l'état)

```python
# Permet l'évolution du schéma
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .option("mergeSchema", "true") \
    .start()
```

### 4. Monitoring des checkpoints

```python
# Vérifier le statut de la query
print(query.status)

# Voir la dernière progression
print(query.lastProgress)

# Vérifier si la query est active
print(query.isActive)
```

## Cas d'usage SmartTech

### Phase 2.1 - Pipeline Bronze

```python
# Checkpoint dédié pour la pipeline Bronze
CHECKPOINT_PATH = "/opt/spark/checkpoints/bronze"

query = bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", DELTA_BRONZE_PATH) \
    .start()
```

**Avantages** :
- Aucune perte de données même en cas de panne
- Reprise automatique après redémarrage
- Traçabilité complète des fichiers traités

### Phase 2.2 - Pipeline Silver (Kafka)

```python
# Checkpoint gère automatiquement les offsets Kafka
CHECKPOINT_PATH = "/opt/spark/checkpoints/silver"

query = kafka_stream.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", DELTA_SILVER_PATH) \
    .start()
```

**Avantages** :
- Gestion automatique des offsets Kafka
- Pas de duplication même si le consumer redémarre
- Cohérence garantie entre Kafka et Delta Lake

## Limitations et considérations

### Limitations

1. **Changements de schéma** : Peuvent nécessiter la suppression du checkpoint
2. **Taille du checkpoint** : Peut croître avec le temps (nettoyage périodique recommandé)
3. **Performance** : Le checkpointing ajoute une légère latence

### Considérations de performance

- **Fréquence de checkpoint** : Automatique, mais peut être optimisée
- **Stockage** : Utiliser un stockage rapide pour les checkpoints
- **Nettoyage** : Supprimer les anciens checkpoints périodiquement

## Sources

- [Apache Spark - Fault Tolerance](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics)
- [Structured Streaming Checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
- [Delta Lake - Streaming Writes and Checkpointing](https://docs.delta.io/delta-streaming.html#checkpoint-location)
- [Databricks - Checkpointing in Structured Streaming](https://docs.databricks.com/structured-streaming/checkpointing.html)

