# Triggers

## Vue d'ensemble

Les triggers contrôlent **quand** une requête de streaming traite les données disponibles. Ils déterminent la fréquence à laquelle Spark exécute les micro-batches.

## Types de triggers

### 1. ProcessingTime Trigger (par défaut)

**Définition** : Traite les données à intervalles réguliers, basé sur l'horloge système.

```python
query = df.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("delta") \
    .start()
```

**Caractéristiques** :
- ✅ Simple à configurer
- ✅ Latence prévisible
- ✅ Bon pour la plupart des cas d'usage
- ⚠️ Latence minimale : ~100ms (mode micro-batch)

**Intervalles supportés** :
- `"5 seconds"`, `"10 seconds"`, `"1 minute"`
- `"5 minutes"`, `"1 hour"`
- Format : `"interval timeUnit"`

**Cas d'usage SmartTech** :
- **Phase 2.1 (Bronze)** : Traitement toutes les 10-30 secondes
- **Phase 2.2 (Silver)** : Traitement toutes les 5-10 secondes pour latence réduite

**Exemple** :
```python
# Traite les données toutes les 10 secondes
bronze_stream.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .start()
```

### 2. Once Trigger

**Définition** : Traite toutes les données disponibles une seule fois, puis arrête la query.

```python
query = df.writeStream \
    .trigger(once=True) \
    .format("delta") \
    .start()
```

**Caractéristiques** :
- ✅ Traite toutes les données en attente
- ✅ S'arrête automatiquement après traitement
- ✅ Utile pour les migrations batch
- ⚠️ La query s'arrête après exécution

**Cas d'usage SmartTech** :
- Traitement initial de données historiques
- Migration de données batch vers streaming
- Traitement ponctuel de backlog

**Exemple** :
```python
# Traite toutes les données en attente une fois
backlog_stream.writeStream \
    .trigger(once=True) \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/backlog") \
    .start()

# La query s'arrête automatiquement après traitement
```

### 3. Continuous Trigger (depuis Spark 2.3)

**Définition** : Traitement continu avec latence très faible (~1ms).

```python
query = df.writeStream \
    .trigger(continuous="1 second") \
    .format("delta") \
    .start()
```

**Caractéristiques** :
- ✅ Latence très faible : ~1ms
- ✅ Traitement continu (pas de micro-batches)
- ⚠️ Garantie : at-least-once (pas exactly-once)
- ⚠️ Limitations : Sources et sinks compatibles uniquement

**Sources compatibles** :
- Kafka
- Rate source (pour tests)

**Sinks compatibles** :
- Kafka
- Console
- Memory

**Cas d'usage SmartTech** :
- Détection d'anomalies ultra-rapide (< 1 seconde)
- Alertes temps réel critiques
- Dashboards avec latence minimale

**Exemple** :
```python
# Traitement continu avec latence de 1 seconde
# ⚠️ Nécessite des sources/sinks compatibles
kafka_stream.writeStream \
    .trigger(continuous="1 second") \
    .format("kafka") \
    .option("topic", "alerts") \
    .start()
```

## Comparaison des triggers

| Critère | ProcessingTime | Once | Continuous |
|---------|---------------|------|------------|
| **Latence** | ~100ms | Variable | ~1ms |
| **Garantie** | Exactly-once | Exactly-once | At-least-once |
| **Comportement** | Continu | Une fois puis arrêt | Continu |
| **Cas d'usage** | Production générale | Migration/Maintenance | Ultra-low latency |
| **Compatibilité** | Tous sources/sinks | Tous sources/sinks | Limité |

## Choix du trigger selon les besoins

### Latence acceptable : 10-60 secondes
```python
.trigger(processingTime="30 seconds")
```
**Exemple** : Ingestion Bronze, historisation

### Latence faible : 1-10 secondes
```python
.trigger(processingTime="5 seconds")
```
**Exemple** : Pipeline Silver, dashboards temps réel

### Latence critique : < 1 seconde
```python
.trigger(continuous="1 second")
```
**Exemple** : Détection d'anomalies critiques, alertes

### Traitement ponctuel
```python
.trigger(once=True)
```
**Exemple** : Migration de données, traitement de backlog

## Configuration avancée

### Trigger par défaut

Si aucun trigger n'est spécifié, Spark utilise `processingTime` avec un intervalle par défaut :

```python
# Équivalent à trigger(processingTime="0 seconds")
df.writeStream.format("delta").start()
```

**Comportement** : Traite les données dès qu'elles sont disponibles, avec latence minimale.

### Combinaison avec d'autres options

```python
query = df.writeStream \
    .trigger(processingTime="10 seconds") \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints") \
    .option("maxFilesPerTrigger", 10) \
    .start()
```

**Note** : `maxFilesPerTrigger` contrôle le nombre de fichiers par micro-batch, indépendamment du trigger.

## Monitoring des triggers

### Vérifier la latence

```python
# Voir les métriques de la dernière progression
progress = query.lastProgress

print(f"Latence du batch : {progress['batchDuration']} ms")
print(f"Temps de traitement : {progress['durationMs']} ms")
print(f"Nombre d'inputRows : {progress['inputRowsPerSecond']}")
```

### Ajuster le trigger selon les performances

Si la latence est trop élevée :
- Réduire l'intervalle du trigger
- Optimiser les transformations
- Augmenter les ressources Spark

Si le système est surchargé :
- Augmenter l'intervalle du trigger
- Réduire `maxFilesPerTrigger`
- Optimiser les écritures

## Recommandations pour SmartTech

### Phase 2.1 - Pipeline Bronze

```python
# Trigger toutes les 10-30 secondes
# Priorité : Fiabilité > Latence
.trigger(processingTime="10 seconds")
```

**Raison** : L'ingestion Bronze n'a pas besoin de latence ultra-faible, la fiabilité est prioritaire.

### Phase 2.2 - Pipeline Silver

```python
# Trigger toutes les 5-10 secondes
# Priorité : Latence réduite pour dashboards
.trigger(processingTime="5 seconds")
```

**Raison** : Les données Silver alimentent les dashboards, nécessitent une latence plus faible.

### Détection d'anomalies (futur)

```python
# Trigger continu pour latence minimale
# ⚠️ Nécessite sources/sinks compatibles
.trigger(continuous="1 second")
```

**Raison** : Les anomalies doivent être détectées le plus rapidement possible.

## Sources

- [Apache Spark - Triggers](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
- [Structured Streaming Triggers](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
- [Databricks - Triggers in Structured Streaming](https://docs.databricks.com/structured-streaming/triggers.html)
- [Continuous Processing Mode](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing)

