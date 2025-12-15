# Concepts fondamentaux du streaming structuré

## Vue d'ensemble

Le streaming structuré (Structured Streaming) est un moteur de traitement de flux scalable et tolérant aux pannes, construit sur le moteur Spark SQL. Il permet de traiter des flux de données en continu de manière similaire au traitement par batch.

## Principe du streaming structuré

### Définition

Structured Streaming permet d'exprimer des calculs sur des flux de données de la même manière que sur des données statiques. Le moteur Spark SQL s'occupe d'exécuter ces calculs de manière incrémentale et continue, mettant à jour les résultats finaux au fur et à mesure que les données arrivent.

### Caractéristiques principales

- **Modèle de programmation unifié** : Utilise l'API Dataset/DataFrame de Spark
- **Traitement incrémental** : Les calculs sont effectués de manière incrémentale
- **Garanties de tolérance aux pannes** : Exactly-once grâce au checkpointing et aux Write-Ahead Logs
- **Latence faible** : Peut atteindre 100ms en mode micro-batch, 1ms en mode continu

### Différence avec le batch processing

| Aspect | Batch Processing | Streaming Processing |
|--------|-----------------|---------------------|
| **Données** | Statiques, complètes | Dynamiques, continues |
| **Traitement** | Une fois, sur tout le dataset | Continu, sur des micro-batches |
| **Résultats** | Finaux après traitement complet | Mis à jour progressivement |
| **Latence** | Minutes/heures | Millisecondes/secondes |

### Modèle de programmation unifié

Avec Structured Streaming, vous utilisez la même API Dataset/DataFrame que pour le batch :

```python
# Batch
df = spark.read.json("data.json")
result = df.groupBy("sensor_id").avg("temperature")

# Streaming (même code !)
df = spark.readStream.json("data/")
result = df.groupBy("sensor_id").avg("temperature")
```

## Architecture interne

### Mode micro-batch (par défaut)

- Traite les flux comme une série de petits jobs batch
- Latence : ~100ms
- Garantie : exactly-once
- Idéal pour la plupart des cas d'usage

### Mode continu (depuis Spark 2.3)

- Traitement continu avec latence très faible
- Latence : ~1ms
- Garantie : at-least-once
- Nécessite des sources et sinks compatibles

## Cas d'usage IoT SmartTech

Dans le contexte SmartTech, le streaming structuré permet de :

1. **Traiter les données en temps réel** : Les mesures des capteurs arrivent continuellement et doivent être traitées immédiatement
2. **Détecter les anomalies rapidement** : Identifier les comportements anormaux dès leur apparition
3. **Alimenter les dashboards** : Mettre à jour les visualisations en temps réel
4. **Historiser efficacement** : Stocker les données dans Delta Lake de manière incrémentale

## Avantages pour SmartTech

- **Simplicité** : Même API que le batch, courbe d'apprentissage réduite
- **Fiabilité** : Tolérance aux pannes intégrée, pas de perte de données
- **Performance** : Traitement distribué et optimisé par Spark SQL
- **Flexibilité** : Support de nombreuses sources et sinks

## Sources

- [Apache Spark - Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Structured Streaming Overview](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview)
- [Databricks - Introduction to Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
- [Spark: The Definitive Guide - Chapter 20: Structured Streaming](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)

