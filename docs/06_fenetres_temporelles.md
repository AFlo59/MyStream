# Fenêtres temporelles (Windowing)

## Vue d'ensemble

Les fenêtres temporelles permettent de regrouper et d'agréger des événements sur des périodes de temps définies. C'est essentiel pour analyser des données de streaming basées sur le temps.

## Concept de windowing

### Définition

Le windowing consiste à diviser un flux de données en fenêtres de temps et à appliquer des agrégations sur chaque fenêtre.

**Exemple SmartTech** : Calculer la température moyenne par capteur toutes les 5 minutes.

### Types de fenêtres

#### 1. Fenêtres glissantes (Sliding Windows)

**Définition** : Fenêtres qui se chevauchent, avançant par incréments réguliers.

```python
from pyspark.sql.functions import window, avg

# Fenêtre de 10 minutes, avançant toutes les 5 minutes
df.groupBy(
    window("timestamp", "10 minutes", "5 minutes"),
    "sensor_id"
).avg("temperature")
```

**Caractéristiques** :
- Chevauchement entre fenêtres
- Plus de granularité dans les résultats
- Plus coûteux en calcul

**Exemple** :
```
Fenêtre 1: [10:00 - 10:10]
Fenêtre 2: [10:05 - 10:15]  ← Chevauchement de 5 minutes
Fenêtre 3: [10:10 - 10:20]
```

#### 2. Fenêtres fixes (Tumbling Windows)

**Définition** : Fenêtres qui ne se chevauchent pas, chaque événement appartient à une seule fenêtre.

```python
# Fenêtre de 10 minutes, sans chevauchement
df.groupBy(
    window("timestamp", "10 minutes"),
    "sensor_id"
).avg("temperature")
```

**Caractéristiques** :
- Pas de chevauchement
- Plus efficace en calcul
- Moins de granularité

**Exemple** :
```
Fenêtre 1: [10:00 - 10:10]
Fenêtre 2: [10:10 - 10:20]  ← Pas de chevauchement
Fenêtre 3: [10:20 - 10:30]
```

## Watermarks

### Définition

Les watermarks définissent le délai maximum d'attente pour les données tardives (late data). Au-delà de ce délai, les données sont considérées comme trop tardives et ignorées.

### Pourquoi les watermarks ?

Dans le streaming, les événements peuvent arriver **hors ordre** :
- Problèmes réseau
- Différences d'horloge entre systèmes
- Retard de traitement

Les watermarks permettent de :
- Gérer les données tardives de manière contrôlée
- Libérer la mémoire (état des fenêtres anciennes)
- Utiliser Append mode avec des agrégations

### Configuration

```python
from pyspark.sql.functions import window, avg

# Watermark de 10 minutes : données arrivant plus de 10 min en retard sont ignorées
df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "sensor_id"
    ) \
    .avg("temperature") \
    .writeStream \
    .outputMode("append") \  # Possible grâce au watermark !
    .format("delta") \
    .start()
```

### Comment ça marche

1. **Watermark = timestamp_max - délai**
2. Les fenêtres avec `window.end < watermark` sont considérées comme complètes
3. Les données avec `timestamp < watermark` sont ignorées
4. L'état des fenêtres complètes peut être libéré

**Exemple** :
```
Timestamp max observé : 10:30
Watermark (délai 10 min) : 10:20
→ Fenêtres se terminant avant 10:20 sont complètes
→ Données avec timestamp < 10:20 sont ignorées
```

## Agrégations sur fenêtres

### Agrégations simples

```python
from pyspark.sql.functions import window, avg, max, min, count

# Température moyenne par fenêtre de 5 minutes
df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "sensor_id"
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        max("temperature").alias("max_temp"),
        min("temperature").alias("min_temp"),
        count("*").alias("measurement_count")
    )
```

### Agrégations complexes

```python
from pyspark.sql.functions import window, avg, stddev, collect_list

# Statistiques détaillées par fenêtre
df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "10 minutes"),
        "building_id"
    ) \
    .agg(
        avg("energy_consumption").alias("avg_energy"),
        stddev("energy_consumption").alias("energy_stddev"),
        collect_list("sensor_id").alias("sensors")
    )
```

## Cas d'usage SmartTech

### 1. Température moyenne par capteur (fenêtre de 5 minutes)

```python
avg_temp = stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "sensor_id"
    ) \
    .avg("temperature") \
    .writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("path", "/delta/silver/avg_temperature") \
    .start()
```

**Utilité** : Réduire le volume de données, détecter les tendances

### 2. Consommation énergétique horaire par bâtiment

```python
hourly_energy = stream \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window("timestamp", "1 hour"),
        "building_id"
    ) \
    .sum("energy_consumption") \
    .writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("path", "/delta/gold/hourly_energy") \
    .start()
```

**Utilité** : Facturation, optimisation énergétique

### 3. Détection d'anomalies sur fenêtre glissante

```python
# Fenêtre glissante : 10 minutes, avançant toutes les 1 minute
anomaly_detection = stream \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window("timestamp", "10 minutes", "1 minute"),
        "sensor_id"
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        stddev("temperature").alias("temp_stddev")
    ) \
    .filter(col("temp_stddev") > 5.0) \  # Anomalie si écart-type élevé
    .writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("path", "/delta/silver/anomalies") \
    .start()
```

**Utilité** : Détection en temps réel de comportements anormaux

## Bonnes pratiques

### 1. Choisir le bon délai de watermark

**Trop court** : Risque d'ignorer des données valides
```python
.withWatermark("timestamp", "1 minute")  # ⚠️ Trop court pour IoT
```

**Trop long** : Consommation mémoire excessive
```python
.withWatermark("timestamp", "24 hours")  # ⚠️ Trop long, mémoire gaspillée
```

**Optimal** : Basé sur la latence attendue des données
```python
.withWatermark("timestamp", "10 minutes")  # ✅ Bon compromis pour IoT
```

### 2. Taille de fenêtre vs slide

**Fenêtre = slide** : Fenêtre fixe (tumbling)
```python
window("timestamp", "10 minutes")  # Slide = 10 minutes par défaut
```

**Fenêtre > slide** : Fenêtre glissante
```python
window("timestamp", "10 minutes", "5 minutes")  # Slide = 5 minutes
```

**Recommandation** : Utiliser des fenêtres fixes sauf si besoin spécifique de granularité.

### 3. Colonne timestamp

**Important** : La colonne timestamp doit être de type `TimestampType`

```python
# ✅ Bon : Conversion explicite
df.withColumn("timestamp", to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss"))

# ❌ Mauvais : Timestamp en string
df.groupBy(window("timestamp_str", "10 minutes"))  # Ne fonctionnera pas !
```

## Limitations

### Limitations des watermarks

1. **Append mode uniquement** : Les watermarks permettent d'utiliser Append mode avec agrégations
2. **Données tardives** : Les données arrivant après le watermark sont ignorées
3. **État mémoire** : L'état des fenêtres incomplètes est conservé en mémoire

### Limitations des fenêtres

1. **Colonne timestamp requise** : Nécessite une colonne timestamp valide
2. **Performance** : Les fenêtres glissantes sont plus coûteuses
3. **Granularité** : Limité par la précision des timestamps

## Sources

- [Apache Spark - Window Operations](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time)
- [Handling Late Data and Watermarking](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
- [Databricks - Windowing in Structured Streaming](https://docs.databricks.com/structured-streaming/windowing.html)
- [Event Time and Watermarks](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#types-of-time)

