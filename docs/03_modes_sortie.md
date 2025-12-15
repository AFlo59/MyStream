# Modes de sortie (Output Modes)

## Vue d'ensemble

Les modes de sortie définissent comment les résultats d'une requête de streaming sont écrits dans le sink. Chaque mode a ses propres garanties et cas d'usage.

## Les trois modes de sortie

### 1. Append Mode

**Définition** : Ajoute uniquement les nouvelles lignes au résultat final.

```python
df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .start()
```

**Caractéristiques** :
- ✅ Ajoute uniquement les nouvelles lignes
- ✅ Ne modifie jamais les lignes existantes
- ✅ Compatible avec tous les sinks
- ⚠️ Limitation : Ne supporte pas les agrégations avec watermarks

**Cas d'usage SmartTech** :
- **Phase 2.1 (Bronze)** : Ingestion de données brutes
- Écriture de logs d'événements
- Historisation de données temporelles

**Exemple** :
```python
# Chaque micro-batch ajoute de nouvelles lignes
# Résultat final : toutes les lignes accumulées
bronze_stream.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("path", "/delta/bronze") \
    .start()
```

### 2. Update Mode

**Définition** : Met à jour les lignes modifiées depuis le dernier micro-batch.

```python
df.writeStream \
    .outputMode("update") \
    .format("delta") \
    .start()
```

**Caractéristiques** :
- ✅ Met à jour uniquement les lignes modifiées
- ✅ Plus efficace que Complete pour les mises à jour partielles
- ✅ Supporte les agrégations
- ⚠️ Nécessite un sink qui supporte les mises à jour (Delta Lake, certaines bases de données)

**Cas d'usage SmartTech** :
- Mise à jour d'états de capteurs
- Calculs d'agrégations incrémentales
- Dashboards temps réel avec mises à jour partielles

**Exemple** :
```python
# Calcule la température moyenne par capteur
# Met à jour uniquement les capteurs avec de nouvelles données
avg_temp = stream.groupBy("sensor_id").avg("temperature")

avg_temp.writeStream \
    .outputMode("update") \
    .format("delta") \
    .option("path", "/delta/silver/avg_temperature") \
    .start()
```

### 3. Complete Mode

**Définition** : Réécrit complètement la table de sortie à chaque micro-batch.

```python
df.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .start()
```

**Caractéristiques** :
- ✅ Contient toujours l'état complet et à jour
- ✅ Idéal pour les agrégations globales
- ⚠️ Peut être coûteux pour de grandes tables
- ⚠️ Nécessite de stocker tout l'état en mémoire

**Cas d'usage SmartTech** :
- Statistiques globales (nombre total de capteurs actifs)
- Top N des anomalies détectées
- Agrégations complètes nécessitant l'état global

**Exemple** :
```python
# Compte total des anomalies par bâtiment
# Réécrit la table complète à chaque micro-batch
anomaly_count = stream \
    .filter(col("anomaly_detected") == True) \
    .groupBy("building_id") \
    .count()

anomaly_count.writeStream \
    .outputMode("complete") \
    .format("delta") \
    .option("path", "/delta/gold/anomaly_stats") \
    .start()
```

## Comparaison des modes

| Critère | Append | Update | Complete |
|---------|--------|--------|----------|
| **Nouvelles lignes** | ✅ Ajoute | ✅ Ajoute | ✅ Ajoute |
| **Mises à jour** | ❌ Non | ✅ Oui | ✅ Oui (réécriture) |
| **Agrégations** | ⚠️ Limité | ✅ Oui | ✅ Oui |
| **Performance** | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| **Taille mémoire** | Faible | Moyenne | Élevée |
| **Cas d'usage** | Ingestion | Mises à jour | Agrégations globales |

## Compatibilité avec les sinks

### Delta Lake
- ✅ **Append** : Parfait pour l'ingestion
- ✅ **Update** : Supporte les mises à jour (MERGE)
- ✅ **Complete** : Supporte la réécriture complète

### Fichiers (Parquet, JSON, etc.)
- ✅ **Append** : Ajoute de nouveaux fichiers
- ❌ **Update** : Non supporté (fichiers immutables)
- ✅ **Complete** : Réécrit les fichiers

### Console / Memory
- ✅ Tous les modes supportés (pour tests)

## Watermarks et modes de sortie

Les **watermarks** permettent de gérer les données tardives et sont essentiels pour certaines agrégations :

```python
# Avec watermark, Append mode peut être utilisé avec des agrégations
stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "sensor_id") \
    .avg("temperature") \
    .writeStream \
    .outputMode("append") \  # Possible grâce au watermark !
    .format("delta") \
    .start()
```

**Règle** : Avec watermark, Append mode peut être utilisé pour des agrégations sur fenêtres temporelles.

## Recommandations pour SmartTech

### Phase 2.1 - Bronze Layer
```python
# Append mode : ingestion de données brutes
outputMode("append")
```
**Raison** : On veut simplement accumuler toutes les données brutes sans modification.

### Phase 2.2 - Silver Layer
```python
# Update mode : mise à jour des données nettoyées
outputMode("update")
```
**Raison** : Les données peuvent être corrigées/normalisées, nécessitant des mises à jour.

### Gold Layer (futur)
```python
# Complete mode : agrégations globales
outputMode("complete")
```
**Raison** : Pour les statistiques globales nécessitant l'état complet.

## Sources

- [Apache Spark - Output Modes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes)
- [Databricks - Output Modes in Structured Streaming](https://docs.databricks.com/structured-streaming/output-modes.html)
- [Delta Lake - Streaming Writes](https://docs.delta.io/delta-streaming.html)

