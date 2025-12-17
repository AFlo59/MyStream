# Architecture Médaillon (Medallion Architecture)

## Vue d'ensemble

L'architecture Médaillon est un pattern de design pour organiser les données dans un data lake. Elle divise les données en trois niveaux de qualité : **Bronze**, **Silver**, et **Gold**.

## Les trois niveaux

### Bronze Layer : Données brutes

**Définition** : Données ingérées telles quelles, sans transformation.

**Caractéristiques** :
- ✅ Données brutes, non modifiées
- ✅ Historique complet préservé
- ✅ Format d'origine conservé
- ✅ Aucune perte d'information

**Cas d'usage SmartTech** :
- Ingestion initiale des données IoT
- Sauvegarde complète pour audit
- Reprocessing si nécessaire

**Exemple Phase 2.1** :
```python
# Pipeline Bronze : JSON → Delta Lake
bronze_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("path", "/delta/bronze") \
    .start()
```

**Schéma** : Identique aux données sources (sensor_id, timestamp, temperature, etc.)

### Silver Layer : Données nettoyées

**Définition** : Données brutes nettoyées, validées et normalisées.

**Caractéristiques** :
- ✅ Données nettoyées et validées
- ✅ Schéma normalisé
- ✅ Qualité garantie
- ✅ Prêtes pour l'analyse

**Transformations appliquées** :
- Nettoyage des valeurs nulles
- Validation des plages de valeurs
- Normalisation des formats
- Déduplication si nécessaire
- Enrichissement avec métadonnées

**Cas d'usage SmartTech** :
- Données prêtes pour les dashboards
- Base pour les analyses avancées
- Source pour le niveau Gold

**Exemple Phase 2.2** :
```python
# Pipeline Silver : Kafka → Delta Lake (données nettoyées)
silver_stream = kafka_stream \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("temperature").between(-50, 60)) \
    .withColumn("ingestion_timestamp", current_timestamp())

silver_stream.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("path", "/delta/silver") \
    .start()
```

**Schéma** : Normalisé, avec colonnes supplémentaires (ingestion_timestamp, validation_status, etc.)

### Gold Layer : Données agrégées

**Définition** : Données agrégées et optimisées pour des cas d'usage spécifiques.

**Caractéristiques** :
- ✅ Données agrégées par métrique
- ✅ Optimisées pour les requêtes
- ✅ Schémas dénormalisés
- ✅ Prêtes pour la consommation finale

**Types d'agrégations** :
- Agrégations temporelles (moyennes horaires, quotidiennes)
- Agrégations par dimension (par bâtiment, par capteur)
- Métriques calculées (tendances, anomalies)
- Vues business (KPIs, rapports)

**Cas d'usage SmartTech** :
- Tableaux de bord exécutifs
- Rapports de consommation énergétique
- Statistiques par bâtiment
- Métriques de performance

**Exemple futur** :
```python
# Pipeline Gold : Agrégations horaires
gold_stream = silver_df \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(
        window("timestamp", "1 hour"),
        "building_id"
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        sum("energy_consumption").alias("total_energy"),
        count("*").alias("measurement_count")
    )

gold_stream.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("path", "/delta/gold/hourly_stats") \
    .start()
```

**Schéma** : Optimisé pour les requêtes analytiques (building_id, hour, avg_temp, total_energy, etc.)

## Flux de données

```
┌─────────────┐
│   Sources   │
│  (Kafka,    │
│   Files)    │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│   BRONZE        │  ← Phase 2.1 : Ingestion brute
│   (Raw Data)    │
└──────┬──────────┘
       │
       │ Nettoyage & Validation
       ▼
┌─────────────────┐
│   SILVER        │  ← Phase 2.2 : Données nettoyées
│   (Cleaned)     │
└──────┬──────────┘
       │
       │ Agrégation & Optimisation
       ▼
┌─────────────────┐
│   GOLD          │  ← Futur : Données analytiques
│   (Aggregated)  │
└─────────────────┘
```

## Avantages de l'architecture Médaillon

### 1. Séparation des responsabilités

- **Bronze** : Ingestion et stockage
- **Silver** : Qualité et normalisation
- **Gold** : Optimisation et consommation

### 2. Flexibilité

- Reprocessing possible à chaque niveau
- Historique complet préservé
- Évolution indépendante des niveaux

### 3. Performance

- Optimisations spécifiques par niveau
- Requêtes plus rapides sur Gold (données pré-agrégées)
- Réduction du volume de données à chaque niveau

### 4. Qualité des données

- Validation progressive
- Traçabilité complète
- Gestion des erreurs à chaque niveau

## Implémentation avec Delta Lake

### Avantages de Delta Lake

- **Transactions ACID** : Garantit la cohérence
- **Time Travel** : Accès aux versions historiques
- **Schema Evolution** : Gestion automatique des changements de schéma
- **Optimisations** : Z-ordering, compaction automatique

### Configuration par niveau

#### Bronze
```python
# Append uniquement, pas de modifications
outputMode("append")
partitionBy("building_id", "sensor_type")
```

#### Silver
```python
# Update pour corrections
outputMode("update")
partitionBy("building_id", "date")
mergeSchema=True  # Évolution du schéma
```

#### Gold
```python
# Complete pour agrégations
outputMode("complete")
partitionBy("building_id", "date")
optimizeWrite=True  # Optimisations automatiques
```

## Cas d'usage SmartTech détaillés

### Phase 2.1 : Bronze Layer

**Objectif** : Ingérer toutes les données IoT sans perte

**Pipeline** :
```
Fichiers JSON → Spark Streaming → Delta Lake Bronze
```

**Caractéristiques** :
- Format : JSON brut
- Mode : Append
- Partitionnement : building_id, sensor_type
- Checkpoint : `/checkpoints/bronze`

**Données stockées** :
- Toutes les mesures brutes (même invalides)
- Métadonnées d'ingestion (timestamp, source_format, pipeline_version)
- Normalisation minimale de schéma (compatibilité)
- Format original préservé (timestamp_string)

**❌ Transformations NON effectuées en Bronze** :
- Filtrage strict des valeurs → Silver
- Validation approfondie → Silver
- Colonnes calculées métier → Silver
- Agrégations → Gold

### Phase 2.2 : Silver Layer

**Objectif** : Nettoyer et normaliser les données pour l'analyse

**Pipeline** :
```
Kafka → Spark Streaming → Transformations → Delta Lake Silver
```

**Caractéristiques** :
- Format : Données normalisées
- Mode : Update (pour corrections)
- Partitionnement : building_id, date
- Checkpoint : `/checkpoints/silver`

**Transformations** :
- Nettoyage approfondi des valeurs nulles
- Validation stricte des plages (température, humidité, énergie)
- Normalisation approfondie des timestamps
- Filtrage des données invalides
- Enrichissement avec colonnes calculées métier (temp_status, energy_status)
- Score de qualité des données (data_quality_score)
- Statuts de validation (temperature_status, humidity_status, energy_status)

### Phase 2.3 : Gold Layer

**Objectif** : Créer des vues analytiques optimisées avec agrégations

**Pipeline** :
```
Silver (Delta Lake) → Spark Streaming → Agrégations → Gold (Delta Lake)
```

**Agrégations implémentées** :
- **Agrégations horaires** : Moyennes, min, max par bâtiment et par heure
- **Statistiques par capteur** : Stats sur fenêtres glissantes (1h, slide 30min)
- **KPIs quotidiens** : Consommation totale, taux d'anomalies, capteurs actifs

**Caractéristiques** :
- Format : Données agrégées
- Mode : Complete (pour agrégations avec fenêtres)
- Windowing : Fenêtres temporelles (horaires, quotidiennes)
- Watermarks : Gestion des données tardives (1 heure)
- Optimisations : Partitionnement par building_id
- Schémas dénormalisés pour performance

## Bonnes pratiques

### 1. Gestion des schémas

**Bronze** : Accepter tous les schémas (mergeSchema=True)
**Silver** : Schéma strict et validé
**Gold** : Schémas optimisés pour les requêtes

### 2. Rétention des données

**Bronze** : Rétention longue (audit, reprocessing)
**Silver** : Rétention moyenne (analyse historique)
**Gold** : Rétention selon besoins business

### 3. Monitoring

- Volume de données par niveau
- Qualité des données (taux d'erreur)
- Latence des pipelines
- Performance des requêtes

### 4. Sécurité

- Contrôles d'accès par niveau
- Bronze : Accès restreint (données brutes)
- Silver : Accès analytique
- Gold : Accès large (données agrégées)

## Sources

- [Delta Lake - Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Databricks - Bronze, Silver, Gold](https://docs.databricks.com/delta/medallion.html)
- [Delta Lake Best Practices](https://docs.delta.io/best-practices.html)
- [Data Lakehouse Architecture](https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html)

