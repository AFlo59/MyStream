# Clarification : Architecture Médaillon - Répartition Bronze/Silver/Gold

## Question posée

> "J'ai déjà des agrégations dans ma pipeline Bronze et des transformations. Ne devraient-elles pas être en étape Silver ? Et potentiellement Gold ?"

**Excellente question !** C'est une préoccupation légitime sur la séparation des responsabilités dans l'architecture Médaillon.

## Analyse de la pipeline Bronze actuelle

### Transformations présentes dans Bronze

1. ✅ **Normalisation de schéma** (device_id → sensor_id)
   - **Justification** : Compatibilité entre formats de données
   - **Acceptable en Bronze** : Oui, pour assurer la lisibilité minimale

2. ⚠️ **Normalisation des timestamps** (ISO 8601)
   - **Justification** : Parsing nécessaire pour stocker en format timestamp
   - **Acceptable en Bronze** : Limite acceptable, mais idéalement en Silver

3. ⚠️ **Filtrage avec validation des plages** (température, humidité)
   - **Justification** : Éviter d'écrire des données complètement invalides
   - **Acceptable en Bronze** : Non, devrait être en Silver

4. ⚠️ **Colonnes calculées** (temp_status, energy_status)
   - **Justification** : Enrichissement des données
   - **Acceptable en Bronze** : Non, devrait être en Silver ou Gold

5. ✅ **Agrégations pour visualisation** (groupBy().count())
   - **Justification** : Monitoring et affichage uniquement
   - **Acceptable en Bronze** : Oui, car pas écrites dans Bronze

## Architecture Médaillon idéale

### Bronze Layer : Données brutes

**Principe** : Stocker les données telles qu'elles arrivent, avec transformations minimales.

**Transformations acceptables** :
- ✅ Parsing basique (JSON → DataFrame)
- ✅ Normalisation de schéma minimale (pour compatibilité)
- ✅ Ajout de métadonnées d'ingestion (`ingestion_timestamp`)
- ✅ Validation basique (éviter les données complètement corrompues)

**Transformations à éviter** :
- ❌ Filtrage strict des valeurs
- ❌ Normalisation approfondie
- ❌ Calculs métier
- ❌ Agrégations

**Exemple idéal Bronze** :
```python
bronze_stream = raw_stream \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("json_files")) \
    # Pas de filtrage strict, pas de calculs
```

### Silver Layer : Données nettoyées

**Principe** : Nettoyer, valider et normaliser les données pour l'analyse.

**Transformations attendues** :
- ✅ Nettoyage approfondi (valeurs nulles, formats)
- ✅ Validation des plages de valeurs
- ✅ Normalisation des formats (timestamps, unités)
- ✅ Déduplication si nécessaire
- ✅ Enrichissement avec métadonnées calculées
- ✅ Filtrage des données invalides

**Exemple Silver** :
```python
silver_stream = bronze_df \
    .filter(
        (col("temperature").between(-50, 60)) &
        (col("humidity").between(0, 100))
    ) \
    .withColumn("data_quality_score", calculate_quality_score()) \
    .withColumn("temp_status", 
        when(col("temperature") > 30, "HIGH").otherwise("NORMAL")
    )
```

### Gold Layer : Données agrégées

**Principe** : Créer des vues analytiques optimisées pour la consommation.

**Transformations attendues** :
- ✅ Agrégations temporelles (moyennes horaires, quotidiennes)
- ✅ Agrégations par dimension (par bâtiment, par capteur)
- ✅ Métriques calculées (tendances, anomalies)
- ✅ Optimisations pour les requêtes (dénormalisation)

**Exemple Gold** :
```python
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
```

## Recommandations pour le projet SmartTech

### Option 1 : Approche pragmatique (actuelle)

**Justification** : Pour un projet pédagogique, il est acceptable d'avoir quelques transformations en Bronze pour :
- Simplifier la démonstration
- Montrer les concepts de streaming
- Éviter une complexité excessive

**Ce qui reste en Bronze** :
- Normalisation de schéma minimale
- Parsing des timestamps (nécessaire pour le type)
- Validation basique (éviter les données complètement invalides)

**Ce qui devrait être déplacé vers Silver** :
- Filtrage strict des plages de valeurs
- Colonnes calculées (temp_status, energy_status)

### Option 2 : Approche stricte Médaillon (recommandée pour production)

**Réorganisation proposée** :

#### Bronze : Minimal
```python
# Bronze : Juste ingestion avec métadonnées
bronze_stream = raw_stream \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_format", lit("json")) \
    # Pas de filtrage, pas de calculs
    # Accepter toutes les données (même invalides)
```

#### Silver : Nettoyage complet
```python
# Silver : Toutes les transformations de qualité
silver_stream = bronze_df \
    # Normalisation des timestamps
    .withColumn("timestamp", normalize_timestamp(col("timestamp"))) \
    # Validation et filtrage
    .filter(validate_data_quality()) \
    # Enrichissement
    .withColumn("temp_status", calculate_temp_status()) \
    .withColumn("data_quality_score", calculate_quality())
```

#### Gold : Agrégations
```python
# Gold : Agrégations pour l'analyse
gold_stream = silver_df \
    .groupBy(window("timestamp", "1 hour"), "building_id") \
    .agg(avg("temperature"), sum("energy_consumption"))
```

## Comparaison : Pipeline actuelle vs idéale

| Aspect | Pipeline actuelle (Bronze) | Pipeline idéale |
|--------|---------------------------|-----------------|
| **Normalisation schéma** | ✅ En Bronze | ✅ En Bronze |
| **Parsing timestamps** | ⚠️ En Bronze | ⚠️ En Bronze (acceptable) |
| **Filtrage valeurs** | ⚠️ En Bronze | ❌ En Silver |
| **Colonnes calculées** | ⚠️ En Bronze | ❌ En Silver |
| **Agrégations** | ✅ Juste pour affichage | ✅ En Gold |

## Impact sur le projet

### Pour la démonstration pédagogique

**Approche actuelle acceptable** car :
- Montre les concepts de streaming
- Simplifie la compréhension
- Les deux pipelines (Bronze et Silver) restent distinctes

### Pour la production

**Réorganisation recommandée** :
1. **Simplifier Bronze** : Ingestion minimale uniquement
2. **Enrichir Silver** : Déplacer filtrage et calculs
3. **Créer Gold** : Ajouter les agrégations

## Conclusion

Votre observation est **correcte** ! Dans une architecture Médaillon stricte :

- **Bronze** devrait être le plus brut possible
- **Silver** devrait faire le nettoyage et normalisation
- **Gold** devrait faire les agrégations

**Pour ce projet** :
- L'approche actuelle est acceptable pour la démonstration
- Une réorganisation serait idéale pour la production
- Les concepts fondamentaux restent valides

**Recommandation** : Garder l'approche actuelle pour la démonstration, mais documenter que pour la production, certaines transformations devraient être déplacées vers Silver/Gold.

## Implémentation réalisée

✅ **Réorganisation complète effectuée selon l'architecture Médaillon stricte** :

1. ✅ **Bronze simplifié** : Ingestion minimale uniquement
   - Normalisation de schéma minimale (compatibilité)
   - Métadonnées d'ingestion
   - Pas de filtrage strict
   - Pas de colonnes calculées métier

2. ✅ **Silver enrichi** : Toutes les transformations de qualité
   - Nettoyage approfondi
   - Validation stricte des plages
   - Filtrage des données invalides
   - Colonnes calculées métier (temp_status, energy_status_calculated)
   - Score de qualité

3. ✅ **Gold créé** : Agrégations temporelles
   - Agrégations horaires par bâtiment
   - Statistiques par capteur (fenêtres glissantes)
   - KPIs quotidiens
   - Utilisation de windowing et watermarks

## Architecture finale

```
JSON Files
    ↓
BRONZE (01_pipeline_bronze.ipynb)
    - Données brutes
    - Pas de filtrage
    ↓
Kafka Topic
    ↓
SILVER (02_pipeline_silver.ipynb)
    - Nettoyage approfondi
    - Validation stricte
    - Colonnes calculées
    ↓
GOLD (03_pipeline_gold.ipynb)
    - Agrégations horaires
    - Stats par capteur
    - KPIs quotidiens
```
