# Architecture Médaillon Finale - Réorganisation Complète

## ✅ Réorganisation effectuée

L'architecture a été réorganisée selon les principes stricts de l'architecture Médaillon avec **3 niveaux distincts** :

### 🥉 Bronze : Données Brutes

**Notebook** : `01_pipeline_bronze.ipynb`

**Principe** : Ingestion minimale, toutes les données conservées

**Transformations acceptables** :
- ✅ Normalisation minimale de schéma (compatibilité device_id → sensor_id)
- ✅ Parsing basique (string → timestamp pour type)
- ✅ Métadonnées d'ingestion (ingestion_timestamp, source_format, pipeline_version)
- ✅ Conservation du format original (timestamp_string)

**Transformations retirées** :
- ❌ Filtrage strict des valeurs → Déplacé vers Silver
- ❌ Validation approfondie → Déplacé vers Silver
- ❌ Colonnes calculées métier (temp_status, energy_status) → Déplacé vers Silver

**Schéma Bronze** :
```
- sensor_id, timestamp, timestamp_string
- temperature, humidity, energy_consumption
- anomaly_detected, building_id, sensor_type, location
- ingestion_timestamp, source_format, pipeline_version
```

### 🥈 Silver : Données Nettoyées

**Notebook** : `02_pipeline_silver.ipynb`

**Principe** : Qualité garantie pour l'analyse

**Transformations ajoutées** :
- ✅ Nettoyage approfondi (valeurs nulles, formats)
- ✅ Validation stricte des plages (température, humidité, énergie)
- ✅ Filtrage des données invalides
- ✅ Colonnes calculées métier (temp_status, energy_status_calculated)
- ✅ Score de qualité (data_quality_score)
- ✅ Statuts de validation (temperature_status, humidity_status, energy_status)

**Source** : Kafka Topic (sensor-data-iot)

**Schéma Silver** :
```
- Toutes les colonnes Bronze +
- data_quality_score
- temperature_status, humidity_status, energy_status
- temp_status, energy_status_calculated
```

### 🥇 Gold : Données Agrégées

**Notebook** : `03_pipeline_gold.ipynb`

**Principe** : Optimisées pour l'analyse et la consommation

**Agrégations implémentées** :

1. **Agrégations horaires** (`/delta/gold/hourly_stats`)
   - Moyennes, min, max par bâtiment et par heure
   - Consommation totale, nombre de capteurs, anomalies

2. **Statistiques par capteur** (`/delta/gold/sensor_stats`)
   - Stats sur fenêtres glissantes (1h, slide 30min)
   - Dernières valeurs, moyennes, totaux par capteur

3. **KPIs quotidiens** (`/delta/gold/daily_kpis`)
   - Consommation énergétique totale quotidienne
   - Température moyenne quotidienne
   - Taux d'anomalies
   - Capteurs actifs

**Concepts utilisés** :
- Windowing (fenêtres temporelles)
- Watermarks (gestion données tardives)
- Mode Complete (pour agrégations)

**Source** : Delta Lake Silver (streaming)

## Flux de données complet

```
┌─────────────┐
│ JSON Files  │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│   BRONZE        │  ← 01_pipeline_bronze.ipynb
│   (Raw Data)    │     - Ingestion minimale
└──────┬──────────┘     - Pas de filtrage
       │                 - Toutes les données
       │
       │ (optionnel)
       ▼
┌─────────────┐
│   Kafka     │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│   SILVER        │  ← 02_pipeline_silver.ipynb
│   (Cleaned)     │     - Nettoyage approfondi
└──────┬──────────┘     - Validation stricte
       │                 - Colonnes calculées
       │
       ▼
┌─────────────────┐
│   GOLD          │  ← 03_pipeline_gold.ipynb
│   (Aggregated)  │     - Agrégations horaires
└─────────────────┘     - Stats par capteur
                        - KPIs quotidiens
```

## Comparaison : Avant vs Après

| Aspect | Avant (Pédagogique) | Après (Médaillon strict) |
|--------|---------------------|--------------------------|
| **Bronze** | Transformations + filtrage | Ingestion minimale uniquement |
| **Silver** | Nettoyage basique | Nettoyage approfondi + calculs |
| **Gold** | ❌ N'existe pas | ✅ Agrégations complètes |

## Utilisation

### Ordre d'exécution recommandé

1. **Bronze** : `notebooks/01_pipeline_bronze.ipynb`
   - Génère les données JSON
   - Ingère dans Delta Bronze

2. **Silver** : `notebooks/02_pipeline_silver.ipynb`
   - Lance le producer Kafka
   - Nettoie et valide les données
   - Écrit dans Delta Silver

3. **Gold** : `notebooks/03_pipeline_gold.ipynb`
   - Lit depuis Silver (streaming)
   - Calcule les agrégations
   - Écrit dans Delta Gold

### Nettoyage entre les niveaux

Si vous voulez repartir de zéro :

```bash
# Nettoyer Bronze
CLEAN_DELTA = True dans 01_pipeline_bronze.ipynb (cellule 1)

# Nettoyer Silver
CLEAN_SILVER = True dans 02_pipeline_silver.ipynb (cellule 1)

# Nettoyer Gold
CLEAN_GOLD = True dans 03_pipeline_gold.ipynb (cellule 1)
```

Ou utiliser les scripts :
```bash
./scripts/clean_delta.sh
```

## Avantages de cette architecture

1. **Séparation claire des responsabilités**
   - Bronze : Ingestion
   - Silver : Qualité
   - Gold : Analytics

2. **Reprocessing facilité**
   - Reprocesser Silver depuis Bronze si nécessaire
   - Reprocesser Gold depuis Silver si règles changent

3. **Traçabilité complète**
   - Historique complet dans Bronze
   - Métadonnées d'ingestion préservées

4. **Performance optimisée**
   - Bronze : Stockage rapide (pas de calculs)
   - Silver : Requêtes sur données nettoyées
   - Gold : Requêtes ultra-rapides (données pré-agrégées)

## Conformité avec la roadmap

✅ **Respecte la roadmap** :
- Phase 2.1 : Bronze (JSON → Delta) ✅
- Phase 2.2 : Silver (Kafka → Delta) ✅
- Phase 2.3 : Gold (Silver → Agrégations) ✅ (extension naturelle)

✅ **Concepts couverts** :
- Structured Streaming ✅
- Checkpointing ✅
- Windowing ✅
- Watermarks ✅
- Architecture Médaillon ✅

## Prochaines étapes

1. Exécuter les 3 notebooks dans l'ordre
2. Vérifier les données à chaque niveau
3. Analyser les agrégations Gold pour les dashboards
