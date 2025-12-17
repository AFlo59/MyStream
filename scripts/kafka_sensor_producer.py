#!/usr/bin/env python3
"""
Simulateur de capteurs IoT pour Kafka
Produit des messages JSON BRUTS dans un topic Kafka pour la pipeline Silver

IMPORTANT : Ce producteur génère des données BRUTES (comme dans Bronze) avec :
- Valeurs null ou manquantes
- Formats de timestamp variés (avec/sans timezone, avec 'Z', etc.)
- Valeurs hors limites
- Types incorrects (string au lieu de number)
- Champs manquants

C'est à la pipeline Silver de NETTOYER ces données brutes.
"""

import json
import random
import time
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError

def load_existing_data_patterns(data_dir: Path) -> dict:
    """
    Charge les fichiers JSON existants et extrait les valeurs uniques pour générer des données similaires
    
    Args:
        data_dir: Répertoire contenant les fichiers JSON
    
    Returns:
        dict: Dictionnaire contenant les valeurs uniques extraites
    """
    patterns = {
        "sensor_ids": set(),
        "building_ids": set(),
        "sensor_types": set(),
        "locations": set(),
        "temperatures": [],
        "humidities": [],
        "energy_consumptions": []
    }
    
    json_files = list(data_dir.glob("sensor_data_*.json"))
    
    if not json_files:
        print(f"⚠️  Aucun fichier JSON trouvé dans {data_dir}")
        print("   Utilisation de valeurs par défaut pour la génération")
        return patterns
    
    print(f"📂 Lecture de {len(json_files)} fichier(s) JSON pour extraire les patterns...")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    continue
                    
                data = json.loads(content)
                
                # Gérer les deux formats possibles : liste ou objet unique
                records = data if isinstance(data, list) else [data]
                
                for record in records:
                    if not isinstance(record, dict):
                        continue
                        
                    if "sensor_id" in record and record["sensor_id"]:
                        patterns["sensor_ids"].add(str(record["sensor_id"]))
                    if "building_id" in record and record["building_id"]:
                        patterns["building_ids"].add(str(record["building_id"]))
                    if "sensor_type" in record and record["sensor_type"]:
                        patterns["sensor_types"].add(str(record["sensor_type"]))
                    if "location" in record and record["location"]:
                        patterns["locations"].add(str(record["location"]))
                    if "temperature" in record and record["temperature"] is not None:
                        try:
                            patterns["temperatures"].append(float(record["temperature"]))
                        except (ValueError, TypeError):
                            pass
                    if "humidity" in record and record["humidity"] is not None:
                        try:
                            patterns["humidities"].append(float(record["humidity"]))
                        except (ValueError, TypeError):
                            pass
                    if "energy_consumption" in record and record["energy_consumption"] is not None:
                        try:
                            patterns["energy_consumptions"].append(float(record["energy_consumption"]))
                        except (ValueError, TypeError):
                            pass
        except json.JSONDecodeError as e:
            print(f"   ⚠️  Erreur JSON dans {json_file.name}: {e}")
            continue
        except Exception as e:
            print(f"   ⚠️  Erreur lors de la lecture de {json_file.name}: {e}")
            continue
    
    # Convertir les sets en listes pour faciliter l'utilisation
    patterns["sensor_ids"] = list(patterns["sensor_ids"])
    patterns["building_ids"] = list(patterns["building_ids"])
    patterns["sensor_types"] = list(patterns["sensor_types"])
    patterns["locations"] = list(patterns["locations"])
    
    print(f"   ✓ {len(patterns['sensor_ids'])} sensor_id(s) unique(s)")
    print(f"   ✓ {len(patterns['building_ids'])} building_id(s) unique(s)")
    print(f"   ✓ {len(patterns['sensor_types'])} sensor_type(s) unique(s)")
    print(f"   ✓ {len(patterns['locations'])} location(s) unique(s)")
    print(f"   ✓ {len(patterns['temperatures'])} valeur(s) de température")
    print(f"   ✓ {len(patterns['humidities'])} valeur(s) d'humidité")
    print(f"   ✓ {len(patterns['energy_consumptions'])} valeur(s) de consommation")
    
    return patterns

def generate_sensor_data(sensor_id: str, base_timestamp: datetime, patterns: dict = None) -> dict:
    """
    Génère des données BRUTES de capteur (comme dans Bronze) pour que Silver puisse les nettoyer
    
    Les données peuvent contenir :
    - Valeurs null ou manquantes
    - Formats de timestamp variés (avec/sans timezone, avec 'Z', etc.)
    - Valeurs hors limites
    - Types incorrects (string au lieu de number)
    - Champs manquants
    
    Args:
        sensor_id: Identifiant du capteur
        base_timestamp: Timestamp de base pour la mesure
        patterns: Dictionnaire contenant les valeurs uniques extraites des JSON existants
    
    Returns:
        dict: Données BRUTES du capteur au format JSON (à nettoyer dans Silver)
    """
    # Utiliser les patterns si disponibles, sinon valeurs par défaut
    if patterns and patterns["sensor_ids"]:
        # Utiliser un sensor_id existant ou générer un nouveau basé sur les patterns
        if sensor_id not in patterns["sensor_ids"]:
            sensor_id = random.choice(list(patterns["sensor_ids"]))
    else:
        # Valeurs par défaut si aucun pattern
        if not patterns:
            patterns = {
                "building_ids": [],
                "sensor_types": [],
                "locations": [],
                "temperatures": [],
                "humidities": [],
                "energy_consumptions": []
            }
    
    # Sélectionner building_id depuis les patterns ou générer
    if patterns["building_ids"]:
        building_id = random.choice(list(patterns["building_ids"]))
    else:
        building_id = f"building_{random.randint(1, 5)}"
    
    # Sélectionner sensor_type depuis les patterns ou générer
    if patterns["sensor_types"]:
        sensor_type = random.choice(list(patterns["sensor_types"]))
    else:
        sensor_type = random.choice(["temperature", "humidity", "co2", "energy", "motion"])
    
    # Sélectionner location depuis les patterns ou générer
    if patterns["locations"]:
        location = random.choice(list(patterns["locations"]))
    else:
        location = random.choice(["room_101", "room_102", "lobby", "parking", "roof"])
    
    # Générer les données BRUTES avec des problèmes potentiels (comme dans Bronze)
    data = {
        "sensor_id": sensor_id,
        "building_id": building_id,
        "sensor_type": sensor_type,
        "location": location
    }
    
    # Format de timestamp VARIABLE (comme dans Bronze) - 30% avec 'Z', 30% avec timezone, 40% sans timezone
    timestamp_format = random.random()
    if timestamp_format < 0.3:
        # Format avec 'Z' (UTC)
        if base_timestamp.tzinfo is None:
            timestamp_utc = base_timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp_utc = base_timestamp.astimezone(timezone.utc)
        data["timestamp"] = timestamp_utc.isoformat().replace('+00:00', 'Z')
    elif timestamp_format < 0.6:
        # Format avec timezone offset
        if base_timestamp.tzinfo is None:
            timestamp_utc = base_timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp_utc = base_timestamp.astimezone(timezone.utc)
        data["timestamp"] = timestamp_utc.isoformat()
    else:
        # Format sans timezone (problème potentiel)
        data["timestamp"] = base_timestamp.isoformat()
    
    # Générer les valeurs numériques avec des problèmes potentiels
    # 15% de chance de valeur null
    # 10% de chance de valeur hors limites
    # 5% de chance de type incorrect (string)
    
    # Température
    temp_rand = random.random()
    if temp_rand < 0.15:
        data["temperature"] = None  # Valeur null
    elif temp_rand < 0.25:
        data["temperature"] = random.uniform(-100, 200)  # Valeur hors limites
    elif temp_rand < 0.30:
        data["temperature"] = "invalid"  # Type incorrect
    else:
        if patterns["temperatures"]:
            base_temp = random.choice(patterns["temperatures"])
            data["temperature"] = round(base_temp + random.uniform(-2, 2), 2)
        else:
            base_temp = random.uniform(18, 25)
            data["temperature"] = round(base_temp + random.uniform(-2, 2), 2)
    
    # Humidité
    hum_rand = random.random()
    if hum_rand < 0.15:
        data["humidity"] = None  # Valeur null
    elif hum_rand < 0.25:
        data["humidity"] = random.uniform(-50, 150)  # Valeur hors limites
    elif hum_rand < 0.30:
        data["humidity"] = "N/A"  # Type incorrect
    else:
        if patterns["humidities"]:
            base_humidity = random.choice(patterns["humidities"])
            data["humidity"] = round(base_humidity + random.uniform(-5, 5), 2)
        else:
            base_humidity = random.uniform(40, 60)
            data["humidity"] = round(base_humidity + random.uniform(-5, 5), 2)
    
    # Consommation d'énergie
    energy_rand = random.random()
    if energy_rand < 0.15:
        data["energy_consumption"] = None  # Valeur null
    elif energy_rand < 0.25:
        data["energy_consumption"] = random.uniform(-1000, 5000)  # Valeur hors limites
    elif energy_rand < 0.30:
        data["energy_consumption"] = "unknown"  # Type incorrect
    else:
        if patterns["energy_consumptions"]:
            base_energy = random.choice(patterns["energy_consumptions"])
            data["energy_consumption"] = round(base_energy + random.uniform(-50, 50), 2)
        else:
            base_energy = random.uniform(100, 500)
            data["energy_consumption"] = round(base_energy + random.uniform(-50, 50), 2)
    
    # Détection d'anomalies (10% de chance) - peut créer des valeurs extrêmes
    anomaly_detected = random.random() < 0.1
    if anomaly_detected:
        if random.random() < 0.5:
            data["temperature"] = random.uniform(30, 50)  # Température anormalement élevée
        else:
            data["energy_consumption"] = random.uniform(800, 2000)  # Consommation anormale
    
    data["anomaly_detected"] = anomaly_detected
    
    # 5% de chance d'avoir un champ manquant (sensor_id toujours présent)
    if random.random() < 0.05:
        # Retirer un champ aléatoire (sauf sensor_id)
        fields_to_remove = ["building_id", "sensor_type", "location", "timestamp"]
        if random.random() < 0.5:
            field = random.choice(fields_to_remove)
            if field in data:
                del data[field]
    
    return data

def create_kafka_producer(bootstrap_servers: str):
    """
    Crée et configure un producer Kafka
    
    Args:
        bootstrap_servers: Adresse du broker Kafka (ex: "localhost:9092")
    
    Returns:
        KafkaProducer: Producer Kafka configuré
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',  # Attendre la confirmation de tous les replicas
        retries=3,  # Nombre de tentatives en cas d'échec
        max_in_flight_requests_per_connection=1,  # Garantir l'ordre
        enable_idempotence=True  # Garantir exactly-once semantics
    )
    return producer

def produce_sensor_messages(
    producer: KafkaProducer,
    topic: str,
    num_sensors: int = 10,
    messages_per_sensor: int = 100,
    interval_seconds: float = 1.0,
    patterns: dict = None
):
    """
    Produit des messages de capteurs dans Kafka
    
    Args:
        producer: Producer Kafka
        topic: Nom du topic Kafka
        num_sensors: Nombre de capteurs à simuler
        messages_per_sensor: Nombre de messages par capteur
        interval_seconds: Intervalle entre les messages (en secondes)
        patterns: Dictionnaire contenant les valeurs uniques extraites des JSON existants
    """
    # Utiliser les sensor_ids depuis les patterns si disponibles
    if patterns and patterns["sensor_ids"]:
        # Utiliser les sensor_ids existants et en ajouter si nécessaire
        existing_sensor_ids = patterns["sensor_ids"]
        if len(existing_sensor_ids) >= num_sensors:
            sensor_ids = existing_sensor_ids[:num_sensors]
        else:
            sensor_ids = existing_sensor_ids + [f"sensor_{i:03d}" for i in range(len(existing_sensor_ids) + 1, num_sensors + 1)]
    else:
        sensor_ids = [f"sensor_{i:03d}" for i in range(1, num_sensors + 1)]
    
    base_time = datetime.now(timezone.utc)
    
    print(f"🚀 Démarrage de la production de messages dans le topic '{topic}'...")
    print(f"   - Nombre de capteurs : {num_sensors}")
    print(f"   - Messages par capteur : {messages_per_sensor}")
    print(f"   - Intervalle : {interval_seconds}s")
    print(f"   - Broker Kafka : {producer.config['bootstrap_servers']}")
    print()
    
    total_messages = 0
    
    try:
        for sensor_id in sensor_ids:
            print(f"📡 Production de messages pour {sensor_id}...")
            
            for i in range(messages_per_sensor):
                # Générer un timestamp avec un léger décalage temporel
                timestamp = base_time + timedelta(seconds=i * interval_seconds)
                
                # Générer les données du capteur en utilisant les patterns
                sensor_data = generate_sensor_data(sensor_id, timestamp, patterns)
                
                # Envoyer le message dans Kafka
                # Utiliser sensor_id comme clé pour garantir l'ordre par capteur
                future = producer.send(
                    topic,
                    key=sensor_id,
                    value=sensor_data
                )
                
                # Optionnel : attendre la confirmation (pour garantir l'ordre)
                # future.get(timeout=10)
                
                total_messages += 1
                
                # Afficher un message tous les 10 messages (réduire la verbosité en mode continu)
                if (i + 1) % 10 == 0:
                    print(f"   ✓ {i + 1}/{messages_per_sensor} messages envoyés")
                
                # Attendre avant d'envoyer le message suivant
                time.sleep(interval_seconds)
            
            print(f"   ✅ {sensor_id} : {messages_per_sensor} messages envoyés")
        
        # Attendre que tous les messages soient envoyés
        producer.flush()
        
        print(f"✅ Cycle terminé : {total_messages} messages envoyés dans '{topic}'\n")
        
    except KafkaError as e:
        print(f"❌ Erreur Kafka : {e}")
        raise
    except KeyboardInterrupt:
        print("\n⚠️  Production interrompue par l'utilisateur")
        producer.flush()
        print("✓ Messages en attente envoyés")

def main():
    """Point d'entrée principal"""
    import sys
    
    # Configuration depuis les variables d'environnement ou valeurs par défaut
    # Détection automatique : utiliser kafka:29092 dans Docker, localhost:9092 en local
    # Le script peut s'exécuter depuis la machine hôte ou depuis le conteneur Docker
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", 
        "kafka:29092" if os.path.exists("/opt/spark") else "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_IOT", "sensor-data-iot")
    
    # Répertoire des données JSON
    data_dir = Path(os.getenv("DATA_DIR", "/opt/spark/data"))
    
    # Paramètres de production (peuvent être passés en arguments)
    num_sensors = int(os.getenv("NUM_SENSORS", "10"))
    messages_per_sensor = int(os.getenv("MESSAGES_PER_SENSOR", "100"))
    interval_seconds = float(os.getenv("INTERVAL_SECONDS", "1.0"))
    
    # Mode continu (produire indéfiniment) ou batch (nombre limité de messages)
    continuous_mode = os.getenv("CONTINUOUS_MODE", "true").lower() == "true"
    
    # Parser les arguments de ligne de commande si présents
    if len(sys.argv) > 1:
        num_sensors = int(sys.argv[1])
    if len(sys.argv) > 2:
        messages_per_sensor = int(sys.argv[2])
    if len(sys.argv) > 3:
        interval_seconds = float(sys.argv[3])
    if len(sys.argv) > 4:
        continuous_mode = sys.argv[4].lower() == "true"
    
    print("=" * 60)
    print("SmartTech IoT - Simulateur de capteurs Kafka")
    print("=" * 60)
    print()
    
    # Charger les patterns depuis les fichiers JSON existants
    patterns = load_existing_data_patterns(data_dir)
    
    # Créer le producer Kafka
    try:
        producer = create_kafka_producer(bootstrap_servers)
        print(f"✓ Connexion à Kafka établie : {bootstrap_servers}")
    except Exception as e:
        print(f"❌ Erreur lors de la connexion à Kafka : {e}")
        print(f"   Vérifiez que Kafka est démarré et accessible sur {bootstrap_servers}")
        sys.exit(1)
    
    # Produire les messages
    try:
        if continuous_mode:
            print("🔄 Mode continu activé : production infinie de messages...")
            print("   Appuyez sur Ctrl+C pour arrêter\n")
            while True:
                produce_sensor_messages(
                    producer,
                    topic,
                    num_sensors=num_sensors,
                    messages_per_sensor=messages_per_sensor,
                    interval_seconds=interval_seconds,
                    patterns=patterns
                )
                print("\n🔄 Nouveau cycle de production...\n")
        else:
            produce_sensor_messages(
                producer,
                topic,
                num_sensors=num_sensors,
                messages_per_sensor=messages_per_sensor,
                interval_seconds=interval_seconds,
                patterns=patterns
            )
    except KeyboardInterrupt:
        print("\n⚠️  Production interrompue par l'utilisateur")
        producer.flush()
        print("✓ Messages en attente envoyés")
    except Exception as e:
        print(f"❌ Erreur lors de la production : {e}")
        sys.exit(1)
    finally:
        producer.close()
        print("✓ Producer Kafka fermé")

if __name__ == "__main__":
    main()
