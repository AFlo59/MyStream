#!/usr/bin/env python3
"""
Générateur de données de test pour les capteurs IoT SmartTech
Simule un flux continu de données JSON pour la pipeline Bronze
"""

import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

def generate_sensor_data(sensor_id: str, base_timestamp: datetime) -> dict:
    """
    Génère une mesure de capteur réaliste
    
    Args:
        sensor_id: Identifiant du capteur
        base_timestamp: Timestamp de base pour la mesure
    
    Returns:
        dict: Données du capteur au format JSON
    """
    # Simulation de données réalistes
    base_temp = random.uniform(18, 25)  # Température de base entre 18-25°C
    base_humidity = random.uniform(40, 60)  # Humidité entre 40-60%
    base_energy = random.uniform(100, 500)  # Consommation énergétique en kWh
    
    # Ajouter des variations réalistes
    temperature = round(base_temp + random.uniform(-2, 2), 2)
    humidity = round(base_humidity + random.uniform(-5, 5), 2)
    energy_consumption = round(base_energy + random.uniform(-50, 50), 2)
    
    # Détection d'anomalies (10% de chance)
    anomaly_detected = random.random() < 0.1
    if anomaly_detected:
        # Simuler une anomalie (température extrême, consommation élevée, etc.)
        if random.random() < 0.5:
            temperature = random.uniform(30, 40)  # Température anormalement élevée
        else:
            energy_consumption = random.uniform(800, 1200)  # Consommation anormale
    
    return {
        "sensor_id": sensor_id,
        "timestamp": base_timestamp.isoformat(),
        "temperature": temperature,
        "humidity": humidity,
        "energy_consumption": energy_consumption,
        "anomaly_detected": anomaly_detected,
        "building_id": f"building_{random.randint(1, 5)}",
        "sensor_type": random.choice(["temperature", "humidity", "energy", "motion"]),
        "location": random.choice(["room_101", "room_102", "lobby", "parking", "roof"])
    }

def generate_json_files(output_dir: Path, num_files: int = 10, records_per_file: int = 100):
    """
    Génère plusieurs fichiers JSON pour simuler un flux continu
    
    Args:
        output_dir: Répertoire de sortie
        num_files: Nombre de fichiers à générer
        records_per_file: Nombre d'enregistrements par fichier
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Liste des capteurs
    sensor_ids = [f"sensor_{i:03d}" for i in range(1, 21)]  # 20 capteurs
    
    base_time = datetime.now() - timedelta(hours=1)
    
    for file_num in range(1, num_files + 1):
        filename = output_dir / f"sensor_data_{file_num:04d}.json"
        
        # Générer les données pour ce fichier
        records = []
        for record_num in range(records_per_file):
            sensor_id = random.choice(sensor_ids)
            # Chaque enregistrement est espacé de quelques secondes
            timestamp = base_time + timedelta(
                seconds=(file_num - 1) * records_per_file * 5 + record_num * 5
            )
            records.append(generate_sensor_data(sensor_id, timestamp))
        
        # Écrire le fichier JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Généré : {filename} ({len(records)} enregistrements)")
    
    print(f"\n✓ {num_files} fichiers générés dans {output_dir}")

if __name__ == "__main__":
    import os
    
    # Dans Docker : les variables sont injectées par docker-compose depuis .env
    # En local : utiliser load_dotenv() si nécessaire
    try:
        from dotenv import load_dotenv
        # Essayer de charger .env seulement si on est en local (pas dans Docker)
        if not os.path.exists("/opt/spark"):
            # On est en local, charger le .env
            project_root = Path(__file__).parent.parent
            env_path = project_root / ".env"
            if env_path.exists():
                load_dotenv(env_path)
    except ImportError:
        # python-dotenv pas installé, continuer avec les variables d'environnement système
        pass
    
    # Utiliser DATA_DIR depuis les variables d'environnement ou valeur par défaut
    project_root = Path(__file__).parent.parent
    data_dir_env = os.getenv("DATA_DIR")
    
    if data_dir_env:
        # Si DATA_DIR est un chemin absolu (Docker), utiliser tel quel
        if os.path.isabs(data_dir_env):
            data_dir = Path(data_dir_env)
        else:
            # Chemin relatif, construire depuis la racine du projet
            data_dir = project_root / data_dir_env.lstrip("/")
    else:
        # Valeur par défaut
        data_dir = project_root / "data"
    
    print("Génération de données de test IoT pour SmartTech...")
    print(f"Répertoire de sortie : {data_dir}\n")
    
    generate_json_files(data_dir, num_files=10, records_per_file=100)
    
    print("\n✓ Génération terminée !")
    print("Vous pouvez maintenant utiliser ces fichiers pour tester la pipeline Bronze.")

