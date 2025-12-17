# Script PowerShell d'initialisation pour lancer automatiquement le producteur Kafka
# Ce script attend que Kafka soit prêt avant de lancer le producer

$ErrorActionPreference = "Stop"

Write-Host "🚀 Initialisation du producteur Kafka..." -ForegroundColor Cyan

# Configuration
$KAFKA_BOOTSTRAP_SERVERS = if ($env:KAFKA_BOOTSTRAP_SERVERS) { $env:KAFKA_BOOTSTRAP_SERVERS } else { "kafka:29092" }
$KAFKA_TOPIC_IOT = if ($env:KAFKA_TOPIC_IOT) { $env:KAFKA_TOPIC_IOT } else { "sensor-data-iot" }
$DATA_DIR = if ($env:DATA_DIR) { $env:DATA_DIR } else { "/opt/spark/data" }
$SCRIPT_DIR = if ($env:SCRIPT_DIR) { $env:SCRIPT_DIR } else { "/opt/spark/scripts" }
$MAX_WAIT_TIME = 60  # Temps maximum d'attente en secondes
$WAIT_INTERVAL = 5   # Intervalle entre les vérifications

# Fonction pour vérifier si Kafka est prêt
function Wait-ForKafka {
    Write-Host "⏳ Attente que Kafka soit prêt..." -ForegroundColor Yellow
    $elapsed = 0
    
    while ($elapsed -lt $MAX_WAIT_TIME) {
        try {
            # Vérifier si Kafka répond (test de connexion TCP)
            $tcpClient = New-Object System.Net.Sockets.TcpClient
            $tcpClient.ReceiveTimeout = 2000
            $tcpClient.SendTimeout = 2000
            $tcpClient.Connect("kafka", 29092)
            $tcpClient.Close()
            
            Write-Host "✓ Kafka est prêt" -ForegroundColor Green
            return $true
        }
        catch {
            Write-Host "   ⏳ Kafka n'est pas encore prêt... ($elapsed s/$MAX_WAIT_TIME s)" -ForegroundColor Yellow
            Start-Sleep -Seconds $WAIT_INTERVAL
            $elapsed += $WAIT_INTERVAL
        }
    }
    
    Write-Host "⚠️  Timeout : Kafka n'est pas prêt après $MAX_WAIT_TIME secondes" -ForegroundColor Yellow
    Write-Host "   Le producteur sera lancé quand même..." -ForegroundColor Yellow
    return $false
}

# Attendre que Kafka soit prêt
Wait-ForKafka | Out-Null

# Attendre quelques secondes supplémentaires pour que le topic soit créé
Write-Host "⏳ Attente de 5 secondes supplémentaires pour la création du topic..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Lancer le producteur Kafka en mode continu
Write-Host "🚀 Lancement du producteur Kafka..." -ForegroundColor Cyan
Write-Host "   - Broker : $KAFKA_BOOTSTRAP_SERVERS"
Write-Host "   - Topic : $KAFKA_TOPIC_IOT"
Write-Host "   - Data Dir : $DATA_DIR"
Write-Host "   - Mode : Continu (production infinie)"
Write-Host ""

# Exporter les variables d'environnement pour le script Python
$env:KAFKA_BOOTSTRAP_SERVERS = $KAFKA_BOOTSTRAP_SERVERS
$env:KAFKA_TOPIC_IOT = $KAFKA_TOPIC_IOT
$env:DATA_DIR = $DATA_DIR
$env:CONTINUOUS_MODE = "true"
$env:NUM_SENSORS = if ($env:NUM_SENSORS) { $env:NUM_SENSORS } else { "10" }
$env:MESSAGES_PER_SENSOR = if ($env:MESSAGES_PER_SENSOR) { $env:MESSAGES_PER_SENSOR } else { "100" }
$env:INTERVAL_SECONDS = if ($env:INTERVAL_SECONDS) { $env:INTERVAL_SECONDS } else { "1.0" }

# Lancer le script Python
Set-Location $SCRIPT_DIR
& python3 kafka_sensor_producer.py
