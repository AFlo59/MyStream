#!/bin/bash
# Script d'installation et de démarrage pour le projet SmartTech Streaming

echo "=========================================="
echo "SmartTech Streaming - Configuration"
echo "=========================================="

# Vérifier que Docker est installé
if ! command -v docker &> /dev/null; then
    echo "❌ Docker n'est pas installé. Veuillez l'installer d'abord."
    exit 1
fi

# Vérifier que Docker Compose est installé
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose n'est pas installé. Veuillez l'installer d'abord."
    exit 1
fi

echo "✓ Docker et Docker Compose détectés"

# Construire l'image Docker personnalisée
echo ""
echo "📦 Construction de l'image Docker Spark avec Delta Lake..."
docker build -t smarttech-spark:latest .

if [ $? -eq 0 ]; then
    echo "✓ Image Docker construite avec succès"
else
    echo "❌ Erreur lors de la construction de l'image Docker"
    exit 1
fi

# Créer le fichier .env depuis env.example s'il n'existe pas
echo ""
echo "📝 Vérification du fichier .env..."
if [ ! -f .env ]; then
    if [ -f env.example ]; then
        cp env.example .env
        echo "✓ Fichier .env créé depuis env.example"
    else
        echo "⚠️  Attention : env.example n'existe pas, .env ne sera pas créé"
        echo "   docker-compose pourrait échouer si .env est requis"
    fi
else
    echo "✓ Fichier .env existe déjà"
fi

# Démarrer les services
echo ""
echo "🚀 Démarrage des services Spark..."
docker-compose up -d

if [ $? -eq 0 ]; then
    echo "✓ Services Spark démarrés"
else
    echo "❌ Erreur lors du démarrage des services"
    exit 1
fi

# Attendre que les services soient prêts
echo ""
echo "⏳ Attente du démarrage complet des services..."
sleep 10

# Vérifier le statut
echo ""
echo "📊 Statut des services :"
docker-compose ps

echo ""
echo "=========================================="
echo "✅ Configuration terminée !"
echo "=========================================="
echo ""
echo "Prochaines étapes :"
echo "1. Générer les données de test : python scripts/generate_sensor_data.py"
echo "2. Accéder à Spark UI : http://localhost:8080"
echo "3. Accéder à Jupyter Notebook : http://localhost:8888 (démarre automatiquement)"
echo "   - Ou utiliser : ./scripts/start_jupyter.sh pour redémarrer manuellement"
echo "4. Ouvrir le notebook : notebooks/01_pipeline_bronze.ipynb"
echo ""

