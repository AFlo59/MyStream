#!/bin/bash
# Script pour rebuild complet de l'environnement Docker

echo "=========================================="
echo "SmartTech Streaming - Rebuild Complet"
echo "=========================================="

# Arrêter et supprimer les conteneurs et volumes
echo ""
echo "📦 Arrêt et suppression des conteneurs..."
docker-compose down -v

# Supprimer l'image existante si elle existe
echo ""
echo "🗑️  Suppression de l'image existante..."
docker rmi smarttech-spark:latest 2>/dev/null || echo "   (Image n'existe pas encore)"

# Nettoyer le cache Docker (optionnel mais recommandé)
echo ""
echo "🧹 Nettoyage du cache Docker..."
docker system prune -f

# Rebuild l'image sans cache avec docker build directement
echo ""
echo "🔨 Construction de l'image Docker (sans cache)..."
docker build -t smarttech-spark:latest --no-cache .

if [ $? -eq 0 ]; then
    echo "✓ Image construite avec succès"
else
    echo "❌ Erreur lors de la construction de l'image"
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
echo "🚀 Démarrage des services..."
docker-compose up -d

if [ $? -eq 0 ]; then
    echo "✓ Services démarrés"
else
    echo "❌ Erreur lors du démarrage des services"
    exit 1
fi

# Attendre que les services soient prêts
echo ""
echo "⏳ Attente du démarrage complet des services..."
sleep 5

# Vérifier le statut
echo ""
echo "📊 Statut des services :"
docker-compose ps

echo ""
echo "=========================================="
echo "✅ Rebuild terminé !"
echo "=========================================="
echo ""
echo "Interfaces disponibles :"
echo "  - Spark Master UI : http://localhost:8080"
echo "  - Spark Worker UI : http://localhost:8081"
echo "  - Jupyter Notebook : http://localhost:8888"
echo ""

