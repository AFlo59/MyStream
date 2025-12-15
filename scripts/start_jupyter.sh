#!/bin/bash
# Script pour démarrer Jupyter Notebook dans le conteneur Spark

echo "🚀 Démarrage de Jupyter Notebook..."

docker exec -it spark-master jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --notebook-dir=/opt/bitnami/spark/notebooks

echo ""
echo "✓ Jupyter Notebook accessible sur : http://localhost:8888"
echo "   (Vérifiez le token dans les logs ci-dessus)"

