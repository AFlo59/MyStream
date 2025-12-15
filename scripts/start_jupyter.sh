#!/bin/bash
# Script pour démarrer/redémarrer Jupyter Notebook dans le conteneur dédié
# Note : Jupyter démarre automatiquement avec le conteneur spark-jupyter
# Ce script peut être utilisé pour redémarrer Jupyter manuellement si nécessaire

echo "🚀 Démarrage de Jupyter Notebook..."

docker exec -it spark-jupyter jupyter notebook \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --notebook-dir=/opt/spark/notebooks \
    --NotebookApp.token='' \
    --NotebookApp.password=''

echo ""
echo "✓ Jupyter Notebook accessible sur : http://localhost:8888"
echo "   (Aucun token requis - accès direct)"

