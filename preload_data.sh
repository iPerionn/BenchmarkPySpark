#!/bin/bash
# Ce script d'initialisation se charge de précharger les données en mémoire

# Chemin des données dans Google Cloud Storage
DATA_PATH="gs://public_lddm_data/"

# Chemin de cache sur les nœuds (répertoire temporaire)
CACHE_PATH="/tmp/pagerank_data/"

# Créer un répertoire temporaire pour le cache des données
mkdir -p $CACHE_PATH

# Télécharger les données depuis GCS vers le répertoire temporaire local
gsutil -m cp -r $DATA_PATH* $CACHE_PATH

echo "Les données sont préchargées dans le répertoire $CACHE_PATH"