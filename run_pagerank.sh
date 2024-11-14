#!/bin/bash

# Variables de configuration
PROJECT_ID="donneedistribuees"   # Remplacez par votre Project ID GCP
REGION="us-central1"             # Modifiez selon votre région préférée
REPO_URL="https://github.com/iPerionn/BenchmarkPySpark"  # URL du dépôt GitHub
SCRIPT_NAME="pagerank_the_final_sql.py"      # Nom du script à exécuter
INPUT_DATA="gs://public_lddm_data/"  # Lien vers les données
OUTPUT_BUCKET="gs://benchmark_output"  # Remplacez par votre bucket GCS

gsutil cp ./${SCRIPT_NAME} gs://benchmark_output/ 

# Définir le nom du cluster pour cette exécution
CLUSTER_NAME="cluster-singlenode"
OUTPUT_DATA="${OUTPUT_BUCKET}/${CLUSTER_NAME}"  # Dossier de sortie spécifique pour chaque exécution


echo "Création du cluster Dataproc $CLUSTER_NAME avec 1 nœud de travail..."
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --single-node \                        # Mode single-node
    --master-machine-type=n4-standard-4 \  # Type de machine pour le nœud maître (4 vCPU, 16 Go RAM)
    --worker-machine-type=n4-standard-4 \  # Type de machine pour les nœuds travailleurs (même type de machine)
    --image-version=2.0-debian10 \         # Version de l'image Dataproc
    --num-workers=0                       # Pas de nœuds workers, seulement un nœud maître
    --master-disksize=50GB \
    --project $PROJECT_ID

echo "Exécution du script PySpark RDD PageRank sur le cluster $CLUSTER_NAME ..."

gcloud dataproc jobs submit pyspark gs://benchmark_output/${SCRIPT_NAME} \
    --cluster $CLUSTER_NAME \
    --region $REGION \
    -- gs://benchmark_output/  # Ajustez ce chemin selon vos besoins de sortie

echo "Exécution terminée. Les résultats sont disponibles dans $OUTPUT_DATA"

# Attendre que tous les jobs en arrière-plan soient terminés
wait

echo "Tous les jobs sont terminés. Suppression du cluster..."
gcloud dataproc clusters delete $CLUSTER_NAME --region $REGION --quiet


