#!/bin/bash

# Variables de configuration
PROJECT_ID="donneedistribuees"   # Remplacez par votre Project ID GCP
REGION="us-central1"             # Modifiez selon votre région préférée
REPO_URL="https://github.com/iPerionn/BenchmarkPySpark"  # URL du dépôt GitHub
SCRIPT_NAME="pagerank_an_pyspark_rdd.py"      # Nom du script à exécuter
INPUT_DATA="gs://public_lddm_data/"  # Lien vers les données
OUTPUT_BUCKET="gs://benchmark_output"  # Remplacez par votre bucket GCS

gsutil cp ./${SCRIPT_NAME} gs://benchmark_output/ 

# Définir le nom du cluster pour cette exécution
CLUSTER_NAME="cluster-quadnode"
OUTPUT_DATA="${OUTPUT_BUCKET}/${CLUSTER_NAME}"  # Dossier de sortie spécifique pour chaque exécution


echo "Création du cluster Dataproc $CLUSTER_NAME avec 4 nœud de travail..."
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --master-machine-type n2-highmem-8 \
    --master-boot-disk-size 100GB \
    --num-workers 4 \
    --worker-machine-type n2-highmem-8 \
    --worker-boot-disk-size 100GB \
    --image-version "2.0-debian10" \
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


