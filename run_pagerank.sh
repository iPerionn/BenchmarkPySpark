#!/bin/bash

# Variables de configuration
PROJECT_ID="donneedistribuees"   # Remplacez par votre Project ID GCP
REGION="us-central1"             # Modifiez selon votre région préférée
ZONE="us-central1-a"             # Modifiez selon votre zone préférée
REPO_URL="https://github.com/iPerionn/BenchmarkPySpark"  # URL du dépôt GitHub
SCRIPT_NAME="pypagerank.py"      # Nom du script à exécuter
INPUT_DATA="gs://public_lddm_data/"  # Lien vers les données
OUTPUT_BUCKET="gs://benchmark_output"  # Remplacez par votre bucket GCS

# Nombre d'itérations pour collecter 4 fois les données
NUM_RUNS=4

for run in $(seq 1 $NUM_RUNS); do
    # Définir le nom du cluster pour cette exécution
    CLUSTER_NAME="pagerank-cluster-$run"
    OUTPUT_DATA="${OUTPUT_BUCKET}/run_$run"  # Dossier de sortie spécifique pour chaque exécution

    echo "Création du cluster Dataproc $CLUSTER_NAME avec 1 nœud de travail..."
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region $REGION \
        --zone $ZONE \
        --single-node \
        --master-machine-type "n1-standard-4" \
        --master-boot-disk-size "50GB" \
        --image-version "2.0-debian10" \
        --project $PROJECT_ID

    # Se connecter au cluster Dataproc, cloner le repo GitHub, et exécuter le script
    echo "Clonage du dépôt GitHub et exécution du script PySpark RDD PageRank sur le cluster $CLUSTER_NAME (Run $run)..."
    
    gcloud dataproc jobs submit pyspark \
        --cluster $CLUSTER_NAME \
        --region $REGION \
        --py-files gs://your-bucket-name/path-to-your-script/pypagerank.py \
        -- gs://your-bucket-name/output_data/ # Adjust this path to where you want the output

    # Supprimer le cluster après l'exécution
    echo "Suppression du cluster $CLUSTER_NAME..."
    gcloud dataproc clusters delete $CLUSTER_NAME --region $REGION --quiet

    echo "Exécution $run terminée. Les résultats sont disponibles dans $OUTPUT_DATA"
done

echo "Toutes les exécutions sont terminées."
