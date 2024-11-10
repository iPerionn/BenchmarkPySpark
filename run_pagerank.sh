#!/bin/bash

# Variables de configuration
PROJECT_ID="donneedistribuees"   # Remplacez par votre Project ID GCP
REGION="us-central1"             # Modifiez selon votre région préférée
ZONE="us-central1-a"             # Modifiez selon votre zone préférée
REPO_URL="https://github.com/iPerionn/BenchmarkPySpark"  # URL du dépôt GitHub
SCRIPT_NAME="pagerank_an_pyspark_rdd.py"      # Nom du script à exécuter
INPUT_DATA="gs://public_lddm_data/"  # Lien vers les données
OUTPUT_BUCKET="gs://benchmark_output"  # Remplacez par votre bucket GCS

gsutil cp ./${SCRIPT_NAME} gs://benchmark_output/ 

# Nombre d'itérations pour collecter 4 fois les données
NUM_RUNS=3
# Définir le nom du cluster pour cette exécution
CLUSTER_NAME="pagerank-cluster-$run"
OUTPUT_DATA="${OUTPUT_BUCKET}/run_$run"  # Dossier de sortie spécifique pour chaque exécution


echo "Création du cluster Dataproc $CLUSTER_NAME avec 1 nœud de travail..."
gcloud dataproc clusters create $CLUSTER_NAME \
    --region $REGION \
    --zone $ZONE \
    --single-node \
    --master-machine-type "n2-highmem-16" \
    --worker-machine-type "n2-highmem-16" \
    --master-boot-disk-size "100GB" \
    --image-version "2.0-debian10" \
    --project $PROJECT_ID

if [ $? -eq 0 ]; then
    for run in $(seq 1 $NUM_RUNS); do
        echo "Exécution du script PySpark RDD PageRank sur le cluster $CLUSTER_NAME (Run $run)..."
        
        gcloud dataproc jobs submit pyspark gs://benchmark_output/${SCRIPT_NAME} \
            --cluster $CLUSTER_NAME \
            --region $REGION \
            -- gs://benchmark_output/  # Ajustez ce chemin selon vos besoins de sortie

        echo "Exécution $run terminée. Les résultats sont disponibles dans $OUTPUT_DATA"
    done
else
    echo "Failed to create the cluster. Exiting..."
    exit 1
fi

# Attendre que tous les jobs en arrière-plan soient terminés
wait

echo "Tous les jobs sont terminés. Suppression du cluster..."
gcloud dataproc clusters delete $CLUSTER_NAME --region $REGION --quiet


