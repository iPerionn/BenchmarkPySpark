# -*- coding: utf-8 -*-
"""Modified PyPageRank with unique output file to GCS"""

# Importations
from operator import add
import time
import re
import sys
import datetime
from datetime import datetime
from pyspark.sql import SparkSession

# Création de la session Spark
spark = SparkSession.builder\
        .appName("PyPageRank")\
        .getOrCreate()

# Use a directory for output
output_path = "gs://benchmark_output/pagerank_results"

# Ajout d'un horodatage pour un nom de fichier unique
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
output_file = f"{output_path}/pagerank_result_{timestamp}.txt"

# Chargement des données
lines = spark.read.text("gs://public_lddm_data/small_page_links.nt").rdd.map(lambda r: r[0])

# Fonctions pour le calcul du PageRank
def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

# Initialisation des liens et des rangs
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

# Exécution du calcul du PageRank
max_iterations = 10
debut = time.time()

for iteration in range(max_iterations):
    contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]
        ))
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

result = ranks.collect()
fin = time.time()

link_with_highest_rank = max(result, key=lambda x: x[1])

# Enregistrement des résultats dans un format de chaîne
output_data = (
    f"Temps d'exécution : {fin-debut} secondes\n"
    f"Link with the highest PageRank: {link_with_highest_rank[0]}, PageRank: {link_with_highest_rank[1]}\n"
)

# Définir un nom de fichier unique basé sur l'heure actuelle
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
output_file = f"gs://benchmark_output/pagerank_results/pagerank_result_{timestamp}.txt"

# Sauvegarder les résultats directement dans GCS
sc = spark.sparkContext
sc.parallelize([output_data]).saveAsTextFile(output_file)

# Arrêter le SparkContext
spark.stop()
