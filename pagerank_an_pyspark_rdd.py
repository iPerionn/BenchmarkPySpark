
# This code is for running on Google Cloud Platform

from pyspark.sql import SparkSession
from operator import add
import time
import sys
from datetime import datetime

spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

# Use a directory for output
output_path = "gs://benchmark_output/pagerank_results"

iterations = 10

debut1 = time.time()

# Chargement des données
lines = spark.read.text("gs://public_lddm_data/page_links_en.nt.bz2").rdd.map(lambda r: r[0]).cache()
#lines = spark.read.text("gs://public_lddm_data/small_page_links.nt").rdd.map(lambda r: r[0]).cache()
lines.count()

import re
def computeContribs(urls, rank) :
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls) :
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

# Loads all URLs from input file and initialize their neighbors.
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

links.map(lambda x: (x[0],list(x[1])))

links.map(lambda x: (x[0],len(list(x[1])))).sortBy(lambda x:x[1],ascending=False)

links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

damping_factor = 0.85

for iteration in range(iterations):
  # Calculates URL contributions to the rank of other URLs.
  contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

  # Re-calculates URL ranks based on neighbor contributions.
  # OLD ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
  ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * damping_factor + (1 - damping_factor))

ranks.collect()

fin1 = time.time()
# Find the page with the maximum rank
highest_rank_page = ranks.max(key=lambda x: x[1])

# Enregistrement des résultats dans un format de chaîne
output_data = (
    f"Temps d'exécution : {fin1-debut1} secondes\n"
    f"Link with the highest PageRank: {highest_rank_page[0]}, PageRank: {highest_rank_page[1]}\n"
)

# Generate a unique timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the output path using the timestamp
output_file = f"gs://benchmark_output/pagerank_results/RDD_Quad_output_folder_{timestamp}"

# Sauvegarder les résultats directement dans GCS
sc = spark.sparkContext
sc.parallelize([output_data]).saveAsTextFile(output_file)

for i in range(2):
    start_time = time.time()  # Start the timer for each iteration
    
    #exec here
    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    links.map(lambda x: (x[0],list(x[1])))

    links.map(lambda x: (x[0],len(list(x[1])))).sortBy(lambda x:x[1],ascending=False)

    links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
                url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
            ))

    for iteration in range(iterations):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
                    url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
                ))

        # Re-calculates URL ranks based on neighbor contributions.
        # OLD ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * damping_factor + (1 - damping_factor))

    ranks.collect()

    end_time = time.time()
    # Find the page with the maximum rank
    highest_rank_page = ranks.max(key=lambda x: x[1])

    # Enregistrement des résultats dans un format de chaîne
    output_data = (
        f"Temps d'exécution : {end_time-start_time} secondes\n"
        f"Link with the highest PageRank: {highest_rank_page[0]}, PageRank: {highest_rank_page[1]}\n"
    )
    # Generate a unique timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Define the output path using the timestamp
    output_file = f"gs://benchmark_output/pagerank_results/RDD_Quad_output_folder_{timestamp}"

    # Sauvegarder les résultats directement dans GCS
    sc = spark.sparkContext
    sc.parallelize([output_data]).saveAsTextFile(output_file)

# Arrêter le SparkContext
spark.stop()