
# This code is for running on Google Cloud Platform

from pyspark.sql import SparkSession
from operator import add
import time

spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

# Use a directory for output
output_path = "gs://benchmark_output/pagerank_results"

# Chargement des données
#lines = spark.read.text("gs://public_lddm_data/page_links_en.nt.bz2").rdd.map(lambda r: r[0])
lines = spark.read.text("gs://public_lddm_data/small_page_links.nt").rdd.map(lambda r: r[0])

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
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

links.map(lambda x: (x[0],list(x[1]))).take(5)

links.map(lambda x: (x[0],len(list(x[1])))).sortBy(lambda x:x[1],ascending=False).take(10)

ranks.take(5)

links.join(ranks).take(5)

links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        )).take(5)

damping_factor = 0.85
max_iterations = 10
debut = time.time()

for iteration in range(10):
  # Calculates URL contributions to the rank of other URLs.
  contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

  # Re-calculates URL ranks based on neighbor contributions.
  # OLD ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
  ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * damping_factor + (1 - damping_factor))

fin = time.time()
# Find the page with the maximum rank
highest_rank_page = ranks.max(key=lambda x: x[1])

# Enregistrement des résultats dans un format de chaîne
output_data = (
    f"Temps d'exécution : {fin-debut} secondes\n"
    f"Link with the highest PageRank: {highest_rank_page[0]}, PageRank: {highest_rank_page[1]}\n"
)

def generate_unique_filename():
    unique_id = uuid.uuid4().hex
    return f"result_{unique_id}.txt"

# Exemple d'utilisation
file_name = generate_unique_filename()

# Définir un nom de fichier unique basé sur l'heure actuelle
output_file = f"gs://benchmark_output/pagerank_results/{file_name}"

# Sauvegarder les résultats directement dans GCS
sc = spark.sparkContext
sc.parallelize([output_data]).saveAsTextFile(output_file)

# Arrêter le SparkContext
spark.stop()