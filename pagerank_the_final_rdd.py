from pyspark.sql import Row

import time
import sys
from datetime import datetime
from pyspark.sql import SparkSession

output_path = "gs://benchmark_output/pagerank_results"

# Créez une session Spark
spark = SparkSession.builder\
    .appName("MonAppSpark")\
    .master("local[*]")\
    .config("spark.executor.memory", "12g")\
    .config("spark.executor.cores", "4")\
    .config("spark.driver.memory", "12g")\
    .config("spark.driver.cores", "1")\
    .config("spark.local.dir", "/tmp/spark-tmp")\
    .getOrCreate()

# Charger le fichier en tant que RDD
rdd_data = spark.sparkContext.textFile("gs://public_lddm_data/page_links_en.nt.bz2")
#rdd_data = spark.sparkContext.textFile("gs://public_lddm_data/small_page_links.nt")

# Transformer chaque ligne pour extraire les colonnes source, predicate, et target
rdd_data = rdd_data.map(lambda line: line.split(" ")).map(lambda parts: (parts[0], parts[1], parts[2]))

rdd_data = rdd_data.map(lambda parts: (parts[0][1:-1], parts[1][1:-1], parts[2][1:-1]))

# Étape 1 : Calculer le nombre de liens sortants par page
outgoing_links = rdd_data.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)

# Étape 2 : Récupérer toutes les pages distinctes à partir des sources et des cibles
pages_from_source = rdd_data.map(lambda x: x[0]).distinct()
pages_from_target = rdd_data.map(lambda x: x[2]).distinct()

# Étape 3 : Union des pages distinctes pour obtenir la liste complète de toutes les pages
all_pages = pages_from_source.union(pages_from_target).distinct()

# Étape 4 : Joindre avec outgoing_links pour obtenir le nombre de liens sortants par page
pagerank_rdd = all_pages.map(lambda page: (page, 0)).leftOuterJoin(outgoing_links) \
    .map(lambda x: (x[0], x[1][1] if x[1][1] is not None else 0))

# Étape 5 : Ajouter une valeur initiale de PageRank (1.0) pour chaque page
pagerank_rdd = pagerank_rdd.map(lambda x: Row(page=x[0], outDegree=x[1], pagerank=float(1.0)))

# Convertir en DataFrame si nécessaire pour afficher le résultat
pagerank = spark.createDataFrame(pagerank_rdd)

# Facteur de dumping et nombre d'itérations
d = 0.85
iterations = 10

# Convertir le DataFrame pagerank en RDD (page, (outDegree, pagerank))
pagerank_rdd = pagerank.rdd.map(lambda row: (row.page, (row.outDegree, row.pagerank)))

debut1 = time.time()

# Boucle d'itérations
for i in range(iterations):
  # Joindre data (liens entre pages) avec pagerank_rdd pour avoir les informations de chaque lien avec son pagerank
  links_with_pagerank = rdd_data.map(lambda x: (x[0], x[2])) \
                                .join(pagerank_rdd) \
                                .map(lambda x: (x[1][0], x[1][1][1] / x[1][1][0] if x[1][1][0] > 0 else 0))

  # links_with_pagerank contient maintenant (target, contribution) pour chaque lien

  # Calculer la somme des contributions pour chaque target
  contributions = links_with_pagerank.reduceByKey(lambda x, y: x + y)

  # Joindre avec toutes les pages pour inclure les pages sans liens entrants
  # pagerank_rdd = all_pages.map(lambda page: (page, 0)).leftOuterJoin(contributions) \
  #                         .mapValues(lambda x: x[1] if x[1] is not None else x[0])
  pagerank_rdd = all_pages.map(lambda page: (page, 0)) \
                        .leftOuterJoin(contributions) \
                        .mapValues(lambda x: x[1] if x[1] is not None else 1)

  # Calculer le nouveau pagerank pour chaque page en appliquant la formule de PageRank
  pagerank_rdd = pagerank_rdd.mapValues(lambda contrib: max(min((1 - d) + d * contrib, float('inf')), 0))

  # Ajouter de nouveau le outDegree et préparer pour la prochaine itération
  pagerank_rdd = pagerank_rdd.leftOuterJoin(outgoing_links) \
                               .mapValues(lambda x: (x[1] if x[1] is not None else 0, x[0]))

pagerank_rdd.collect()

fin1 = time.time()
# Trouver la page avec le PageRank le plus élevé
highest_pagerank = pagerank_rdd.reduce(lambda a, b: a if a[1][1] > b[1][1] else b)

# Afficher le résultat
print(f"La page avec le plus haut PageRank est : {highest_pagerank[0]} avec un score de PageRank de {highest_pagerank[1][1]}")

# Enregistrement des résultats dans un format de chaîne
output_data = (
    f"Temps d'exécution : {fin1-debut1} secondes\n"
    f"Link with the highest PageRank: {highest_pagerank[0]}, PageRank: {highest_pagerank[1][1]}\n"
)

# Generate a unique timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the output path using the timestamp
output_file = f"gs://benchmark_output/pagerank_results/RDD_Singlenode_output_folder_{timestamp}"

# Sauvegarder les résultats directement dans GCS
sc = spark.sparkContext
sc.parallelize([output_data]).saveAsTextFile(output_file)
