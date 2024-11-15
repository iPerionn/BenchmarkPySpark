from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

import time
import sys
from datetime import datetime

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

schema = StructType([
    StructField("source", StringType(), nullable=True),
    StructField("predicate", StringType(), nullable=True),
    StructField("target", StringType(), nullable=True)
])

# Chargez vos données web en tant que DataFrame
data = spark.read.option("delimiter"," ").csv("gs://public_lddm_data/page_links_en.nt.bz2", header=False, schema=schema)
#data = spark.read.option("delimiter"," ").csv("gs://public_lddm_data/small_page_links.nt", header=False, schema=schema)

# Calculer le nombre de liens sortants par page
outgoing_links = data.groupBy("source").agg(count("target").alias("outDegree"))

# Récupérer toutes les pages distinctes
pages_from_source = data.select("source").distinct()
pages_from_target = data.select("target").distinct()

# Créer un DataFrame avec toutes les pages distinctes
all_pages = pages_from_source.union(pages_from_target).distinct()

# Joindre pour obtenir le nombre de liens sortants, en remplaçant les valeurs nulles par 0
pagerank = all_pages.join(outgoing_links, all_pages.source == outgoing_links.source, "left") \
                  .select(all_pages.source.alias("page"), col("outDegree")) \
                  .fillna(0, subset=["OutDegree"])

# Ajouter une colonne pagerank initialisée à 1
pagerank = pagerank.withColumn("pagerank", lit(1.0).cast(DoubleType()))

iteration = 10

# Facteur de dumping
d = 0.85

debut1 = time.time()

for i in range(iteration):
  links_with_pagerank = data.join(pagerank, data.source == pagerank.page)\
    .select("target", "page", "pagerank", "outDegree")

  # Calculer la contribution de chaque page
  contributions = links_with_pagerank.withColumn("contribution", col("pagerank") / col("OutDegree").cast(DoubleType()))

  # Somme des contributions pour chaque page cible
  rank_updates = contributions.groupBy("target").agg(F.sum("contribution").alias("sum_contributions"))

  # Calculer le nouveau PageRank en utilisant le facteur de dumping
  result = pagerank.join(rank_updates, pagerank.page == rank_updates.target, "left") \
                  .withColumn("sum_contributions", when(col("sum_contributions").isNull(), 0).otherwise(col("sum_contributions")).cast(DoubleType())) \
                  .withColumn("pagerank", ((1 - d) + d * col("sum_contributions")).cast(DoubleType())) \
                  .select("page", "outDegree", "pagerank")

result.collect()

fin1 = time.time()

max_pagerank_page = result.orderBy(col("pagerank"), ascending=False).limit(1).collect()[0]

# Enregistrement des résultats dans un format de chaîne
output_data = (
    f"Temps d'exécution : {fin1-debut1} secondes\n"
    f"Link with the highest PageRank: {max_pagerank_page['page']}, PageRank: {max_pagerank_page['pagerank']}\n"
)

# Generate a unique timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the output path using the timestamp
output_file = f"gs://benchmark_output/pagerank_results/SQL_Singlenode_output_folder_{timestamp}"

# Sauvegarder les résultats directement dans GCS
sc = spark.sparkContext
sc.parallelize([output_data]).saveAsTextFile(output_file)