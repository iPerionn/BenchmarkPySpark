# -*- coding: utf-8 -*-
"""PyPageRank.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/github/momo54/large_scale_data_management/blob/main/PyPageRank.ipynb
"""

!pip install pyspark

!pip install -q findspark
import findspark
findspark.init()

"""# SPARK INSTALLED... lets play"""

from pyspark.sql import SparkSession
spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

!wget -q https://storage.googleapis.com/public_lddm_data/small_page_links.nt
!ls

lines = spark.read.text("small_page_links.nt").rdd.map(lambda r: r[0])
#lines.take(5)

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

#links.take(5)

#groupByKey makes lists !!
#links.map(lambda x: (x[0],list(x[1]))).take(5)

#groupByKey makes lists !!
#links.map(lambda x: (x[0],len(list(x[1])))).sortBy(lambda x:x[1],ascending=False).take(10)

#ranks.take(5)

#links.join(ranks).take(5)

#init
links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        )).take(5)

from operator import add
import time
max_iterations = 10
debut = time.time()

for iteration in range(max_iterations):
  # Calculates URL contributions to the rank of other URLs.
  contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

  # Re-calculates URL ranks based on neighbor contributions.
  ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
result = ranks.collect()
fin = time.time()
print(f"Temps d'exécution : {fin-debut} secondes /")

# Finds the link with the highest PageRank
link_with_highest_rank = max(result, key=lambda x: x[1])

# Prints the link with the highest PageRank and its value
print(f"Link with the highest PageRank: {link_with_highest_rank[0]}, PageRank: {link_with_highest_rank[1]}")