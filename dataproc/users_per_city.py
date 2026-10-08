"""
Count users per city from gold.users_details, and expose the city skew in the Spark UI.

Skew demo: each user row is copied MULTIPLIER times (same 50% Tel Aviv mix), then every
row is shuffled by city. The partition that receives Tel Aviv gets half of all rows, which
shows up in the Spark UI as one task far larger than the rest.

A plain groupBy("city").count() can't show this: Spark pre-aggregates per task before
the shuffle (partial aggregation), so only a few subtotal rows per city are shuffled.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SOURCE = "deloitte-ha.gold.users_details"
MULTIPLIER = 20000
PARTITIONS = 8  # matches the 8 worker cores, so all tasks run in one wave

spark = SparkSession.builder.appName("users-per-city-skew").getOrCreate()

# Optimizations that would hide or blur the skew in the Spark UI.
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.shuffle.partitions", str(PARTITIONS))

users = spark.read.format("bigquery").option("table", SOURCE).load().select("user_id", "city")

# 1. The answer: users per city.
users.groupBy("city").count().orderBy(F.desc("count")).show()

# 2. The skew demo: rows per partition after shuffling by city.
amplified = spark.range(MULTIPLIER, numPartitions=PARTITIONS).crossJoin(F.broadcast(users))
(
    amplified.repartition(PARTITIONS, "city")
    .groupBy(F.spark_partition_id().alias("partition"))
    .count()
    .orderBy(F.desc("count"))
    .show()
)

spark.stop()
