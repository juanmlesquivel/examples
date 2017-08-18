#!/usr/bin/env python3

"""
Apache Spark program that uses a SQL query as data source to feed into spark.

Intended to be run using spark-submit [this py file].
"""

from pyspark.sql import SparkSession

logFile = "/usr/local/spark/README.md"
spark = SparkSession.builder.appName("BasicSQL").master("local").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
