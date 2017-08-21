#!/usr/bin/env python3

"""
Apache Spark program that uses a SQL query as data source to feed into spark.

Intended to be run using spark-submit [this py file].
Note that it's possible that adding the JDBC driver path is needed, for
instance:
  --driver-class-path [path to]/postgresql-42.1.4.jar
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Basic JDBC reader") \
    .getOrCreate()

df = spark \
    .read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/jesquivel") \
    .option("user", "test") \
    .option("password", "test") \
    .option("dbtable", "transactions") \
    .load()

df.printSchema()

sumByCustomer = df.groupBy("customer_id").sum()
sumByCustomer.show()
