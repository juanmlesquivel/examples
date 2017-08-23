#!/usr/bin/env python3

"""
Apache Spark program that uses a SQL query as data source to feed into spark.

Intended to be run using spark-submit [this py file].
Note that it's possible that adding the JDBC driver path is needed, for
instance:
  --driver-class-path [path to]/postgresql-42.1.4.jar
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import DateType

spark = SparkSession \
    .builder \
    .appName("Basic JDBC read / aggregate / write pipeline") \
    .getOrCreate()

# Step #1
# Read a DataFrame by getting all rows from the transactions table.
df = spark \
    .read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/jesquivel") \
    .option("user", "test") \
    .option("password", "test") \
    .option("dbtable", "transactions") \
    .load()

# Step 2
# Transform / aggregate data

# The transactions table contains timestamps that are more granular than the
# daily aggregation expected in the output table (at least that's what the
# example is trying to do).
#
# The withColumn method will return a new data frame with one new column.
# In 2 steps we first transform timestamp to string and then to a proper
# spark date object.
string_to_date = \
    udf(lambda text_date: datetime.strptime(text_date,
        '%m/%d/%Y'), DateType())

df = df.withColumn("date_string", date_format(col("purchased_at"), 'MM/dd/yyyy'))
df = df.withColumn("date", string_to_date(df.date_string))

# With data properly formatted, group by customer and aggregate, which is what
# we want to write.

sum_df = df.groupBy("customer_id", "date").sum()
stats_df = sum_df.select(col('customer_id'), col('date'),
        col('sum(amount)').alias('amount'))

# Step 3
# Generate output

# Print the contents of the Spark Data Frame to check everything is fine.
stats_df.printSchema()
stats_df.show()

# Write to the database. It is assumed that the schema of the data frame matches
# the schema of the DB table.
#
# Note on mode('append'): Ideally we want to just update the (customer, date)
# pairs to make the program idempotent. In practice, this allows us to update
# stats when new data flows in or is corrected. However UPDATE/INSERT conceptual
# operations are not supported by Spark. For now we just write assuming that
# there's no primary key collision. A more elaborate program can do something
# like removing data for some date D and then inserting for that date using this
# perogram.
stats_df.write \
    .mode('append') \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/jesquivel") \
    .option("user", "test") \
    .option("password", "test") \
    .option("dbtable", "daily_transaction_stats") \
    .save()
