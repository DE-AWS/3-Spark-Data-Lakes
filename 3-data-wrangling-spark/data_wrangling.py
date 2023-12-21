# Take care of any imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# Create the Spark Context

spark = SparkSession.builder.appName("Our first Python Spark SQL example").getOrCreate()


# Complete the script

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
path = "../data/sparkify_log_small.json"


# # Data Exploration

df = spark.read.json(path)
# 
# # Explore the data set.

# View 5 records

print(df.take(5))

# Print the schema
df.printSchema()


# Describe the dataframe
df.describe().show()

# Describe the statistics for the song length column

df.describe('length').show()

# Count the rows in the dataframe

print(
    df.count()
)
   

# Select the page column, drop the duplicates, and sort by page

df.select('page').dropDuplicates().sort("page").show()


# Select data for all pages where userId is 1046
df.select(['page']).where(df.userId == 1046).show()

df.select(["userId", "firstname", "page", "song"]).where(df.userId == 1046).show()


# # Calculate Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

df = df.withColumn("hour", get_hour(df.ts))

print(
    # Get the first row
    df.head(1)
)


# Select just the NextSong page
songs_in_hour_df = df.filter(df.page == "NextSong") \
    .groupby(df.hour) \
    .count() \
    .orderBy(df.hour.cast("float"))

songs_in_hour_df.show()


songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()


# # Drop Rows with Missing Values

# How many are there now that we dropped rows with null userId or sessionId?

user_log_valid_df = df.dropna(how = "any", subset = ["userId", "sessionId"])
print(user_log_valid_df.count())

# select all unique user ids into a dataframe

df.select("userId") \
    .dropDuplicates() \
    .sort("userId").show()


# Select only data for where the userId column isn't an empty string (different from null)

user_log_valid_df.filter(user_log_valid_df['userId'] != "").show()
# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 

df.filter("page = 'Submit Downgrade'").show()

userId_filtered = df.filter("page = 'Submit Downgrade'").select('userId')
userId_filtered.show()
user_id_value = userId_filtered.first()["userId"]

df.select(["userId", "firstname", "page", "level", "song"]) \
    .where(df.userId == user_id_value) \
    .show()


# Create a user defined function to return a 1 if the record contains a downgrade
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# Select data including the user defined function
user_log_valid_df = user_log_valid_df.withColumn("downgraded", flag_downgrade_event("page"))

print(
    user_log_valid_df.head()
)


from pyspark.sql import Window

# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
windowval = Window.partitionBy("userId") \
    .orderBy(desc("ts")) \
    .rangeBetween(Window.unboundedPreceding, 0)

# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
user_log_valid_df = user_log_valid_df \
    .withColumn("phase", Fsum("downgraded").over(windowval))

user_log_valid_df.show()

# Show the phases for user 1138
user_log_valid_df \
    .select(["userId", "firstname", "ts", "page", "level", "phase"]) \
    .where(df.userId == "1138") \
    .sort("ts") \
    .show()
