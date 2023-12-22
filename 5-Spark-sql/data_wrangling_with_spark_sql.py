#!/usr/bin/env python
# coding: utf-8

# # Data Wrangling with Spark SQL Quiz
# 
# This code uses the same dataset and most of the same questions from the earlier code using dataframes.
# For this script, however, use Spark SQL instead of Spark Data Frames.


from pyspark.sql import SparkSession

# TODOS: 
# 1) import any other libraries you might need
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 2) instantiate a Spark session
spark = SparkSession.builder.appName("Spark Sql").getOrCreate()

# 3) read in the data set located at the path "data/sparkify_log_small.json"
df = spark.read.json("../data/sparkify_log_small.json")


df.printSchema()
print(df.describe())
df.show(2)


# 4) create a view to use with your SQL queries

df.createOrReplaceTempView("user_log_table")

spark.sql("SELECT * from user_log_table LIMIT 2").show()

# 5) write code to answer the quiz questions 


# # Question 1
# 
# Which page did user id ""(empty string) NOT visit?

# TODO: write your code to answer question 1

spark.sql("""
    SELECT distinct page 
    FROM user_log_table
    WHERE userId <>''
""").show()


# # Question 2 - Reflect
# 
# Why might you prefer to use SQL over data frames? Why might you prefer data frames over SQL?

# # Question 3
# 
# How many female users do we have in the data set?

# TODO: write your code to answer question 3
print(spark.sql(
    """
        SELECT distinct (userId)
        FROM user_log_table
        WHERE gender == 'F'
        
    """
).count())

# # Question 4
#
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

spark.udf.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x/1000.0).hour))

spark.sql(
    """
    SELECT *, get_hour(ts) AS hour
    FROM user_log_table
    LIMIT 1
    """
).collect()


spark.sql(
    """
        SELECT get_hour(ts) as hour, count(*) as plays_per_hour
        FROM user_log_table
        WHERE page = 'NextSong'
        GROUP BY hour
        ORDER BY cast(hour as int) ASC
    """
).show()


# Converting Results to Pandas

songs_in_hour = spark.sql(
    """
        SELECT get_hour(ts) as hour, count(*) as plays_per_hour
        FROM user_log_table
        WHERE page = 'NextSong'
        GROUP BY hour
        ORDER BY cast(hour as int) ASC
    """
)

songs_in_hour_pd = songs_in_hour.toPandas()
print(songs_in_hour_pd)