# # Data Wrangling with DataFrames Coding Quiz

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, col

# TODOS: 
# 1) import any other libraries you might need

# 2) instantiate a Spark session

spark = SparkSession.builder.appName("Quiz Data Wrangling").getOrCreate()
# 3) read in the data set located at the path "../data/sparkify_log_small.json"

df = spark.read.json("../data/sparkify_log_small.json")
# 4) write code to answer the quiz questions

# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# TODO: write your code to answer question 1


blank_pages_df = df.filter(df['userId'] == "").select('page').alias('black_pages').dropDuplicates()
blank_pages_df.show()


all_pages_df = df.select('page').dropDuplicates()

for row in set(all_pages_df.collect()) - set(blank_pages_df.collect()):
    print(row.page)


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 


# TODO: use this space to explore the behavior of the user with an empty string


# # Question 3
# 
# How many female users do we have in the data set?


# TODO: write your code to answer question 3

print(df.filter(df.gender == 'F').select('userId','gender').dropDuplicates().count())

# # Question 4
# 
# How many songs were played from the most played artist?

# TODO: write your code to answer question 4

df.filter(df.page == 'NextSong').select('artist').groupBy('artist').agg({'artist':'count'}).withColumnRenamed('count(artist)', 'PlayCount').sort(desc('Playcount')).show(1)




# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: write your code to answer question 5

