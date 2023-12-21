### 
# You might have noticed this code in the screencast.
#
# import findspark
# findspark.init('spark-2.3.2-bin-hadoop2.7')
#
# The findspark Python module makes it easier to install
# Spark in local mode on your computer. This is convenient
# for practicing Spark syntax locally. 
# However, the workspaces already have Spark installed and you do not
# need to use the findspark module
#
###



import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('lower_case').getOrCreate()



# Starting off with a regular python list
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]


# parallelize the log_of_songs to use with Spark
song_log_rdd = spark.sparkContext.parallelize(log_of_songs)

# show the original input data is preserved
song_log_rdd.foreach(print)

# create a python function to convert strings to lowercase
def convert_song_to_lowercase(song):
    return song.lower()


# use the map function to transform the list of songs with the python function that converts strings to lowercase

song_lower = song_log_rdd.map(convert_song_to_lowercase)
song_lower.foreach(print)
# Show the original input data is still mixed case


# Use lambda functions instead of named functions to do the same map operation
lower_song_lambda = song_log_rdd.map(lambda song: song.lower())
lower_song_lambda.foreach(print)