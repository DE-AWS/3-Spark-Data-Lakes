# 3-Spark-Data-Lakes
1. [Exercise mapreduce](#schema1)
2. [Map & Lambda](#schema2)


<hr>
<a name='schema1'></a>

## 1. Exercise mapreduce
mapreduce_practice.ipynb

<hr>
<a name='schema2'></a>

## 2. Map & Lambda
```
def convert_song_to_lowercase(song):
    return song.lower()
``` 

Use the map function to transform the list of songs with the python function that converts strings to lowercase
```
song_lower = song_log_rdd.map(convert_song_to_lowercase)
```
Use lambda functions instead of named functions to do the same map operation
```
lower_song_lambda = song_log_rdd.map(lambda song: song.lower())
```
rdd_song_lower_case.py