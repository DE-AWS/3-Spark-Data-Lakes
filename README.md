# 3-Spark-Data-Lakes
1. [Exercise mapreduce](#schema1)
2. [Map & Lambda](#schema2)
3. [Reading & Writing Data](#schema3)
4. [Data Wranglign](#schema4)

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

<hr>
<a name='schema3'></a>

## 3. Reading & Writing Data

```
spark.sparkContext.getConf().getAll()
```

El código spark.sparkContext.getConf().getAll() en PySpark se utiliza para obtener todas las configuraciones asociadas 
con el contexto de Spark. Aquí hay una explicación detallada de cada parte:

- spark: Es una instancia de SparkSession, que es la entrada principal para interactuar con Spark en el contexto de SQL 
y DataFrame.

- sparkContext: Es el contexto de Spark asociado con la instancia de SparkSession. El SparkContext es responsable de 
la conexión con el clúster de Spark y de la coordinación de las operaciones.

- getConf(): Es un método del SparkContext que devuelve un objeto SparkConf. SparkConf es la clase que contiene 
la configuración de Spark, incluyendo las configuraciones del clúster, la aplicación, y otras opciones de configuración.

- getAll(): Es un método del objeto SparkConf que devuelve todas las configuraciones presentes en ese objeto 
como una lista de pares clave-valor.

En resumen, spark.sparkContext.getConf().getAll() te proporcionará una lista de todas las configuraciones actualmente 
configuradas para tu aplicación Spark, lo cual puede ser útil para verificar la configuración en tiempo de ejecución. 
Esto puede incluir configuraciones predeterminadas, así como configuraciones que hayas establecido explícitamente en 
tu código. Puedes imprimir o analizar esta lista para obtener información detallada sobre la configuración de Spark en 
tu aplicación.


Save it into a different format, for example, into a CSV file, with the write.save() method
```
df.write.mode("overwrite").save("../data/output.csv", format="csv", header=True)
```

Use the read.csv method
```
df_csv = spark.read.csv("../data/output.csv",header = True, inferSchema = True)
```
<hr>
<a name='schema4'></a>

## 4. Data Wranglign

data_wrangling.py
