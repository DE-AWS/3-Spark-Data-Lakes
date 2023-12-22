# 3-Spark-Data-Lakes
1. [Exercise mapreduce](#schema1)
2. [Map & Lambda](#schema2)
3. [Reading & Writing Data](#schema3)
4. [Data Wranglign](#schema4)
5. [Quiz Data Wrangling with DataFrames](#schema5)
6. [Data Wrangling with DataFrames Extra Tips](#schema6)
7. [Spark SQL](#schema7)

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

<hr>
<a name='schema5'></a>

## 5. Quiz Data Wrangling with DataFrames

quiz_data_wrangling_with_dataframes.py

```
df.filter(df.page == 'NextSong').select('artist').groupBy('artists').agg({'artist':'count'})\
.withColumnRenamed('count(artist)', 'PlayCount').sort(desc('Playcount')).show(1)
```

- agg
Ejemplo de agg `df.agg({'Sales':'sum'}).show()`, está en el formato de agregación mediante 
un diccionario donde las claves son los nombres de las columnas y los valores son las funciones de agregación 
que deseas aplicar a esas columnas.

Sin embargo, es importante mencionar que la sintaxis de agg con un diccionario en PySpark requiere que uses 
la función `pyspark.sql.functions`  para definir las operaciones de agregación.

Recuerda que el resultado de agg es un DataFrame con una sola fila, por lo que puedes usar `show()` 
para imprimir los resultados



<hr>
<a name='schema6'></a>

## 6. Data Wrangling with DataFrames Extra Tips

Extra Tips for Working With PySpark DataFrame Functions
In the previous video, we've used a number of functions to manipulate our dataframe. Let's take a look at the 
different type of functions and their potential pitfalls.

**General functions**
We have used the following general functions that are quite similar to methods of pandas dataframes:


- `select()`: returns a new DataFrame with the selected columns
- `filter()`: filters rows using the given condition
- `where()`: is just an alias for filter()
- `groupBy()`: groups the DataFrame using the specified columns, so we can run aggregation on them
- `sort()`: returns a new DataFrame sorted by the specified column(s). By default the second parameter 'ascending' 
is True.
- `dropDuplicates()`: returns a new DataFrame with unique rows based on all or just a subset of columns
- `withColumn()`: returns a new DataFrame by adding a column or replacing the existing column that has the same name. 
The first parameter is the name of the new column, the second is an expression of how to compute it.

**Aggregate functions**
Spark SQL provides built-in methods for the most common aggregations such as `count()`, `countDistinct()`, 
`avg()`, `max()`, `min()`, etc. in the `pyspark.sql.functions module`. These methods are not the same as the 
built-in methods in the Python Standard Library, where we can find min() for example as well, hence you need to be 
careful not to use them interchangeably.

In many cases, there are multiple ways to express the same aggregations. For example, if we would like to compute 
one type of aggregate for one or more columns of the DataFrame we can just simply chain the aggregate method after 
a `groupBy()`. If we would like to use different functions on different columns, `agg()`comes in handy. 
For example `agg({"salary": "avg", "age": "max"})` computes the average salary and maximum age.

**User defined functions (UDF)**
In Spark SQL we can define our own functions with the udf method from the pyspark.sql.functions module. 
The default type of the returned variable for UDFs is string. If we would like to return an other type we need to 
explicitly do so by using the different types from the pyspark.sql.types module.

**Window functions**
Window functions are a way of combining the values of ranges of rows in a DataFrame. When defining the window we 
can choose how to sort and group (with the partitionBy method) the rows and how wide of a window we'd like to use 
(described by `rangeBetween` or `rowsBetween`).



<hr>
<a name='schema7'></a>

## 7. Spark SQL


- Create a view to use with your SQL queries

```
df.createOrReplaceTempView("user_log_table")
```

- First query
```
spark.sql("SELECT * from user_log_table LIMIT 2").show()
```

```

+-------------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-------------+---------+--------------------+------+-------------+--------------------+------+
|       artist|     auth|firstName|gender|itemInSession|lastName|   length|level|            location|method|    page| registration|sessionId|                song|status|           ts|           userAgent|userId|
+-------------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-------------+---------+--------------------+------+-------------+--------------------+------+
|Showaddywaddy|Logged In|  Kenneth|     M|          112|Matthews|232.93342| paid|Charlotte-Concord...|   PUT|NextSong|1509380319284|     5132|Christmas Tears W...|   200|1513720872284|"Mozilla/5.0 (Win...|  1046|
|   Lily Allen|Logged In|Elizabeth|     F|            7|   Chase|195.23873| free|Shreveport-Bossie...|   PUT|NextSong|1512718541284|     5027|       Cheryl Tweedy|   200|1513720878284|"Mozilla/5.0 (Win...|  1000|
+-------------+---------+---------+------+-------------+--------+---------+-----+--------------------+------+--------+-------------+---------+--------------------+------+-------------+--------------------+------+

```




