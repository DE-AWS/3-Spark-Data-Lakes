# First let's import SparkConf and SparkSession



from pyspark.sql import SparkSession

# Create SparkSession



# Crea una instancia de SparkSession
spark = SparkSession.builder.appName("Our first Python Spark SQL example").getOrCreate()

# Establece el nivel de registro de SparkSession
spark.sparkContext.setLogLevel("ERROR")

# Let's check if the change went through

spark.sparkContext.getConf().getAll()
"""
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

"""

print(spark.sparkContext.getConf().getAll())
