# First let's import SparkConf and SparkSession



from pyspark.sql import SparkSession

# Create SparkSession


# Crea una instancia de SparkSession
spark = SparkSession.builder.appName("Our first Python Spark SQL example").getOrCreate()

# Establece el nivel de registro de SparkSession
spark.sparkContext.setLogLevel("ERROR")

# Let's check if the change went through

print(
    spark.sparkContext.getConf().getAll()
)

# Load a JSON file into a Spark DataFrame called user_log.
df = spark.read.json("../data/sparkify_log_small.json")
df.show(2)

# Print the schema with the printSchema() method.
df.printSchema()

# Try the describe() method to see what we can learn from our data.

print(df.describe())



# Use the take() method to grab the first few records.

print(df.take(5))

# Save it into a different format, for example, into a CSV file, with the write.save() method
df.write.mode("overwrite").save("../data/output.csv", format="csv", header=True)

# Use the read.csv method
df_csv = spark.read.csv("../data/output.csv",header = True, inferSchema = True)
df_csv.printSchema()
print(df_csv.take(1))


# Show the userID column for the first several rows
df_csv.select("userID").show()