import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQLServer").getOrCreate()

# Read the CSV file into a pandas DataFrame
df = pd.read_csv("./data/input.csv")

# Randomly shuffle the DataFrame rows
df_shuffled = df.sample(frac=1)

# Select a random row from the shuffled DataFrame
random_row = df_shuffled.sample(n=1)

# Convert the random row to a Spark DataFrame
spark_df = spark.createDataFrame(random_row)

# Register the Spark DataFrame as a temporary table
spark_df.createOrReplaceTempView("random_row")

# Execute the concatenation query against the Spark SQL server
concatenated_df = spark.sql("SELECT * FROM your_table_name UNION SELECT * FROM random_row")

# Output the concatenated DataFrame
concatenated_df.show()

# Stop the Spark session
spark.stop()
