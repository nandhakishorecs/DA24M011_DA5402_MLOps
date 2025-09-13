from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Read the CSV file
# Replace 'input.csv' with your actual CSV file path
df = spark.read.csv("/Users/nandhakishorecs/Documents/IITM/Jan_2025/DA5402/submissions/assignment-09-DA24M011-Nandhakishore/Reviews.csv", header=True, inferSchema=True)

# Add sentiment column based on Score
df_with_sentiment = df.withColumn("sentiment", 
    when(col("Score") >= 3, "positive").otherwise("negative"))

# Write the output to a single CSV file
# Replace 'output.csv' with your desired output file path
df_with_sentiment.coalesce(1).write.csv("output.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()

