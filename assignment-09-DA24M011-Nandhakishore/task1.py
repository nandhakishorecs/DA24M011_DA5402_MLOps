from pyspark.sql import SparkSession                                # type: ignore
from pyspark.sql.functions import col                               # type: ignore
from pyspark.sql.types import StructType, StructField, StringType   # type: ignore
from transformers import pipeline                   
from datasets import Dataset                                        # type: ignore
import pandas as pd
import re
import numpy as np
import logging
from tqdm import tqdm  # Import tqdm                                # type: ignore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('task1.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Spark session with 6 GB RAM and 6 CPU cores
logger.info("Initializing Spark session")
try:
    spark = SparkSession.builder.appName("GourmetFoodsParquet").config("spark.executor.memory", "5g").config("spark.driver.memory", "1g").config("spark.executor.cores", "6").config("spark.executor.instances", "1").config("spark.sql.shuffle.partitions", "12").getOrCreate()
    spark.conf.set("spark.default.parallelism", spark.sparkContext.defaultParallelism)
except Exception as e:
    logger.error(f"Failed to initialize Spark session: {str(e)}")
    raise

# Initialize sentiment analysis pipeline
logger.info("Initializing sentiment analysis pipeline")
try:
    sentiment_pipeline = pipeline("sentiment-analysis", device=0)
except Exception as e:
    logger.error(f"Failed to initialize sentiment pipeline: {str(e)}")
    raise

# Function to parse the text file
def parse_text_file(lines):
    records = []
    current_record = {}
    # Convert lines to list to get length for tqdm
    lines = list(lines)
    logger.debug(f"Parsing {len(lines)} lines")
    for line in tqdm(lines, desc="Parsing text file"):
        line = line.strip()
        if not line:
            if current_record:
                records.append(current_record)
                current_record = {}
            continue
        match = re.match(r"(\w+)/(\w+): (.*)", line)
        if match:
            category, key, value = match.groups()
            if category == "product":
                current_record[f"product_{key}"] = value
            elif category == "review":
                current_record[f"review_{key}"] = value
    if current_record:
        records.append(current_record)
    return records

# Read text file as Spark RDD
logger.info("Reading Gourmet_Foods.txt")
try:
    text_rdd = spark.sparkContext.textFile("Gourmet_Foods.txt")
except Exception as e:
    logger.error(f"Failed to read input file: {str(e)}")
    spark.stop()
    raise

# Parse the text file in parallel
logger.debug("Parsing text file")
parsed_rdd = text_rdd.mapPartitions(parse_text_file)

# Define schema for DataFrame
schema = StructType([
    StructField("product_productId", StringType(), True),
    StructField("product_title", StringType(), True),
    StructField("product_price", StringType(), True),
    StructField("review_userId", StringType(), True),
    StructField("review_profileName", StringType(), True),
    StructField("review_helpfulness", StringType(), True),
    StructField("review_score", StringType(), True),
    StructField("review_time", StringType(), True),
    StructField("review_summary", StringType(), True),
    StructField("review_text", StringType(), True)
])

# Convert RDD to DataFrame
logger.info("Converting RDD to DataFrame")
try:
    df = spark.createDataFrame(parsed_rdd, schema)
except Exception as e:
    logger.error(f"Failed to create DataFrame: {str(e)}")
    spark.stop()
    raise

# Extract review_text for sentiment analysis
logger.debug("Extracting review_text for sentiment analysis")
review_texts = df.select("review_text").na.fill({"review_text": ""}).collect()
texts = [row["review_text"] for row in tqdm(review_texts, desc="Collecting review texts")]

# Convert to Dataset for batched processing
logger.info("Converting texts to Dataset")
dataset = Dataset.from_dict({"text": texts})

# Batch process sentiment analysis
def process_batch(batch):
    results = sentiment_pipeline(batch["text"], truncation=True, max_length=512, batch_size=16)
    return {"predicted_sentiment": [result["label"] if result else "NEUTRAL" for result in results]}

# Apply sentiment analysis in batches with tqdm
logger.info("Performing sentiment analysis")
try:
    # Use tqdm to track progress of dataset mapping
    results = dataset.map(
        process_batch,
        batched=True,
        batch_size=16,
        desc="Sentiment analysis"
    )
except Exception as e:
    logger.error(f"Failed during sentiment analysis: {str(e)}")
    spark.stop()
    raise

# Convert results to DataFrame
logger.debug("Converting sentiment results to DataFrame")
sentiment_df = spark.createDataFrame(
    pd.DataFrame({"predicted_sentiment": results["predicted_sentiment"]})
)

# Add row index to join with original DataFrame
logger.debug("Joining sentiment results with original DataFrame")
df_with_index = df.rdd.zipWithIndex().map(lambda x: x[0].asDict() | {"index": x[1]}).toDF()
sentiment_df_with_index = sentiment_df.rdd.zipWithIndex().map(lambda x: x[0].asDict() | {"index": x[1]}).toDF()

df_with_sentiment = df_with_index.join(
    sentiment_df_with_index.select("index", "predicted_sentiment"),
    "index"
).drop("index")

# Write to Parquet
logger.info("Writing to Parquet file")
try:
    df_with_sentiment.write.mode("overwrite").parquet("gourmet_foods.parquet")
except Exception as e:
    logger.error(f"Failed to write Parquet file: {str(e)}")
    spark.stop()
    raise

# Print first 5 rows to verify
logger.info("Displaying first 5 rows")
df_with_sentiment.show(5, truncate=False)

# Stop Spark session
logger.info("Stopping Spark session")
spark.stop()