from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, substring, when, sum as sum_
from pyspark.sql.types import StringType
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import time
from threading import Thread
from uuid import uuid4
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark session with adjusted resource settings
spark = SparkSession.builder \
    .appName("SentimentAnalysisMapReduce") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

# Load the sentiment analysis pipeline with a lighter model
try:
    from transformers import pipeline
    sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=-1)
    logger.info("Sentiment analysis pipeline loaded successfully")
except Exception as e:
    logger.error(f"Failed to load sentiment analysis pipeline: {e}")
    raise

# Define a Pandas UDF for sentiment analysis with progress bar
@pandas_udf(StringType())
def predict_sentiment(reviews: pd.Series) -> pd.Series:
    """
    Apply sentiment analysis to a series of reviews in batches with a progress bar.
    Returns 'POSITIVE' or 'NEGATIVE' based on the model's prediction.
    """
    results = []
    batch_size = 32
    total_reviews = len(reviews)
    num_batches = (total_reviews + batch_size - 1) // batch_size
    logger.info(f"Processing {total_reviews} reviews in {num_batches} batches")
    
    with tqdm(total=total_reviews, desc="Sentiment Analysis UDF", unit="reviews") as pbar:
        for i in range(0, total_reviews, batch_size):
            batch_reviews = reviews[i:i + batch_size].tolist()
            batch_reviews = [str(r)[:512] if r is not None and not pd.isna(r) else "" for r in batch_reviews]
            try:
                batch_results = sentiment_analyzer(batch_reviews)
                batch_labels = ['POSITIVE' if r['label'] == 'POSITIVE' else 'NEGATIVE' for r in batch_results]
                results.extend(batch_labels)
            except Exception as e:
                logger.error(f"Error processing batch {i // batch_size + 1}: {e}")
                results.extend(['NEGATIVE'] * len(batch_reviews))
            pbar.update(len(batch_reviews))
    
    return pd.Series(results)

# Read the CSV file with progress indication
try:
    logger.info("Starting CSV loading")
    start_time = time.time()
    with tqdm(total=1, desc="Loading CSV", unit="file") as pbar:
        df = spark.read.csv("/Users/nandhakishorecs/Documents/IITM/Jan_2025/DA5402/submissions/assignment-09-DA24M011-Nandhakishore/Reviews.csv", header=True, inferSchema=True)
        pbar.update(1)
    logger.info(f"Input CSV loaded in {time.time() - start_time:.2f} seconds")
except Exception as e:
    logger.error(f"Failed to load input CSV: {e}")
    raise

# Log dataset size and partition count
row_count = df.count()
partition_count = df.rdd.getNumPartitions()
logger.info(f"Dataset row count: {row_count}")
logger.info(f"Number of partitions: {partition_count}")

# Sample the dataset for testing (1% of data)
with tqdm(total=1, desc="Sampling Dataset", unit="operation") as pbar:
    df = df.sample(fraction=0.01, seed=42)
    # Alternatively, limit to 1000 rows for debugging
    # df = df.limit(1000)
    pbar.update(1)
logger.info("Sampled dataset to 1% of original size")

# Print schema to verify column names
logger.info("Dataset schema:")
df.printSchema()

# Ensure the ground truth 'sentiment' column exists
with tqdm(total=1, desc="Creating Sentiment Column", unit="operation") as pbar:
    df = df.withColumn("sentiment", when(col("Score") >= 3, "POSITIVE").otherwise("NEGATIVE"))
    pbar.update(1)

# Truncate the text column to 512 characters
text_column = "Text"
if text_column not in df.columns:
    logger.error(f"Column '{text_column}' not found in dataset. Available columns: {df.columns}")
    raise ValueError(f"Column '{text_column}' not found")
with tqdm(total=1, desc="Truncating Text Column", unit="operation") as pbar:
    df = df.withColumn("truncated_text", substring(col(text_column), 1, 512))
    pbar.update(1)

# Check for nulls in text column
with tqdm(total=1, desc="Checking Nulls in Text", unit="operation") as pbar:
    null_text_count = df.filter(col(text_column).isNull()).count()
    pbar.update(1)
if null_text_count > 0:
    logger.warning(f"Found {null_text_count} rows with null values in '{text_column}'. These will be treated as NEGATIVE.")

# Map Phase: Apply sentiment analysis
try:
    logger.info("Starting sentiment analysis")
    start_time = time.time()
    df_mapped = df.withColumn("predicted_sentiment", predict_sentiment(col("truncated_text")))
    logger.info(f"Sentiment analysis completed in {time.time() - start_time:.2f} seconds")
except Exception as e:
    logger.error(f"Error during sentiment analysis: {e}")
    raise

# Check for nulls in output columns
with tqdm(total=1, desc="Checking Nulls in Metrics", unit="operation") as pbar:
    null_metrics_count = df_mapped.filter(col("sentiment").isNull() | col("predicted_sentiment").isNull()).count()
    pbar.update(1)
if null_metrics_count > 0:
    logger.warning(f"Found {null_metrics_count} rows with null values in 'sentiment' or 'predicted_sentiment'.")

# Revised Confusion Matrix Computation with timeout and progress bar
def collect_with_timeout(df, timeout_seconds=300):
    """
    Collect DataFrame with a timeout and progress bar.
    Returns None if timeout occurs.
    """
    result = [None]
    def collect_task():
        try:
            with tqdm(total=1, desc="Collecting Metrics", unit="operation") as pbar:
                result[0] = df.collect()
                pbar.update(1)
        except Exception as e:
            result[0] = e
    
    thread = Thread(target=collect_task)
    thread.start()
    with tqdm(total=timeout_seconds, desc="Waiting for Collect", unit="seconds") as pbar:
        for _ in range(timeout_seconds):
            if not thread.is_alive():
                break
            time.sleep(1)
            pbar.update(1)
    
    if thread.is_alive():
        logger.error(f"Collect operation timed out after {timeout_seconds} seconds")
        return None
    if isinstance(result[0], Exception):
        raise result[0]
    return result[0]

try:
    logger.info("Starting Confusion Matrix computation")
    start_time = time.time()
    # Filter out null values
    with tqdm(total=1, desc="Filtering Nulls", unit="operation") as pbar:
        df_metrics = df_mapped.filter(col("sentiment").isNotNull() & col("predicted_sentiment").isNotNull())
        pbar.update(1)

    # Compute TP, TN, FP, FN using conditional aggregations
    with tqdm(total=1, desc="Aggregating Metrics", unit="operation") as pbar:
        confusion_metrics = df_metrics.groupBy().agg(
            sum_(when((col("sentiment") == "POSITIVE") & (col("predicted_sentiment") == "POSITIVE"), 1).otherwise(0)).alias("tp"),
            sum_(when((col("sentiment") == "NEGATIVE") & (col("predicted_sentiment") == "NEGATIVE"), 1).otherwise(0)).alias("tn"),
            sum_(when((col("sentiment") == "NEGATIVE") & (col("predicted_sentiment") == "POSITIVE"), 1).otherwise(0)).alias("fp"),
            sum_(when((col("sentiment") == "POSITIVE") & (col("predicted_sentiment") == "NEGATIVE"), 1).otherwise(0)).alias("fn")
        )
        pbar.update(1)

    # Collect with timeout
    metrics_result = collect_with_timeout(confusion_metrics, timeout_seconds=300)
    if metrics_result is None:
        raise RuntimeError("Confusion Matrix computation timed out")
    
    metrics_row = metrics_result[0]
    tp = metrics_row["tp"] or 0
    tn = metrics_row["tn"] or 0
    fp = metrics_row["fp"] or 0
    fn = metrics_row["fn"] or 0

    logger.info(f"Confusion Matrix computation completed in {time.time() - start_time:.2f} seconds")
except Exception as e:
    logger.error(f"Error computing Confusion Matrix: {e}")
    raise

# Compute Precision and Recall
precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0

# Display Confusion Matrix and Metrics
print("Confusion Matrix:")
print(f"{'':>15} {'Predicted POSITIVE':>20} {'Predicted NEGATIVE':>20}")
print(f"{'Actual POSITIVE':>15} {tp:>20} {fn:>20}")
print(f"{'Actual NEGATIVE':>15} {fp:>20} {tn:>20}")
print(f"\nPrecision: {precision:.4f}")
print(f"Recall: {recall:.4f}")

# Create a DataFrame for the Confusion Matrix (for CSV)
try:
    with tqdm(total=1, desc="Creating Confusion Matrix DF", unit="operation") as pbar:
        confusion_data = [
            ("Actual POSITIVE", tp, fn),
            ("Actual NEGATIVE", fp, tn)
        ]
        confusion_df = spark.createDataFrame(
            confusion_data,
            ["Actual", "Predicted POSITIVE", "Predicted NEGATIVE"]
        )
        pbar.update(1)
    logger.info("Confusion Matrix DataFrame created")
except Exception as e:
    logger.error(f"Error creating Confusion Matrix DataFrame: {e}")
    raise

# Save the Confusion Matrix to a single CSV file
try:
    with tqdm(total=1, desc="Saving Confusion Matrix CSV", unit="file") as pbar:
        confusion_df.coalesce(1).write.csv("confusion_matrix.csv", header=True, mode="overwrite")
        pbar.update(1)
    logger.info("Confusion Matrix CSV saved")
except Exception as e:
    logger.error(f"Error saving Confusion Matrix CSV: {e}")
    raise

# Create and save the Confusion Matrix as an image
def save_confusion_matrix_image(tp, tn, fp, fn, output_path="confusion_matrix.png"):
    """
    Create a heatmap of the Confusion Matrix and save it as an image with progress bar.
    """
    try:
        with tqdm(total=1, desc="Saving Confusion Matrix Image", unit="file") as pbar:
            cm = np.array([[tp, fn], [fp, tn]])
            labels = ['POSITIVE', 'NEGATIVE']
            
            plt.figure(figsize=(8, 6))
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                        xticklabels=labels, yticklabels=labels)
            plt.xlabel('Predicted')
            plt.ylabel('Actual')
            plt.title('Confusion Matrix')
            
            plt.savefig(output_path, bbox_inches='tight', dpi=300)
            plt.close()
            pbar.update(1)
        logger.info(f"Confusion Matrix image saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving Confusion Matrix image: {e}")
        raise

# Save the Confusion Matrix image
try:
    save_confusion_matrix_image(tp, tn, fp, fn, "confusion_matrix.png")
except Exception as e:
    logger.error(f"Error saving Confusion Matrix image: {e}")
    raise

# Write the processed dataset to a single CSV file (commented out)
try:
    with tqdm(total=1, desc="Saving Processed Dataset CSV", unit="file") as pbar:
        df_mapped.coalesce(1).write.csv("output.csv", header=True, mode="overwrite")
        pbar.update(1)
    logger.info("Processed dataset CSV saved")
except Exception as e:
    logger.error(f"Error saving output CSV: {e}")
    raise

# Stop the Spark session
with tqdm(total=1, desc="Stopping Spark Session", unit="operation") as pbar:
    spark.stop()
    pbar.update(1)
logger.info("Spark session stopped")