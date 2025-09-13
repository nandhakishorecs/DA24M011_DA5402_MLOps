from pyspark.sql import SparkSession                # type: ignore
from pyspark.sql.functions import col, when         # type: ignore
from pyspark.sql.types import StringType            # type: ignore
import logging
from tqdm import tqdm                               # type: ignore
import matplotlib.pyplot as plt                     # Import matplotlib for plotting
import seaborn as sns                              # type: ignore # Import seaborn for heatmap
import numpy as np                                 # Import numpy for array manipulation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('task2.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Spark session
logger.info("Initializing Spark session")
try:
    spark = SparkSession.builder \
        .appName("SentimentClassifierEvaluation") \
        .getOrCreate()
except Exception as e:
    logger.error(f"Failed to initialize Spark session: {str(e)}")
    raise

# Read the Parquet file
logger.info("Reading gourmet_foods.parquet")
try:
    df = spark.read.parquet("gourmet_foods.parquet")
except Exception as e:
    logger.error(f"Failed to read Parquet file: {str(e)}")
    spark.stop()
    raise

# Discretize review_score into POSITIVE (>= 3.0) and NEGATIVE (< 3.0)
logger.debug("Discretizing review_score")
df = df.withColumn(
    "ground_truth",
    when(col("review_score").cast("float") >= 3.0, "POSITIVE").otherwise("NEGATIVE")
)

# Map step: Create pairs of (predicted, ground_truth) for confusion matrix
def map_to_confusion_matrix(row):
    predicted = row.predicted_sentiment
    ground_truth = row.ground_truth
    return ((predicted, ground_truth), 1)

# Reduce step: Aggregate counts for each (predicted, ground_truth) pair
logger.info("Computing confusion matrix")
try:
    confusion_rdd = df.rdd.map(map_to_confusion_matrix).reduceByKey(lambda a, b: a + b)
    # Collect results with tqdm
    confusion_counts = {}
    logger.debug("Collecting confusion matrix counts")
    for (predicted, ground_truth), count in tqdm(confusion_rdd.collect(), desc="Collecting confusion matrix counts"):
        confusion_counts[(predicted, ground_truth)] = count
except Exception as e:
    logger.error(f"Failed to compute confusion matrix: {str(e)}")
    spark.stop()
    raise

# Initialize confusion matrix components
tp = confusion_counts.get(("POSITIVE", "POSITIVE"), 0)  # True Positives
tn = confusion_counts.get(("NEGATIVE", "NEGATIVE"), 0)  # True Negatives
fp = confusion_counts.get(("POSITIVE", "NEGATIVE"), 0)  # False Positives
fn = confusion_counts.get(("NEGATIVE", "POSITIVE"), 0)  # False Negatives

# Calculate Precision and Recall
precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
logger.info(f"Calculated metrics: Precision={precision:.4f}, Recall={recall:.4f}")

# Display confusion matrix and metrics
confusion_output = (
    "Confusion Matrix:\n"
    f"{'':<10} {'Ground Truth':<20}\n"
    f"{'':<10} {'POSITIVE':<10} {'NEGATIVE':<10}\n"
    f"{'Predicted':<10}\n"
    f"{'POSITIVE':<10} {tp:<10} {fp:<10}\n"
    f"{'NEGATIVE':<10} {fn:<10} {tn:<10}\n"
    "\nMetrics:\n"
    f"Precision: {precision:.4f}\n"
    f"Recall: {recall:.4f}\n"
)
print(confusion_output)
logger.info("Displayed confusion matrix and metrics")

# Save confusion matrix and metrics to a text file
logger.info("Saving confusion matrix and metrics to confusion_matrix_stats.txt")
try:
    with open("confusion_matrix_stats.txt", "w") as f:
        f.write(confusion_output)
except Exception as e:
    logger.error(f"Failed to write to confusion_matrix_stats.txt: {str(e)}")
    spark.stop()
    raise

# Create and save confusion matrix as PNG
logger.info("Saving confusion matrix as confusion_matrix.png")
try:
    # Create confusion matrix array
    confusion_matrix = np.array([[tp, fp], [fn, tn]])
    labels = ['POSITIVE', 'NEGATIVE']

    # Plot confusion matrix using seaborn heatmap
    plt.figure(figsize=(8, 6))
    sns.heatmap(
        confusion_matrix,
        annot=True,
        fmt='d',
        cmap='Blues',
        xticklabels=labels,
        yticklabels=labels,
        cbar=True
    )
    plt.title('Confusion Matrix')
    plt.xlabel('Ground Truth')
    plt.ylabel('Predicted')
    
    # Save the plot as PNG
    plt.savefig('confusion_matrix.png', bbox_inches='tight', dpi=300)
    plt.close()  # Close the plot to free memory
    logger.info("Successfully saved confusion matrix as confusion_matrix.png")
except Exception as e:
    logger.error(f"Failed to save confusion matrix as PNG: {str(e)}")
    spark.stop()
    raise

# Stop Spark session
logger.info("Stopping Spark session")
spark.stop()