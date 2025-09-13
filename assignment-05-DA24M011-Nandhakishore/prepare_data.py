import pandas as pd
import yaml
import logging
from sklearn.model_selection import train_test_split

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='pipeline.log'
)
logger = logging.getLogger(__name__)

# Split dataset into train, validation, and test sets.
def prepare_data(input_file, train_file, val_file, test_file, params):
    logger.info("Starting data preparation")
    
    # Load the input dataset
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        logger.error(f"Failed to load {input_file}: {e}")
        raise
    
    # Check if the DataFrame is empty
    if df.empty:
        logger.error(f"Input dataset {input_file} is empty")
        raise ValueError("Cannot split an empty dataset")
    
    logger.info(f"Loaded dataset with {len(df)} samples")
    
    # Extract split parameters
    seed = params['data']['seed']
    train_size = params['split']['train']
    val_size = params['split']['val']
    test_size = params['split']['test']
    
    # Validate split proportions
    total_size = train_size + val_size + test_size
    if not abs(total_size - 1.0) < 1e-6:  # Allow small floating-point errors
        logger.error(f"Split proportions sum to {total_size}, must sum to 1")
        raise ValueError("Train, val, and test proportions must sum to 1")
    
    # Perform train/val/test split
    train_df, temp_df = train_test_split(df, train_size=train_size, random_state=seed)
    val_proportion = val_size / (val_size + test_size)
    val_df, test_df = train_test_split(temp_df, train_size=val_proportion, random_state=seed)
    
    # Log split sizes
    logger.info(f"Train split: {len(train_df)} samples")
    logger.info(f"Validation split: {len(val_df)} samples")
    logger.info(f"Test split: {len(test_df)} samples")
    
    # Save splits to CSV
    train_df.to_csv(train_file, index=False)
    val_df.to_csv(val_file, index=False)
    test_df.to_csv(test_file, index=False)
    logger.info("Data splits saved successfully")

if __name__ == "__main__":
    with open("params.yaml", 'r') as f:
        params = yaml.safe_load(f)
    prepare_data("dataset.csv", "train.csv", "val.csv", "test.csv", params)