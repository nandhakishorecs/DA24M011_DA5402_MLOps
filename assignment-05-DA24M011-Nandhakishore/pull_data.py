import os
import yaml
import logging
import pandas as pd

logging.basicConfig(filename='pipeline.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# Pull specified version(s) of dataset from DVC and save as a DataFrame.
# version: v1, v2, v1+v2, v3, etc
def pull_data(version, output_file):

    logger.info(f"Pulling data for version: {version}")
    
    # Split version string into individual partitions
    versions = version.split('+')
    images = []
    labels = []
    class_names = ['airplane', 'automobile', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck']
    
    for v in versions:
        partition_path = f"partitions/partition_{v}"
        logger.info(f"Checking out {partition_path}.dvc")
        
        # Perform DVC checkout
        checkout_result = os.system(f"dvc checkout {partition_path}.dvc")
        if checkout_result != 0:
            logger.error(f"DVC checkout failed for {partition_path}.dvc")
            raise RuntimeError(f"Failed to checkout {partition_path}.dvc")
        
        if not os.path.exists(partition_path):
            logger.error(f"Partition {partition_path} not found")
            raise FileNotFoundError(f"Partition {partition_path} not found")
        
        # Collect images from class sub-folders
        for class_idx, class_name in enumerate(class_names):
            class_dir = os.path.join(partition_path, class_name)
            if os.path.exists(class_dir):
                class_images = [os.path.join(class_dir, img) for img in os.listdir(class_dir) if img.endswith('.png')]
                images.extend(class_images)
                labels.extend([class_idx] * len(class_images))
                logger.info(f"Found {len(class_images)} images in {class_dir}")
            else:
                logger.warning(f"Class directory {class_dir} not found")
    
    if not images:
        logger.error(f"No images found for version {version}")
        raise ValueError(f"No images available for version {version}")
    
    # Create DataFrame and save
    df = pd.DataFrame({'image': images, 'label': labels})
    df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(df)} images to {output_file}")

if __name__ == "__main__":
    with open("params.yaml", 'r') as f:
        params = yaml.safe_load(f)
    pull_data(params['data']['version'], "dataset.csv")