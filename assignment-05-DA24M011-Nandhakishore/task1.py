import os
import tarfile
import pickle
import numpy as np
from PIL import Image
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='cifar10_extract.log'
)
logger = logging.getLogger(__name__)

def unpickle(file):
    """
    Load a pickled CIFAR-10 batch file.
    
    Args:
        file (str): Path to the pickled file.
    
    Returns:
        dict: Dictionary containing data and labels.
    """
    with open(file, 'rb') as fo:
        data_dict = pickle.load(fo, encoding='bytes')
    return data_dict

def extract_and_organize(tar_path, output_dir):
    """
    Extract CIFAR-10 from a .tar.gz file and organize into class sub-folders.
    
    Args:
        tar_path (str): Path to the .tar.gz file.
        output_dir (str): Directory where images will be saved.
    """
    logger.info(f"Starting extraction of {tar_path}")
    
    # Create output directory if it doesn’t exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Define class names for CIFAR-10
    class_names = [
        'airplane', 'automobile', 'bird', 'cat', 'deer',
        'dog', 'frog', 'horse', 'ship', 'truck'
    ]
    
    # Create sub-folders for each class
    for class_name in class_names:
        class_dir = os.path.join(output_dir, class_name)
        if not os.path.exists(class_dir):
            os.makedirs(class_dir)
    
    # Extract the tar.gz file
    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(path='temp_cifar10')
        logger.info("Extraction completed")
    
    # Process training batches (data_batch_1 to data_batch_5)
    image_count = 0
    for batch_num in range(1, 6):
        batch_file = os.path.join('temp_cifar10', 'cifar-10-batches-py', f'data_batch_{batch_num}')
        if os.path.exists(batch_file):
            batch = unpickle(batch_file)
            images = batch[b'data']  # 10,000 x 3072 (32x32x3)
            labels = batch[b'labels']  # 10,000 labels
            
            # Reshape and save images
            for idx, (image_data, label) in enumerate(zip(images, labels)):
                # Reshape from (3072,) to (3, 32, 32) and transpose to (32, 32, 3)
                image_array = image_data.reshape(3, 32, 32).transpose(1, 2, 0)
                image = Image.fromarray(image_array.astype('uint8'), 'RGB')
                
                class_name = class_names[label]
                img_path = os.path.join(output_dir, class_name, f'image_{image_count}.png')
                image.save(img_path)
                image_count += 1
                
                if image_count % 1000 == 0:
                    logger.info(f"Processed {image_count} training images")
    
    # Process test batch (test_batch)
    test_file = os.path.join('temp_cifar10', 'cifar-10-batches-py', 'test_batch')
    if os.path.exists(test_file):
        test_batch = unpickle(test_file)
        images = test_batch[b'data']  # 10,000 x 3072
        labels = test_batch[b'labels']  # 10,000 labels
        
        for idx, (image_data, label) in enumerate(zip(images, labels)):
            image_array = image_data.reshape(3, 32, 32).transpose(1, 2, 0)
            image = Image.fromarray(image_array.astype('uint8'), 'RGB')
            
            class_name = class_names[label]
            img_path = os.path.join(output_dir, class_name, f'image_{image_count}.png')
            image.save(img_path)
            image_count += 1
            
            if image_count % 1000 == 0:
                logger.info(f"Processed {image_count} total images (including test)")
    
    # Clean up temporary extraction directory
    import shutil
    shutil.rmtree('temp_cifar10')
    logger.info(f"Total {image_count} images organized into {output_dir}")

if __name__ == "__main__":
    # Replace with the path to your .tar.gz file
    tar_path = "cifar-10-python.tar.gz"
    extract_and_organize(tar_path, "cifar10_images")