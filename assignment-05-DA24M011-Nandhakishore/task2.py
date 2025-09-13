import os
import random
import shutil
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='partition_creation.log'
)
logger = logging.getLogger(__name__)

# Create three random partitions with class sub-folders from CIFAR-10 dataset.
def create_partitions(src_dir, output_base_dir, num_samples=20000, seed=42):
    random.seed(seed)
    logger.info("Starting partition creation")
    
    # Define class names
    class_names = ['airplane', 'automobile', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck']
    all_images_by_class = {cls: [] for cls in class_names}
    
    # Collect images by class
    for class_name in class_names:
        class_dir = os.path.join(src_dir, class_name)
        if not os.path.exists(class_dir):
            logger.error(f"Class directory {class_dir} not found")
            raise FileNotFoundError(f"Missing {class_dir}")
        images = [os.path.join(class_dir, img) for img in os.listdir(class_dir) if img.endswith('.png')]
        all_images_by_class[class_name].extend(images)
        logger.info(f"Collected {len(images)} images for class {class_name}")
    
    # Check total images
    total_images = sum(len(imgs) for imgs in all_images_by_class.values())
    if total_images < 3 * num_samples:
        logger.error(f"Only {total_images} images available, need {3 * num_samples}")
        raise ValueError("Insufficient images for 3 partitions")
    
    # Sample proportionally from each class (e.g., 2000 per class for 10 classes)
    samples_per_class = num_samples // len(class_names)
    partitions = [{cls: [] for cls in class_names} for _ in range(3)]
    
    for class_name in class_names:
        images = all_images_by_class[class_name]
        random.shuffle(images)
        partitions[0][class_name] = images[:samples_per_class]
        partitions[1][class_name] = images[samples_per_class:2*samples_per_class]
        partitions[2][class_name] = images[2*samples_per_class:3*samples_per_class]
    
    # Create partition directories with class sub-folders
    for i, partition in enumerate(partitions, 1):
        partition_dir = os.path.join(output_base_dir, f"partition_v{i}")
        if os.path.exists(partition_dir):
            shutil.rmtree(partition_dir)  # Clean up existing
        os.makedirs(partition_dir)
        
        for class_name in class_names:
            class_dir = os.path.join(partition_dir, class_name)
            os.makedirs(class_dir)
            for img_path in partition[class_name]:
                shutil.copy(img_path, class_dir)
        total_in_partition = sum(len(v) for v in partition.values())
        logger.info(f"Created partition_v{i} with {total_in_partition} images")
    
    logger.info("Partition creation completed")

if __name__ == "__main__":
    create_partitions("cifar10_images", "partitions")