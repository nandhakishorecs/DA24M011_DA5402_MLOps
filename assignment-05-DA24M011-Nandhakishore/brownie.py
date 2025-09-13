import pandas as pd
import logging
import numpy as np

logging.basicConfig(filename='hard_to_learn.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# Identify hard-to-learn images from v1 across models.
def analyze_hard_to_learn(v1_file, v1v2_file, v1v2v3_file, output_file):
    logger.info("Analyzing hard-to-learn images")
    
    # Load misclassified images
    v1_mis = pd.read_csv(v1_file)
    v1v2_mis = pd.read_csv(v1v2_file)
    v1v3_mis = pd.read_csv(v1v2v3_file)
    
    # Filter v1 images (paths containing 'partition_v1')
    v1_mis = v1_mis[v1_mis['image'].str.contains('partition_v1')]
    v1v2_mis = v1v2_mis[v1v2_mis['image'].str.contains('partition_v1')]
    v1v3_mis = v1v3_mis[v1v3_mis['image'].str.contains('partition_v1')]
    
    # Find images misclassified in all three models
    common_mis = v1_mis.merge(v1v2_mis, on='image', suffixes=('_v1', '_v1v2')).merge(v1v3_mis, on='image', suffixes=('', '_v1v2v3'))
    
    if common_mis.empty:
        logger.warning("No images were misclassified across all three models")
        report = "No hard-to-learn images found.\n"
    else:
        # Class distribution (using true labels from v1 model)
        class_names = ['airplane', 'automobile', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck']
        class_dist = common_mis['true_v1'].value_counts().reindex(range(10), fill_value=0)
        
        # Misclassification table (true vs most common predicted label) - For simplicity, use predicted label from v1 model
        mis_table = pd.crosstab(common_mis['true_v1'], common_mis['pred_v1'], rownames=['True'], colnames=['Predicted'])
        
        # Generate report
        report = "Class Distribution of Hard-to-Learn Images:\n"
        for idx, count in class_dist.items():
            report += f"{class_names[idx]}: {count}\n"
        report += "\nMisclassification Table (True vs Predicted from v1 model):\n"
        report += str(mis_table) + "\n"
        report += f"\nTotal hard-to-learn images: {len(common_mis)}"
    
    with open(output_file, 'w') as f:
        f.write(report)
    logger.info(f"Analysis saved to {output_file}")

if __name__ == "__main__":
    analyze_hard_to_learn("misclassified_v1.csv", "misclassified_v1+v2.csv", "misclassified_v1+v2+v3.csv", "hard_to_learn_report.txt")