import torch
import pandas as pd
import yaml
import logging
import json
from torchvision import transforms
from torch.utils.data import DataLoader
from sklearn.metrics import confusion_matrix
import numpy as np

from train_model import CIFAR10Dataset, CNN

logging.basicConfig(filename='pipeline.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# Evaluate the model and generate performance metrics and misclassified images.
def evaluate(model_file, test_file, report_file, metrics_file, misclassified_file, params):
    logger.info("Starting evaluation")
    
    with open('best_params.json', 'r') as f:
        best_params = json.load(f)
    conv_filters = best_params['conv_filters']
    logger.info(f"Loaded best conv_filters: {conv_filters}")
    
    transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))])
    test_dataset = CIFAR10Dataset(test_file, transform)
    test_loader = DataLoader(test_dataset, batch_size=32)
    
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = CNN(params['model']['num_conv_layers'], conv_filters, params['model']['kernel_size']).to(device)
    model.load_state_dict(torch.load(model_file))
    model.eval()
    
    all_preds = []
    all_labels = []
    misclassified = []
    
    with torch.no_grad():
        for batch_idx, (images, labels) in enumerate(test_loader):
            images, labels = images.to(device), labels.to(device)
            outputs = model(images)
            _, predicted = torch.max(outputs, 1)
            all_preds.extend(predicted.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())
            
            # Track misclassified images
            for i, (pred, true) in enumerate(zip(predicted, labels)):
                if pred != true:
                    img_idx = batch_idx * 32 + i  # Index in the dataset
                    img_path = test_dataset.data.iloc[img_idx]['image']
                    misclassified.append({'image': img_path, 'true': true.item(), 'pred': pred.item()})
    
    # Class-wise accuracy and overall accuracy
    cm = confusion_matrix(all_labels, all_preds)
    class_acc = cm.diagonal() / cm.sum(axis=1)
    overall_acc = np.sum(np.diag(cm)) / np.sum(cm)
    class_names = ['airplane', 'automobile', 'bird', 'cat', 'deer', 'dog', 'frog', 'horse', 'ship', 'truck']
    
    # Generate text report
    report = "Class-wise Accuracy:\n"
    for name, acc in zip(class_names, class_acc):
        report += f"{name}: {acc:.4f}\n"
    report += f"\nOverall Accuracy: {overall_acc:.4f}\n"
    report += "\nConfusion Matrix:\n" + str(cm)
    
    with open(report_file, 'w') as f:
        f.write(report)
    
    # Save metrics for DVC
    metrics = {
        "overall_accuracy": float(overall_acc),
        "class_accuracy": {name: float(acc) for name, acc in zip(class_names, class_acc)}
    }
    with open(metrics_file, "w") as f:
        json.dump(metrics, f)
    
    # Save misclassified images
    misclassified_df = pd.DataFrame(misclassified)
    misclassified_df.to_csv(misclassified_file, index=False)
    logger.info(f"Saved {len(misclassified_df)} misclassified images to {misclassified_file}")
    
    logger.info("Evaluation completed")

if __name__ == "__main__":
    with open("params.yaml", 'r') as f:
        params = yaml.safe_load(f)
    evaluate("model.pth", "test.csv", "report.txt", "metrics.json", "misclassified.csv", params)