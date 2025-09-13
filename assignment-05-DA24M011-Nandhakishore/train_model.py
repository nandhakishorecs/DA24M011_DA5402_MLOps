import torch
import torch.nn as nn
import torch.optim as optim
import pandas as pd
import yaml
import logging
import json
from torchvision import transforms
from torch.utils.data import Dataset, DataLoader
from PIL import Image

logging.basicConfig(filename='pipeline.log', level=logging.INFO)
logger = logging.getLogger(__name__)

class CIFAR10Dataset(Dataset):
    def __init__(self, csv_file, transform=None):
        self.data = pd.read_csv(csv_file)
        self.transform = transform
    
    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        img_path = self.data.iloc[idx]['image']
        label = self.data.iloc[idx]['label']
        image = Image.open(img_path).convert('RGB')
        if self.transform:
            image = self.transform(image)
        return image, label

class CNN(nn.Module):
    def __init__(self, num_conv_layers, conv_filters, kernel_size):
        super(CNN, self).__init__()
        layers = []
        in_channels = 3
        for _ in range(num_conv_layers):
            layers.append(nn.Conv2d(in_channels, conv_filters, kernel_size, padding=1))
            layers.append(nn.ReLU())
            layers.append(nn.MaxPool2d(2))
            in_channels = conv_filters
        self.conv = nn.Sequential(*layers)
        # Calculate the flattened size after convolutions and pooling
        size_after_conv = 32 // (2 ** num_conv_layers)
        self.fc = nn.Linear(conv_filters * size_after_conv * size_after_conv, 10)
    
    def forward(self, x):
        x = self.conv(x)
        x = x.view(x.size(0), -1)
        return self.fc(x)

# Train and tune the CNN model, saving the best model and its hyperparameters.
def train_and_tune(train_file, val_file, output_model, params):
    
    logger.info("Starting model training and tuning")
    
    transform = transforms.Compose([transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))])
    train_dataset = CIFAR10Dataset(train_file, transform)
    val_dataset = CIFAR10Dataset(val_file, transform)
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=32)
    
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    best_model = None
    best_acc = 0.0
    best_params = {}
    
    for lr in params['tuning']['lr_options']:
        for conv_filters in params['tuning']['conv_filters_options']:
            model = CNN(params['model']['num_conv_layers'], conv_filters, params['model']['kernel_size']).to(device)
            optimizer = optim.Adam(model.parameters(), lr=lr)
            criterion = nn.CrossEntropyLoss()
            
            # Training loop
            for epoch in range(5):  # Limited epochs for demo
                model.train()
                for images, labels in train_loader:
                    images, labels = images.to(device), labels.to(device)
                    optimizer.zero_grad()
                    outputs = model(images)
                    loss = criterion(outputs, labels)
                    loss.backward()
                    optimizer.step()
            
            # Validation
            model.eval()
            correct = 0
            total = 0
            with torch.no_grad():
                for images, labels in val_loader:
                    images, labels = images.to(device), labels.to(device)
                    outputs = model(images)
                    _, predicted = torch.max(outputs, 1)
                    total += labels.size(0)
                    correct += (predicted == labels).sum().item()
            acc = correct / total
            logger.info(f"LR: {lr}, Filters: {conv_filters}, Val Acc: {acc}")
            
            if acc > best_acc:
                best_acc = acc
                best_model = model.state_dict()
                best_params = {'lr': lr, 'conv_filters': conv_filters}
    
    # Save model and hyperparameters
    torch.save(best_model, output_model)
    with open('best_params.json', 'w') as f:
        json.dump(best_params, f)
    logger.info(f"Best model saved with accuracy: {best_acc}, params: {best_params}")

if __name__ == "__main__":
    with open("params.yaml", 'r') as f:
        params = yaml.safe_load(f)
    train_and_tune("train.csv", "val.csv", "model.pth", params)