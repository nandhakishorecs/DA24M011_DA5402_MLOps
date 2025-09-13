import mlflow
import mlflow.keras
import tensorflow as tf
from tensorflow import keras
import numpy as np
from sklearn.model_selection import train_test_split
import logging
import os
import Levenshtein # type: ignore

# Set MLflow tracking URI
mlflow.set_tracking_uri("http://localhost:8000")

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/training.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load and preprocess data (simplified from Keras example)
def load_data():
    # Using MNIST as a placeholder (replace with actual handwriting dataset)
    (x_train, y_train), (x_test, y_test) = keras.datasets.mnist.load_data()
    x_train = x_train.astype("float32") / 255.0
    x_test = x_test.astype("float32") / 255.0
    # Simulate text labels for edit distance (since MNIST has digits)
    # Replace with actual text labels if using IAM dataset
    text_labels_train = [str(i) for i in y_train]  # E.g., "0", "1", ...
    text_labels_test = [str(i) for i in y_test]
    return x_train, y_train, text_labels_train, x_test, y_test, text_labels_test

# Build the model (simplified CNN)
def build_model():
    model = keras.Sequential([
        keras.layers.Input(shape=(28, 28, 1)),
        keras.layers.Conv2D(32, (3, 3), activation="relu"),
        keras.layers.MaxPooling2D((2, 2)),
        keras.layers.MaxPooling2D((2, 2)),
        keras.layers.Flatten(),
        keras.layers.Dense(128, activation="relu"),
        keras.layers.Dense(128, activation="relu"),
        keras.layers.Dense(10, activation="softmax")
    ])
    return model

# Simulate edit distance calculation (replace with actual logic)
def calculate_avg_edit_distance(model, x_data, true_labels):
    # Predict labels
    preds = model.predict(x_data, verbose=0)
    pred_labels = [str(np.argmax(p)) for p in preds]  # E.g., "0", "1", ...
    # Calculate edit distance for each pair using Levenshtein distance
    distances = [Levenshtein.distance(true, pred) for true, pred in zip(true_labels, pred_labels)]
    return np.mean(distances)

# Training function with MLflow tracking
def train_model(run_name, train_split, val_split, random_state):
    try:
        # Start MLflow run
        with mlflow.start_run(run_name=run_name):
            # Log parameters
            params = {"learning_rate": 0.001, "batch_size": 64, "epochs": 5}
            mlflow.log_params(params)

            # Load and split data
            x, y, text_labels, x_test, y_test, text_labels_test = load_data()
            x_train, x_val, y_train, y_val, text_labels_train, text_labels_val = train_test_split(
                x, y, text_labels, train_size=train_split, test_size=val_split, random_state=random_state
            )

            # Build and compile model
            model = build_model()
            model.compile(
                optimizer=keras.optimizers.Adam(learning_rate=params["learning_rate"]),
                loss="sparse_categorical_crossentropy",
                metrics=["accuracy"]
            )

           

            # Log metrics for loss and edit distance during training
            for epoch in range(params["epochs"]):
                # Train for one epoch
                model.fit(
                    x_train, y_train,
                    validation_data=(x_val, y_val),
                    epochs=1,
                    batch_size=params["batch_size"],
                    verbose=1
                )
                
                # Calculate and log loss metrics
                train_loss = model.evaluate(x_train, y_train, verbose=0)[0]
                val_loss = model.evaluate(x_val, y_val, verbose=0)[0]
                mlflow.log_metric("train_loss", train_loss, step=epoch + 1)
                mlflow.log_metric("val_loss", val_loss, step=epoch + 1)
                
                # Calculate edit distance on validation set after each epoch
                avg_edit_distance = calculate_avg_edit_distance(model, x_val, text_labels_val)
                mlflow.log_metric("avg_edit_distance", avg_edit_distance, step=epoch + 1)

            # Log model
            mlflow.keras.log_model(model, "model")

            # Register model
            model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
            mlflow.register_model(model_uri, f"HandwritingModel_{run_name}")
            logger.info(f"Model trained and registered for {run_name}")

    except Exception as e:
        logger.error(f"Error in training: {str(e)}")
        raise

# Run experiments with different splits
if __name__ == "__main__":
    mlflow.set_tracking_uri("http://127.0.0.1:8000/")  # Adjust if needed
    mlflow.set_experiment("Handwriting_Recognition_v2")
    
    splits = [(0.7, 0.2,10), (0.6, 0.3,100), (0.8, 0.1,300)]  # Train-val splits
    for i, (train_split, val_split, random_state) in enumerate(splits):
        train_model(f"run_split_{train_split}_{val_split}_{random_state}", train_split, val_split, random_state)


# Best Run ID: 2189d329d0934f7b9ab53c7c5cf219dc