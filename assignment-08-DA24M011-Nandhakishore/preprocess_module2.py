import json
import numpy as np
from PIL import Image
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def preprocess_image(image_path, output_json="image_data.json"):
    try:
        img = Image.open(image_path).convert("L")  # Grayscale
        img = img.resize((28, 28))  # Match model input shape
        img_array = np.array(img).astype("float32") / 255.0
        img_array = np.expand_dims(img_array, axis=(0, -1))  # Shape: (1, 28, 28, 1)
        payload = {"inputs": img_array.tolist()}
        with open(output_json, "w") as f:
            json.dump(payload, f)
        logger.info(f"Saved preprocessed image data to {output_json}")
        return payload
    except Exception as e:
        logger.error(f"Error preprocessing image: {str(e)}")
        raise

if __name__ == "__main__":
    image_path = input("Enter the path to the image: ")
    preprocess_image(image_path)