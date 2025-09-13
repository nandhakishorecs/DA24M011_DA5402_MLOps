# File Handling 
import os
import pickle

# Data handling
import numpy as np

# For Classifier
from utils import *
from dense_neural_class import *

# Server - HTTP 
from fastapi import FastAPI, Body
import uvicorn
from pydantic import BaseModel

# Function to load the model with absolute path
def load_model(filename):
    # Gets the current directory where the script is being executed and construct the full path of the .pkl file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(current_dir, filename + '.pkl')
    
    with open(filepath, 'rb') as file:
        model_loaded = pickle.load(file)
    
    return model_loaded

# Load the model when starting the program
model = load_model('model')

# Data type to load the details from client. 
class ImagePayload(BaseModel):
    # Expecting a flattened list of 784 values
    image: list  

# create the webapp.
app = FastAPI(title="DA5402_A3")

@app.post("/predict")
def image_predict(payload:ImagePayload):
    try:
        # Convert list to NumPy array
        img_array = np.array(payload.image, dtype=np.float32).reshape(1, 784)  # Reshape for model input

        # Run prediction
        prediction = model.predict(img_array)
        predicted_digit = int(np.argmax(prediction))  # Get the class with highest probability

        # return {"prediction": predicted_digit} #, "confidence": float(np.max(prediction))}
        return {predicted_digit}
    
    except Exception as e:
        return {"error": str(e)}

# Run from command line: uvicorn apiWithBody:app --port 7000 --host 0.0.0.0
uvicorn.run(app, host='127.0.0.1', port=54320)