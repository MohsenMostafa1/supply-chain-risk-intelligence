from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import numpy as np

app = FastAPI()

# Load model from MLflow (in practice download from registry)
model = joblib.load("/models/isolation_forest.pkl")

class SensorData(BaseModel):
    temperature: float
    vibration: float
    rpm: int

@app.post("/predict")
def predict(data: SensorData):
    features = np.array([[data.temperature, data.vibration, data.rpm]])
    prediction = model.predict(features)
    # IsolationForest returns 1 for normal, -1 for anomaly
    is_anomaly = True if prediction[0] == -1 else False
    return {"anomaly": is_anomaly, "score": float(prediction[0])}
