import pandas as pd
import psycopg2
from sklearn.ensemble import IsolationForest
import mlflow
import mlflow.sklearn
from sklearn.metrics import classification_report
import numpy as np

# Connection to PostgreSQL (warehouse)
conn = psycopg2.connect(
    host="localhost",
    database="orion",
    user="admin",
    password="admin123"
)

# Load historical labelled data (assume table `features` has column `is_anomaly` 0/1)
query = "SELECT temperature, vibration, rpm, is_anomaly FROM features"
df = pd.read_sql(query, conn)
conn.close()

X = df[["temperature", "vibration", "rpm"]]
y = df["is_anomaly"]

# Train Isolation Forest
model = IsolationForest(contamination=0.05, random_state=42)
model.fit(X)

# Predict (returns -1 for anomaly, 1 for normal)
y_pred = model.predict(X)
y_pred_binary = [1 if x == -1 else 0 for x in y_pred]   # 1 = anomaly

# Evaluation
report = classification_report(y, y_pred_binary, output_dict=True)

# -------------------------------
# MLflow Tracking
# -------------------------------
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("orion_predictive_maintenance")

with mlflow.start_run(run_name="isolation_forest_v1"):
    mlflow.log_param("contamination", 0.05)
    mlflow.log_metric("precision", report["1"]["precision"])
    mlflow.log_metric("recall", report["1"]["recall"])
    mlflow.log_metric("f1", report["1"]["f1-score"])
    
    # Log model
    mlflow.sklearn.log_model(model, "isolation_forest_model")
    
    # Register the model
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/isolation_forest_model"
    mlflow.register_model(model_uri, "OrionAnomalyDetector")

print("Training complete. Model registered in MLflow.")
