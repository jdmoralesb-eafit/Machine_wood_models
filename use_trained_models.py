import os
import joblib
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

# === Configuration ===
MODEL_NAME = "Random_Forest"  # Change to KNN, SVM, Gradient_Boosting if needed
BASE_DIR = "machine_models_characteristics_80"
MODEL_DIR = os.path.join(BASE_DIR, MODEL_NAME)
PREPROCESS_DIR = os.path.join(BASE_DIR, "preprocessing")

# === Load model and preprocessing tools ===
print("Loading model and preprocessing components...")
model = joblib.load(os.path.join(MODEL_DIR, "model.joblib"))
scaler = joblib.load(os.path.join(PREPROCESS_DIR, "feature_scaler.joblib"))
label_encoder = joblib.load(os.path.join(PREPROCESS_DIR, "label_encoder.joblib"))

# === Load feature names used during training ===
feature_names_path = os.path.join(PREPROCESS_DIR, "feature_names.txt")
with open(feature_names_path, 'r', encoding='utf-8') as f:
    feature_names = [line.strip() for line in f.readlines()]

# === Load and prepare new input data ===
NEW_DATA_FILE = "your_new_data.csv"  # Replace with your actual file
print(f"Reading new data from: {NEW_DATA_FILE}")
try:
    new_data = pd.read_csv(NEW_DATA_FILE, sep=';', encoding='utf-8')
except Exception as e:
    print(f"Error reading input file: {e}")
    exit()

# === Ensure only trained features are used ===
missing_features = [col for col in feature_names if col not in new_data.columns]
if missing_features:
    print("Error: Missing required features in input data:", missing_features)
    exit()

new_data = new_data[feature_names]

# === Impute missing values and scale ===
imputer = SimpleImputer(strategy='constant', fill_value=0)
X_imputed = imputer.fit_transform(new_data)
X_scaled = scaler.transform(X_imputed)

# === Make predictions ===
print("Making predictions...")
y_pred_encoded = model.predict(X_scaled)
y_pred_labels = label_encoder.inverse_transform(y_pred_encoded)

# === Output results ===
print("\nPredictions:")
for i, pred in enumerate(y_pred_labels):
    print(f"Sample {i+1}: Predicted Biome = {pred}")
