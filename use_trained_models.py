import os
import joblib
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

# === Configuration ===
MODEL_NAME = "Gradient_Boosting"  # Change as needed
BASE_DIR = "machine_models_characters_80"
MODEL_DIR = os.path.join(BASE_DIR,"Machine", MODEL_NAME)
PREPROCESS_DIR = os.path.join(BASE_DIR,"Machine", "preprocessing")
NEW_DATA_FILE = "FossilAll.csv"  # Input file
OUTPUT_FILE = "predictions_output_fossil.csv"  # Output file

# === 1. Load model and preprocessing tools ===
print("Loading model and preprocessing components...")
model = joblib.load(os.path.join(MODEL_DIR, "model.joblib"))
scaler = joblib.load(os.path.join(PREPROCESS_DIR, "feature_scaler.joblib"))
label_encoder = joblib.load(os.path.join(PREPROCESS_DIR, "label_encoder.joblib"))

# === 2. Find and read training CSV to get original columns ===
print("\nSearching training data to get original columns...")
training_csv = None
for file in os.listdir(BASE_DIR):
    if file.endswith('.csv'):
        training_csv = os.path.join(BASE_DIR, file)
        break

if not training_csv:
    print("Error: No training CSV file found in", BASE_DIR)
    exit()

# Read just the header to get column names
try:
    training_columns = pd.read_csv(training_csv, sep=';', encoding='utf-8', nrows=0).columns.tolist()
    print(f"Found {len(training_columns)} columns in training data")
except Exception as e:
    print(f"Error reading training CSV: {e}")
    exit()

# === 3. Load and prepare new input data ===
print(f"\nReading new data from: {NEW_DATA_FILE}")
try:
    # Detect file extension
    file_ext = os.path.splitext(NEW_DATA_FILE)[1].lower()

    if file_ext == ".csv":
        original_data = pd.read_csv(NEW_DATA_FILE, sep=';', encoding='utf-8')
    elif file_ext in [".xls", ".xlsx"]:
        original_data = pd.read_excel(NEW_DATA_FILE)
    else:
        print(f"Error: Unsupported file format ({file_ext}). Use .csv or .xlsx")
        exit()

    new_data = original_data.copy()

except Exception as e:
    print(f"Error reading input file: {e}")
    exit()


# === 4. Filter and align columns ===
print("\nFiltering columns to match training data...")
# Common columns between both datasets
common_columns = [col for col in training_columns if col in new_data.columns]
missing_columns = [col for col in training_columns if col not in new_data.columns]

print(f"Using {len(common_columns)} common columns")
if missing_columns:
    print(f"Warning: {len(missing_columns)} training columns missing in new data")
    print("First 5 missing columns:", missing_columns[:5])
    
    # Retry reading input file with comma separator
    print("Retrying to read input file with separator ','...")
    try:
        original_data = pd.read_csv(NEW_DATA_FILE, sep=',', encoding='utf-8')
        new_data = original_data.copy()
        
        # Recalculate common and missing columns
        common_columns = [col for col in training_columns if col in new_data.columns]
        missing_columns = [col for col in training_columns if col not in new_data.columns]
        
        print(f"After retry - Using {len(common_columns)} common columns")
        if missing_columns:
            print(f"Warning: {len(missing_columns)} training columns still missing in new data")
            print("First 5 missing columns:", missing_columns[:5])
            
    except Exception as e:
        print(f"Error when retrying with separator ',': {e}")
        exit()

# Create DataFrame with missing columns initialized to 0
missing_df = pd.DataFrame(0, index=new_data.index, columns=missing_columns)

# Join with original data
new_data = pd.concat([new_data, missing_df], axis=1)

# Reorder columns to match training data
new_data = new_data[training_columns]

# Crear un DataFrame con las columnas faltantes inicializadas en 0
missing_df = pd.DataFrame(0, index=new_data.index, columns=missing_columns)

# Unirlo con los datos originales
new_data = pd.concat([new_data, missing_df], axis=1)

# Reordenar columnas para que coincidan con el entrenamiento
new_data = new_data[training_columns]


# Reorder columns to exactly match training data
new_data = new_data[training_columns]

# === 5. Impute missing values and scale ===
print("\nPreprocessing data...")
imputer = SimpleImputer(strategy='constant', fill_value=0)
X_imputed = imputer.fit_transform(new_data)
X_scaled = scaler.transform(X_imputed)

# === 6. Make predictions ===
print("\nMaking predictions...")
y_pred_encoded = model.predict(X_scaled)
y_pred_labels = label_encoder.inverse_transform(y_pred_encoded)

# === 7. Prepare output file ===
print("\nPreparing output file...")

# Add predictions to original dataframe
original_data['Predicted_Biome'] = y_pred_labels

# Detect output format based on OUTPUT_FILE extension
output_ext = os.path.splitext(OUTPUT_FILE)[1].lower()

try:
    if output_ext == ".csv":
        original_data.to_csv(OUTPUT_FILE, sep=';', index=False, encoding='utf-8')
    elif output_ext in [".xls", ".xlsx"]:
        original_data.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported output format: {output_ext}. Use .csv or .xlsx")

    print(f"✅ Results successfully saved to: {OUTPUT_FILE}")
    print(f"📊 Total samples processed: {len(original_data)}")

except Exception as e:
    print(f"❌ Error saving results: {e}")
    exit()

# === 8. Show summary ===
print("\nPrediction summary:")
print(original_data['Predicted_Biome'].value_counts())