Biome Classification from character and characteristics Data
This repository contains a complete pipeline for retrieving, merging, and classifying biome information based on anatomical wood features and species occurrences. The workflow includes data preparation, biome assignment, and machine learning model training.

Folder Structure
The main structure of the repository is as follows:

- requirements.txt

- use_trained_models.py

- machine_models_characteristics_60/

- machine_models_characteristics_80/

- machine_models_characters_60/

- machine_models_characters_80/

- retrieve_biome/

Each of the machine_models_* folders contains trained machine learning models and scripts for classification using either 60% or 80% as a threshold for the biome selection.

Workflow Overview
1. Biome Retrieval (retrieve_biome/)
This folder contains scripts to assign biomes to species based on their geographic coordinates.

Execution steps:

- coordenadas_ocurrencias.R
  Extracts coordinates for all species in the dataset.
  Note: This step may take a long time depending on the number of species.
  
- get_biome_with_neighbors.py or get_biome.py
  Assigns Holdridge biomes based on species locations.
  Recommendation: use get_biome_with_neighbors.py for more robust assignment using neighborhood information.
  
- merge_biome_features_*.py
  Merges anatomical and biome data to prepare the final dataset for model training.

2. Model Training and Evaluation
The folders named machine_models_* contain:

- compare_models.py
  
- Machine_models.py
  
- Preprocessed CSV files with anatomical and biome labels

Subfolders with trained models (Gradient Boosting, KNN, Random Forest, SVM)

Each subfolder contains:

- Trained model files (.joblib)
  
- Classification reports and confusion matrices
  
- Feature importance plots and data

The preprocessing folder inside each model folder contains:

- Scalers

- Encoders

Feature name references used in training

3. Using Trained Models
To apply the trained models on new data, use the script use_trained_models.py in the root folder. This script will load the selected model (see inputs in the file) and preprocessing pipeline for inference.


# Installation:
Prerequisites
Python 3.8 or later

pip (Python package manager)

Setup Instructions
Get the code:

## Option 1: Clone via Git
git clone https://github.com/jdmoralesb-eafit/Machine_wood_models.git

## Option 2: Download manually
- Click the green "Code" button on GitHub
- Select "Download ZIP"
- Extract the downloaded file


## Navigate to the project and install the dependencies:

Navigate to the project directory: cd Machine_wood_models and then install required dependencies:

pip install -r requirements.txt
or
pip3 install -r requirements.txt

# Usage
Making Predictions
Use the use_trained_models.py script to generate predictions with pre-trained models:

-  Prepare your input data:

- Place your data file (CSV or Excel format) in the project root directory, ensure your data follows the same format as one of the example files (chatacteristics_file_input.csv or chatacters_file_input.csv)

Note: aqui va la explicacion de Camila de como hacer el archivo

# Configure the script:
Open use_trained_models.py in a text editor and modify these key variables:

python
- BASE_DIR = "name_of_model_directory"  # e.g., "machine_models_characters_80"
- MODEL_NAME = "Gradient_Boosting"      # Choose from available models
- NEW_DATA_FILE = "your_input_filename.csv"  # Input file
- OUTPUT_FILE = "your_output_filename.csv"  # Desired output file name
### Available models:

- Gradient_Boosting

- KNN

- Random_Forest

- SVM

Run the prediction script:

python use_trained_models.py
or
python3 use_trained_models.py
