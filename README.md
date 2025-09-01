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

### Installation
1. Clone this repository:
   
git clone https://github.com/jdmoralesb-eafit/Machine_wood_models.git

or

Download it thought this page ( press the green button and it will appe

2. Enter to the repository (it has to be where you download it):
   
4. Install Python dependencies (requires Python 3.8 or later):

pip install -r requirements.txt


Usage
Use the script use_trained_models.py in the root folder to make predictions:

to use it just add the data you want to use to make predictions to the repository

then eneter the file use_trained_models.py and change the base_dir to the name of the directory of the models you want to use

then write the model you want to use on MODEL_NAME, the ones that can be used are

Gradient_Boosting
KNN
Random_Forest
SVM

finally write the name of the output file and then run the following line in the terminal

python use_trained_models.py

As a consideration you should have a the data as the examples files that can be found on the repository

chatacteristics_file_input.csv and chatacters_file_input.csv

and the following things
