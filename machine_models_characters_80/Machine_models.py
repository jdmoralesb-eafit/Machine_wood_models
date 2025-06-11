import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import joblib
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.svm import SVC

BASE_OUTPUT_DIR = "machine_models_characters_80"
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

def create_model_subdirectory(model_type):
    model_dir = os.path.join(BASE_OUTPUT_DIR, f"{model_type}")
    os.makedirs(model_dir, exist_ok=True)
    return model_dir

def save_plot(fig, filename, subfolder=""):
    path = os.path.join(subfolder, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig)

def save_metrics_txt(metrics, filename, subfolder=""):
    path = os.path.join(subfolder, filename)
    with open(path, 'w', encoding='utf-8') as f:
        if isinstance(metrics, dict):
            for key, value in metrics.items():
                f.write(f"{key}: {value}\n")

def save_classification_report(report_dict, filename, subfolder=""):
    path = os.path.join(subfolder, filename)
    with open(path, 'w') as f:
        f.write("{:<25} {:<10} {:<10} {:<10} {:<10}\n".format(
            "Clase", "Precisión", "Recall", "F1-Score", "Support"))
        f.write("-"*65 + "\n")
        
        for class_name, metrics in report_dict.items():
            if class_name in ['accuracy', 'macro avg', 'weighted avg']:
                continue
            f.write("{:<25} {:<10.2f} {:<10.2f} {:<10.2f} {:<10}\n".format(
                class_name, metrics['precision'], metrics['recall'], 
                metrics['f1-score'], int(metrics['support'])))
        
        f.write("\n{:<25} {:<10.2f}\n".format("Accuracy:", report_dict['accuracy']))
        f.write("{:<25} {:<10.2f} {:<10.2f} {:<10.2f} {:<10}\n".format(
            "Macro avg", report_dict['macro avg']['precision'],
            report_dict['macro avg']['recall'], report_dict['macro avg']['f1-score'],
            int(report_dict['macro avg']['support'])))
        f.write("{:<25} {:<10.2f} {:<10.2f} {:<10.2f} {:<10}\n".format(
            "Weighted avg", report_dict['weighted avg']['precision'],
            report_dict['weighted avg']['recall'], report_dict['weighted avg']['f1-score'],
            int(report_dict['weighted avg']['support'])))

def preprocess_data(X, y_cat):
    y_cat = y_cat.fillna("desconocido")
    le = LabelEncoder()
    y_encoded = le.fit_transform(y_cat.values.ravel())
    
    unique_classes, class_counts = np.unique(y_encoded, return_counts=True)
    class_dist = dict(zip(le.classes_, class_counts))
    print("Classes distribution", class_dist)
    
    imputer = SimpleImputer(strategy='constant', fill_value=0)
    X_imputed = imputer.fit_transform(X)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_imputed)
    
    feature_names = X.columns.tolist()
    
    return X_scaled, y_encoded, le, scaler, feature_names

def plot_feature_importance(importance, names, model_type, subfolder):
    feature_importance = pd.DataFrame({
        'feature': names,
        'importance': importance
    }).sort_values('importance', ascending=False)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    feature_importance.head(20).plot.barh(x='feature', y='importance', ax=ax)
    ax.set_title(f'Importancia de Características - {model_type}')
    save_plot(fig, f"feature_importance_{model_type.lower()}.png", subfolder)
    save_metrics_txt(feature_importance.set_index('feature').to_dict()['importance'], 
                    f"feature_importance_{model_type.lower()}.txt", subfolder)

def evaluate_classification(model, X_test, y_test, le, feature_names, model_dir):
    y_pred = model.predict(X_test)
    
    report_dict = classification_report(
        y_test, y_pred,
        target_names=le.classes_,
        labels=le.transform(le.classes_),
        output_dict=True,
        zero_division=0
    )

    save_classification_report(report_dict, "classification_report.txt", model_dir)
    joblib.dump(model, os.path.join(model_dir, "model.joblib"))
    
    if len(np.unique(y_pred)) > 1:
        cm = confusion_matrix(y_test, y_pred, labels=np.arange(len(le.classes_)))
        fig, ax = plt.subplots(figsize=(12, 10))
        ConfusionMatrixDisplay(cm, display_labels=le.classes_).plot(ax=ax, cmap='viridis', xticks_rotation=90)
        save_plot(fig, "confusion_matrix.png", model_dir)
    
    if hasattr(model, 'feature_importances_'):
        if np.sum(model.feature_importances_) > 0:
            plot_feature_importance(model.feature_importances_, feature_names, "Classification", model_dir)

def train_and_evaluate_classification_models(X_train, X_test, y_train, y_test, le, feature_names):
    models = {
        'KNN': KNeighborsClassifier(),
        'Random_Forest': RandomForestClassifier(random_state=42),
        'SVM': SVC(random_state=42, probability=True),
        'Gradient_Boosting': GradientBoostingClassifier(random_state=42)
    }
    
    for name, model in models.items():
        print(f"\nTraining {name}...")
        model_dir = create_model_subdirectory(name)
        model.fit(X_train, y_train)
        evaluate_classification(model, X_test, y_test, le, feature_names, model_dir)

def main():

    try:

        df = pd.read_csv("obtener_biomas_Audebert/tropical_con_biomas_final_v2.csv")
    except Exception as e:
        print(f"Error with the input archive: {e}")
        return
    
    if 'bioma_final' not in df.columns:
        print("Error: the column 'bioma_final' doesnt exist")
        print("Avivable columns:", df.columns.tolist())
        return
    

    columns_to_drop = [
        'sp', 'gen', 'fam', 'TropicalHumid', 
        'TropicalDry', 'TropicalGrasslands', 
        'Mangrove', 'Biome', 'predicted_biome'
    ]
    

    columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    df = df.drop(columns=columns_to_drop, errors='ignore')
    

    df = df.dropna(subset=['bioma_final'])
    df = df[df['bioma_final'].str.strip() != '']
    

    X = df.drop(columns=['bioma_final'], errors='ignore')
    y = df[['bioma_final']]
    
    print("\nPreprocessing Data...")
    
    X_scaled, y_encoded, le, scaler, feature_names = preprocess_data(X, y)
    
    preprocess_dir = os.path.join(BASE_OUTPUT_DIR, "preprocessing")
    os.makedirs(preprocess_dir, exist_ok=True)
    joblib.dump(le, os.path.join(preprocess_dir, "label_encoder.joblib"))
    joblib.dump(scaler, os.path.join(preprocess_dir, "feature_scaler.joblib"))
    
    with open(os.path.join(preprocess_dir, "feature_names.txt"), 'w', encoding='utf-8') as f:
        f.write("\n".join(feature_names))

    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y_encoded, test_size=0.2, random_state=42, stratify=y_encoded)
    
    print("\nStarting model training...")
    train_and_evaluate_classification_models(X_train, X_test, y_train, y_test, le, feature_names)
    
    print(f"\nProcess completed. Results saved in: {BASE_OUTPUT_DIR}")

if __name__ == "__main__":
    main()