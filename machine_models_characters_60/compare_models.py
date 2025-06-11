import os
import matplotlib.pyplot as plt
import json 
def parse_classification_report(file_path):
    """Parse el archivo de reporte de clasificación"""
    report_data = {}
    encodings = ['utf-8', 'ISO-8859-1', 'utf-16']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            # Buscar accuracy (puede estar en diferentes formatos)
            accuracy_line = [line for line in content.split('\n') if 'accuracy' in line.lower()]
            if accuracy_line:
                try:
                    accuracy = float(accuracy_line[0].split()[-1])
                    report_data["accuracy"] = {'accuracy': accuracy}
                except:
                    pass
            
            # Buscar tabla de métricas por clase
            lines = content.split('\n')
            start_index = next((i for i, line in enumerate(lines) 
                             if line.strip().startswith('Clase') or 
                                line.strip().startswith('Class')), None)
            
            if start_index is not None:
                for line in lines[start_index+1:]:
                    if line.strip() and not any(x in line for x in ['avg', 'accuracy', 'macro', 'weighted']):
                        parts = line.split()
                        if len(parts) >= 5:
                            class_name = ' '.join(parts[:-4])
                            try:
                                report_data[class_name] = {
                                    'precision': float(parts[-4]),
                                    'recall': float(parts[-3]),
                                    'f1_score': float(parts[-2]),
                                    'support': int(parts[-1])
                                }
                            except:
                                continue
            return report_data
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"No se pudo leer el archivo {file_path}")

def compare_models(base_dir):
    """Busca reportes en las subcarpetas de modelos"""
    model_reports = {}
    
    # Modelos esperados (según tu imagen)
    expected_models = ['Gradient_Boosting', 'KNN', 'Random_Forest', 'SVM']
    
    for model in expected_models:
        model_path = os.path.join(base_dir, model)
        if os.path.exists(model_path):
            # Buscar cualquier archivo que pueda contener el reporte
            possible_files = [f for f in os.listdir(model_path) 
                            if 'report' in f.lower() or 'classification' in f.lower()]
            
            if possible_files:
                file_path = os.path.join(model_path, possible_files[0])
                try:
                    report = parse_classification_report(file_path)
                    model_reports[model] = report
                    print(f"Procesado: {model}")
                except Exception as e:
                    print(f"Error en {model}: {str(e)}")
            else:
                print(f"No se encontró archivo de reporte en {model}")
        else:
            print(f"No se encontró carpeta para {model}")
    
    return model_reports

def main():
    # Asegúrate de que esta ruta es correcta
    base_dir = os.path.join("machine_models_characters_60", 
                          "Machine")
    
    print(f"\nBuscando modelos en: {base_dir}")
    
    # Verificar si la ruta existe
    if not os.path.exists(base_dir):
        print(f"\nERROR: No se encuentra la carpeta {base_dir}")
        print("Posibles soluciones:")
        print("1. Verifica que la ruta es correcta")
        print("2. Asegúrate que los modelos están en las subcarpetas Gradient_Boosting, KNN, etc.")
        return
    
    model_reports = compare_models(base_dir)
    
    if not model_reports:
        print("\nNo se encontraron reportes de clasificación. Verifica:")
        print("1. Que existan archivos de reporte en cada carpeta de modelo")
        print("2. Que los archivos tengan nombres que contengan 'report' o 'classification'")
        print("\nEstructura esperada:")
        print("Modelos_Audebert_4_etiquetas/")
        print("└── Machine_sin_compartidos_corrigiendo_datos/")
        print("    ├── Gradient_Boosting/")
        print("    │   └── classification_report.txt")
        print("    ├── KNN/") 
        print("    │   └── classification_report.txt")
        print("    └── ...")
    else:
        print(f"\nSe procesaron {len(model_reports)} modelos")

                # Obtener todas las clases posibles
        all_classes = set()
        for report in model_reports.values():
            all_classes.update([k for k in report.keys() if k != 'accuracy'])
        all_classes = sorted(all_classes)

        # Encabezado
        output_path = os.path.join(base_dir, "resumen_resultados_modelos.txt")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("Comparación de modelos (Precisión, Recall, F1-Score, Accuracy):\n")
            f.write(f"{'Clase':<30}")
            for model in model_reports:
                f.write(f"{model:<25}")
            f.write("\n")
            f.write("-" * (30 + 25 * len(model_reports)) + "\n")

            # Escribir métricas por clase
            for clase in all_classes:
                f.write(f"{clase:<30}")
                for model in model_reports:
                    metrics = model_reports[model].get(clase)
                    if metrics:
                        trio = f"{metrics['precision']:.2f}/{metrics['recall']:.2f}/{metrics['f1_score']:.2f}"
                    else:
                        trio = "N/A"
                    f.write(f"{trio:<25}")
                f.write("\n")

            # Accuracy al final
            f.write(f"{'accuracy':<30}")
            for model in model_reports:
                acc = model_reports[model].get('accuracy', {}).get('accuracy')
                acc_str = f"{acc:.2f}" if acc is not None else "N/A"
                f.write(f"{acc_str:<25}")
            f.write("\n")

        print(f"Resumen guardado en: {output_path}")


if __name__ == "__main__":
    main()