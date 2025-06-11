import pandas as pd
import os
from glob import glob

# 1. Cargar el diccionario de reclasificación desde el Excel
reclass_df = pd.read_excel("retrieve_biome/Holdridge biome reclassification.xlsx", sheet_name="Sheet1")

# Crear diccionarios para mapeo
bioma_original_dict = dict(zip(reclass_df.iloc[:, 0], reclass_df.iloc[:, 1]))
bioma_reclasificado_dict = dict(zip(reclass_df.iloc[:, 0], reclass_df.iloc[:, 2]))

# 2. Combinar todos los archivos CSV de biomas
ruta_carpeta = "retrieve_biome/biomas_v2"
archivos_biomas = glob(os.path.join(ruta_carpeta, "biomas_*.csv"))

df_completo = pd.concat([pd.read_csv(archivo) for archivo in archivos_biomas], ignore_index=True)

# 3. Añadir las nuevas columnas
df_completo["bioma_original"] = df_completo["bioma_asignado"].map(bioma_original_dict)
df_completo["bioma_reclasificado"] = df_completo["bioma_asignado"].map(bioma_reclasificado_dict)

# Función para determinar el nivel de agregación
def determinar_nivel(bioma_asignado):
    if pd.isna(bioma_asignado):
        return "No data"
    return "Reclassified" if bioma_asignado in bioma_reclasificado_dict else "Level III"

df_completo["bioma_nivel"] = df_completo["bioma_asignado"].apply(determinar_nivel)

# 4. Guardar el resultado
output_path = os.path.join(ruta_carpeta, "biomas_reclasificados_completos_v2.csv")
df_completo.to_csv(output_path, index=False)

print(f"¡Proceso completado! Archivo guardado en: {output_path}")
print(f"Total de registros: {len(df_completo)}")
print("\nEjemplo de registros:")
print(df_completo.head(3))