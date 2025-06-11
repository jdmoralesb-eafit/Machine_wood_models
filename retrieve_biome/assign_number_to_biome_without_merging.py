import pandas as pd
import os

# 1. Cargar el diccionario de reclasificación desde el Excel
reclass_df = pd.read_excel("retrieve_biome/Holdridge biome reclassification.xlsx", sheet_name="Sheet1")

# Crear diccionarios para mapeo
bioma_original_dict = dict(zip(reclass_df.iloc[:, 0], reclass_df.iloc[:, 1]))
bioma_reclasificado_dict = dict(zip(reclass_df.iloc[:, 0], reclass_df.iloc[:, 2]))

# 2. Cargar directamente el archivo final de salida
ruta_carpeta = "retrieve_biome"
archivo_final = os.path.join(ruta_carpeta, "biomas_reclasificados_completos_final.csv")

if not os.path.exists(archivo_final):
    print(f"Error: No se encontró el archivo {archivo_final}")
    print("Por favor ejecuta primero el script de procesamiento de biomas")
    exit()

df_completo = pd.read_csv(archivo_final)

# 3. Añadir las nuevas columnas
df_completo["bioma_original"] = df_completo["bioma_asignado"].map(bioma_original_dict)
df_completo["bioma_reclasificado"] = df_completo["bioma_asignado"].map(bioma_reclasificado_dict)

# Función para determinar el nivel de agregación
def determinar_nivel(bioma_asignado):
    if pd.isna(bioma_asignado):
        return "No data"
    return "Reclassified" if bioma_asignado in bioma_reclasificado_dict else "Level III"

df_completo["bioma_nivel"] = df_completo["bioma_asignado"].apply(determinar_nivel)

# 4. Guardar el resultado (podemos sobreescribir o crear uno nuevo)
output_path = os.path.join(ruta_carpeta, "biomas_reclasificados_con_info_v2.csv")
df_completo.to_csv(output_path, index=False)

print(f"¡Proceso completado! Archivo guardado en: {output_path}")
print(f"Total de registros: {len(df_completo)}")
print("\nResumen de biomas reclasificados:")
print(df_completo["bioma_reclasificado"].value_counts(dropna=False).head(10))