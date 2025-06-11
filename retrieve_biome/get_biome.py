import pandas as pd
import rasterio
import os
import numpy as np
from multiprocessing import Pool, freeze_support
from tqdm import tqdm  # Importar tqdm para la barra de progreso

def obtener_bioma(lon, lat, tif_path):
    # Abrir el archivo raster dentro del proceso
    with rasterio.open(tif_path) as src:
        # Obtener el tamaño del raster
        width = src.width
        height = src.height

        # Convertir lon/lat a índices de fila y columna
        row_idx, col_idx = src.index(lon, lat)

        # Leer el valor del raster en la posición especificada
        value = None
        if 0 <= row_idx < height and 0 <= col_idx < width:
            # Leer un bloque pequeño alrededor del índice (para evitar errores de memoria)
            window = rasterio.windows.Window(col_off=col_idx, row_off=row_idx, width=1, height=1)
            value = src.read(1, window=window)[0, 0]
    
    return value

def procesar_fila(row, tif_path):
    species = row['species']
    lon = row['longitude']
    lat = row['latitude']
    region_ecologica = row['region_ecologica']

    if pd.isna(region_ecologica):
        bioma = None
    else:
        bioma = obtener_bioma(lon, lat, tif_path)

    return {
        'species': species,
        'longitude': lon,
        'latitude': lat,
        'bioma_asignado': bioma
    }

def main():
    # Crear carpeta si no existe
    output_folder = 'retrieve_biome'
    os.makedirs(output_folder, exist_ok=True)

    # Cargar el CSV
    df = pd.read_csv('retrieve_biome/coordenadas_todas_las_especies.csv')

    # Ruta al GeoTIFF
    tif_path = 'retrieve_biome/HLZ_Level3.tif'

    # Procesar datos por lotes y en paralelo usando multiprocessing
    batch_size = 1000
    results = []
    file_counter = 0

    # Multiprocessing Pool para procesar datos en paralelo
    with Pool() as pool:
        # Usamos map para distribuir el trabajo entre los núcleos
        for idx in tqdm(range(0, len(df), batch_size), desc="Procesando lotes", unit="lote"):
            batch = df.iloc[idx:idx + batch_size]
            batch_results = pool.starmap(procesar_fila, [(row, tif_path) for _, row in batch.iterrows()])
            results.extend(batch_results)

            # Guardar cada 10,000 registros
            if len(results) >= batch_size:
                partial_df = pd.DataFrame(results)
                partial_path = os.path.join(output_folder, f'biomas_{file_counter}.csv')
                partial_df.to_csv(partial_path, index=False)
                print(f"\nGuardado: {partial_path}")
                results = []
                file_counter += 1

        # Guardar cualquier resto que quede
        if results:
            partial_df = pd.DataFrame(results)
            partial_path = os.path.join(output_folder, f'biomas_{file_counter}.csv')
            partial_df.to_csv(partial_path, index=False)
            print(f"\nGuardado: {partial_path}")

    print("¡Proceso completo!")

if __name__ == '__main__':
    freeze_support()  # Solo necesario si el programa se va a congelar para crear un ejecutable
    main()
