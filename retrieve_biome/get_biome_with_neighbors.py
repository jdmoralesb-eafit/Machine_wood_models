import pandas as pd
import rasterio
import os
import numpy as np
from multiprocessing import Pool, freeze_support
from tqdm import tqdm
from scipy.spatial import cKDTree
import time

class ProgressTracker:
    def __init__(self, total_records):
        self.total = total_records
        self.processed = 0
        self.exact_matches = 0
        self.approx_matches = 0
        self.no_matches = 0
        self.start_time = time.time()
        self.last_update = 0
        self.update_interval = 5  # segundos entre actualizaciones

    def update(self, exact, approx, no_match, batch_size):
        self.processed += batch_size
        self.exact_matches += exact
        self.approx_matches += approx
        self.no_matches += no_match
        
        current_time = time.time()
        if current_time - self.last_update >= self.update_interval:
            self.last_update = current_time
            self.print_progress()

    def print_progress(self):
        elapsed = time.time() - self.start_time
        records_per_sec = self.processed / elapsed if elapsed > 0 else 0
        remaining_time = (self.total - self.processed) / records_per_sec if records_per_sec > 0 else 0
        
        print("\n" + "="*50)
        print(f"Progreso: {self.processed}/{self.total} ({self.processed/self.total*100:.2f}%)")
        print(f"Velocidad: {records_per_sec:.2f} registros/segundo")
        print(f"Tiempo estimado restante: {remaining_time/60:.2f} minutos")
        print("\nEstadísticas parciales:")
        print(f"- Biomas exactos: {self.exact_matches} ({self.exact_matches/self.processed*100:.2f}%)")
        print(f"- Biomas aproximados: {self.approx_matches} ({self.approx_matches/self.processed*100:.2f}%)")
        print(f"- Sin bioma: {self.no_matches} ({self.no_matches/self.processed*100:.2f}%)")
        print("="*50 + "\n")

def obtener_bioma(lon, lat, tif_path):
    """Obtiene el bioma para una coordenada, con búsqueda del vecino más cercano si es necesario"""
    with rasterio.open(tif_path) as src:
        # Primero intentamos obtener el bioma exacto
        try:
            row_idx, col_idx = src.index(lon, lat)
            if 0 <= row_idx < src.height and 0 <= col_idx < src.width:
                # Leer solo la ventana necesaria
                window = rasterio.windows.Window(col_off=col_idx, row_off=row_idx, width=1, height=1)
                value = src.read(1, window=window)[0, 0]
                if value != src.nodata:
                    return value, (lon, lat)  # Bioma exacto encontrado
        except:
            pass
        
        # Si no hay bioma exacto, buscamos en los píxeles circundantes
        search_radius = 0.01  # Aprox. 1km (ajusta según tu CRS)
        max_search_radius = 1.0  # Radio máximo de búsqueda (ajusta según necesites)
        
        while search_radius <= max_search_radius:
            # Crear una ventana de búsqueda alrededor del punto
            try:
                min_row, min_col = src.index(lon - search_radius, lat - search_radius)
                max_row, max_col = src.index(lon + search_radius, lat + search_radius)
                
                # Asegurarnos de que estamos dentro de los límites del raster
                min_row = max(0, min_row)
                min_col = max(0, min_col)
                max_row = min(src.height - 1, max_row)
                max_col = min(src.width - 1, max_col)
                
                window = rasterio.windows.Window(col_off=min_col, row_off=min_row,
                                               width=max_col-min_col+1, height=max_row-min_row+1)
                
                data = src.read(1, window=window)
                rows, cols = np.where(data != src.nodata)
                
                if len(rows) > 0:
                    # Encontramos píxeles válidos, tomar el más cercano
                    transform = src.window_transform(window)
                    valid_coords = []
                    
                    for row, col in zip(rows, cols):
                        px_lon, px_lat = transform * (col + min_col, row + min_row)
                        valid_coords.append([px_lon, px_lat])
                    
                    if valid_coords:
                        tree = cKDTree(valid_coords)
                        dist, idx = tree.query([lon, lat], k=1)
                        nearest_coord = valid_coords[idx]
                        
                        # Obtener el valor del píxel más cercano
                        row_idx, col_idx = src.index(nearest_coord[0], nearest_coord[1])
                        window = rasterio.windows.Window(col_off=col_idx, row_off=row_idx, width=1, height=1)
                        nearest_bioma = src.read(1, window=window)[0, 0]
                        
                        return nearest_bioma, nearest_coord
                
            except:
                pass
            
            # Incrementar el radio de búsqueda si no encontramos nada
            search_radius *= 2
        
        return None, (lon, lat)

def procesar_lote(batch, tif_path):
    """Procesa un lote de registros y devuelve resultados y estadísticas"""
    resultados = []
    stats = {'exact': 0, 'approx': 0, 'none': 0}
    
    for _, row in batch.iterrows():
        species = row['species']
        lon = row['longitude']
        lat = row['latitude']
        
        bioma, ref_coords = obtener_bioma(lon, lat, tif_path)
        es_aproximado = ref_coords != (lon, lat)
        
        if bioma is None:
            stats['none'] += 1
        elif es_aproximado:
            stats['approx'] += 1
        else:
            stats['exact'] += 1
            
        resultados.append({
            'species': species,
            'longitude': lon,
            'latitude': lat,
            'bioma_asignado': bioma,
            'es_aproximado': es_aproximado,
            'ref_longitude': ref_coords[0],
            'ref_latitude': ref_coords[1]
        })
    
    return resultados, stats

def main():
    # Configuración inicial
    output_folder = 'retrieve_biome'
    os.makedirs(output_folder, exist_ok=True)
    
    # Cargar datos
    print("Cargando datos de entrada...")
    df = pd.read_csv('retrieve_biome/coordenadas_todas_las_especies.csv', low_memory=False)
    tif_path = 'retrieve_biome/HLZ_Level3.tif'
    
    # Verificar existencia de archivos
    if not os.path.exists(tif_path):
        print(f"\n❌ Error: No se encontró el archivo raster {tif_path}")
        return
    
    # Inicializar tracker de progreso
    progress = ProgressTracker(len(df))
    
    # Procesamiento en lotes
    batch_size = 500
    results = []
    file_counter = 0
    
    # Creamos los lotes
    batches = [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]
    
    # Procesamos con barra de progreso
    with tqdm(total=len(df), desc="Procesando registros", unit="rec") as pbar:
        for batch in batches:
            batch_result, batch_stats = procesar_lote(batch, tif_path)
            results.extend(batch_result)
            progress.update(
                exact=batch_stats['exact'],
                approx=batch_stats['approx'],
                no_match=batch_stats['none'],
                batch_size=len(batch_result)
            )
            pbar.update(len(batch_result))
            
            # Guardar resultados parciales periódicamente
            if len(results) >= 5000:
                partial_df = pd.DataFrame(results)
                partial_path = os.path.join(output_folder, f'biomas_parcial_{file_counter}.csv')
                partial_df.to_csv(partial_path, index=False)
                results = []
                file_counter += 1
    
    # Guardar resultados finales
    final_df = pd.DataFrame(results)
    final_path = os.path.join(output_folder, 'biomas_reclasificados_completos_final.csv')
    final_df.to_csv(final_path, index=False)
    
    # Reporte final
    progress.print_progress()
    print("\n✅ Proceso completado exitosamente!")
    print(f"Archivo final guardado en: {final_path}")

if __name__ == '__main__':
    freeze_support()
    main()