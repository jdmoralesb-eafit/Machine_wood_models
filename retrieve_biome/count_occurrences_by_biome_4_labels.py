import pandas as pd

def procesar_biomas():
    try:
        # 1. Cargar datos principales
        df = pd.read_csv("retrieve_biome/biomas_reclasificados_completos_v2.csv")
        
        # 2. Cargar tabla de reclasificación
        reclasificador = pd.read_excel("etiquetas_para_reclasificar_biomas.xlsx", sheet_name="Sheet2")
        
        # Crear diccionario de reclasificación
        reclas_dict = reclasificador.set_index('original')['reclasificado_2'].to_dict()
        
        # 3. Limpieza de datos básica
        print("\nLimpieza de datos:")
        print(f"Registros iniciales: {len(df)}")
        
        df = df.dropna(subset=['bioma_reclasificado'])
        df['bioma_reclasificado'] = df['bioma_reclasificado'].astype(str).str.strip()
        df = df[df['bioma_reclasificado'] != '']
        
        print(f"Registros válidos: {len(df)}")
        
        # ------------------------------------------------------------
        # PRIMER ARCHIVO: Datos originales sin reclasificación
        # ------------------------------------------------------------
        # Calcular conteos y porcentajes para biomas originales
        conteo_original = df.value_counts(['species', 'bioma_reclasificado']).reset_index(name='conteo')
        total_por_especie_original = conteo_original.groupby('species')['conteo'].transform('sum')
        conteo_original['porcentaje'] = (conteo_original['conteo'] / total_por_especie_original * 100).round(2)
        
        # Crear tabla pivotada combinada para originales
        conteos_pivot_original = conteo_original.pivot(
            index='species',
            columns='bioma_reclasificado',
            values='conteo'
        ).fillna(0).add_prefix('CONTEOS_')
        
        porcentajes_pivot_original = conteo_original.pivot(
            index='species',
            columns='bioma_reclasificado',
            values='porcentaje'
        ).fillna(0).add_prefix('PORCENTAJE_')
        
        resultado_original = pd.concat([conteos_pivot_original, porcentajes_pivot_original], axis=1)
        resultado_original['TOTAL_OCURRENCIAS'] = conteos_pivot_original.sum(axis=1)
        
        # Ordenar columnas
        column_order_original = sorted(resultado_original.columns, 
                                     key=lambda x: ('PORCENTAJE' in x, x))
        resultado_original = resultado_original[column_order_original]
        
        # Guardar archivo original
        output_path_original = "retrieve_biome/ocurrencias_por_bioma_con_porcentaje.csv"
        resultado_original.to_csv(output_path_original)
        print(f"\nArchivo original guardado en: {output_path_original}")
        
        # ------------------------------------------------------------
        # SEGUNDO ARCHIVO: Datos reclasificados a 4 etiquetas (sin Temperate)
        # ------------------------------------------------------------
        # Aplicar reclasificación
        df['bioma_reclasificado_2'] = df['bioma_reclasificado'].map(reclas_dict).fillna('Otros')
        
        # Eliminar registros con bioma "Temperate"
        df_reclas = df[df['bioma_reclasificado_2'] != 'Temperate']
        
        print("\nBiomas reclasificados (excluyendo Temperate):")
        print(df_reclas['bioma_reclasificado_2'].value_counts())
        
        # Calcular conteos y porcentajes para reclasificado_2
        conteo_reclas = df_reclas.value_counts(['species', 'bioma_reclasificado_2']).reset_index(name='conteo')
        total_por_especie_reclas = conteo_reclas.groupby('species')['conteo'].transform('sum')
        conteo_reclas['porcentaje'] = (conteo_reclas['conteo'] / total_por_especie_reclas * 100).round(2)
        
        # Crear tabla pivotada combinada para reclasificados
        conteos_pivot_reclas = conteo_reclas.pivot(
            index='species',
            columns='bioma_reclasificado_2',
            values='conteo'
        ).fillna(0).add_prefix('CONTEOS_')
        
        porcentajes_pivot_reclas = conteo_reclas.pivot(
            index='species',
            columns='bioma_reclasificado_2',
            values='porcentaje'
        ).fillna(0).add_prefix('PORCENTAJE_')
        
        resultado_reclas = pd.concat([conteos_pivot_reclas, porcentajes_pivot_reclas], axis=1)
        resultado_reclas['TOTAL_OCURRENCIAS'] = conteos_pivot_reclas.sum(axis=1)
        
        # Ordenar columnas
        column_order_reclas = sorted(resultado_reclas.columns, 
                                   key=lambda x: ('PORCENTAJE' in x, x))
        resultado_reclas = resultado_reclas[column_order_reclas]
        
        # Guardar archivo reclasificado
        output_path_reclas = "retrieve_biome/ocurrencias_por_bioma_con_porcentaje_4_etiquetas.csv"
        resultado_reclas.to_csv(output_path_reclas)
        print(f"Archivo reclasificado guardado en: {output_path_reclas}")
        
        # ------------------------------------------------------------
        # Reporte final
        # ------------------------------------------------------------
        print("\nResumen estadístico:")
        print(f"- Especies únicas (original): {len(resultado_original)}")
        print(f"- Biomas originales: {len(conteo_original['bioma_reclasificado'].unique())}")
        print(f"- Especies únicas (reclasificado): {len(resultado_reclas)}")
        print(f"- Biomas reclasificados (excluyendo Temperate): {len(conteo_reclas['bioma_reclasificado_2'].unique())}")
        
        return {
            'resultado_original': resultado_original,
            'resultado_reclasificado': resultado_reclas
        }

    except Exception as e:
        print(f"\nError en el procesamiento: {str(e)}")
        return None

if __name__ == "__main__":
    procesar_biomas()