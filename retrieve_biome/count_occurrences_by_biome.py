import pandas as pd

def calcular_ocurrencias_y_porcentajes():
    try:
        # 1. Cargar y limpiar datos
        df = pd.read_csv("retrieve_biome/biomas_reclasificados_completos_v2.csv")
        
        print("\nLimpieza de datos:")
        print(f"Registros iniciales: {len(df)}")
        
        # Eliminar NaN y valores vacíos
        df = df.dropna(subset=['bioma_reclasificado'])
        df['bioma_reclasificado'] = df['bioma_reclasificado'].astype(str).str.strip()
        df = df[df['bioma_reclasificado'] != '']
        
        print(f"Registros válidos: {len(df)}")
        
        # 2. Calcular conteos y porcentajes
        conteo = df.value_counts(['species', 'bioma_reclasificado']).reset_index(name='conteo')
        total_por_especie = conteo.groupby('species')['conteo'].transform('sum')
        conteo['porcentaje'] = (conteo['conteo'] / total_por_especie * 100).round(2)
        
        # 3. Crear tabla pivotada para conteos
        conteos_pivot = conteo.pivot(
            index='species',
            columns='bioma_reclasificado',
            values='conteo'
        ).fillna(0).add_prefix('CONTEOS_')
        
        # 4. Crear tabla pivotada para porcentajes
        porcentajes_pivot = conteo.pivot(
            index='species',
            columns='bioma_reclasificado',
            values='porcentaje'
        ).fillna(0).add_prefix('PORCENTAJE_')
        
        # 5. Combinar ambos resultados
        resultado_final = pd.concat([conteos_pivot, porcentajes_pivot], axis=1)
        
        # Ordenar columnas: primero todos los conteos, luego todos los porcentajes
        column_order = sorted(resultado_final.columns, 
                            key=lambda x: ('PORCENTAJE' in x, x))
        resultado_final = resultado_final[column_order]
        
        # 6. Agregar totales por especie
        resultado_final['TOTAL_OCURRENCIAS'] = conteos_pivot.sum(axis=1)
        
        # 7. Guardar resultados
        output_path = "retrieve_biome/ocurrencias_por_bioma_con_porcentaje.csv"
        resultado_final.to_csv(output_path)
        
        # 8. Generar reporte
        print("\nResultados guardados en:", output_path)
        print("\nResumen estadístico:")
        print(f"- Especies únicas: {len(resultado_final)}")
        print(f"- Biomas considerados: {len(conteo['bioma_reclasificado'].unique())}")
        print("\nEjemplo de resultados:")
        print(resultado_final.iloc[:5, :6].to_markdown())
        
        return resultado_final

    except Exception as e:
        print(f"\nError en el procesamiento: {str(e)}")
        return None

if __name__ == "__main__":
    calcular_ocurrencias_y_porcentajes()