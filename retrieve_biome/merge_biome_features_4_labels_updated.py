import pandas as pd

def procesar_insidewood_con_biomas():
    try:
        print("📂 Cargando archivos...")
        
        # 1. Cargar archivo InsideWood
        df_insidewood = pd.read_csv("retrieve_biome/inside_wood_final_depurado.csv", sep=';', encoding='utf-8')
        
        # Eliminar bioma_final si existe en el archivo InsideWood
        if 'bioma_final' in df_insidewood.columns:
            df_insidewood = df_insidewood.drop(columns=['bioma_final'])
            print("🗑️ Columna 'bioma_final' eliminada del archivo InsideWood")
        
        # 2. Cargar archivo de ocurrencias con bioma final
        df_biomas = pd.read_csv("retrieve_biome/ocurrencias_con_bioma_final.csv")
        
        # 3. Limpieza de columnas en InsideWood
        columnas_a_eliminar = [
            'sp_clean', 'species', 'error_relativo', 
            'info_bioma', 'ranking_bioma', 'species_clean'
        ]
        df_insidewood = df_insidewood.drop(
            columns=[col for col in columnas_a_eliminar if col in df_insidewood.columns], 
            errors='ignore'
        )
        
        print("\nDatos de InsideWood cargados y limpiados")
        print(f"Columnas restantes: {list(df_insidewood.columns)}")
        
        # 4. Realizar LEFT JOIN (manteniendo InsideWood como tabla principal)
        print("\n🔗 Uniendo archivos (InsideWood como base)...")
        df_combinado = df_insidewood.merge(
            df_biomas[['species', 'bioma_final']],
            left_on='Species_1',  # Columna con nombre científico en insidewood
            right_on='species',
            how='left'
        ).drop(columns=['species'])  # Eliminar columna duplicada
        
        # 5. Aplicar reglas especiales de asignación de biomas (sin manglares)
        print("\n🔧 Aplicando reglas especiales de biomas...")
        
        # Regla 1: Si Source es Humboldt -> Tropical dry forest (MÁXIMA PRIORIDAD)
        cond_humboldt = df_combinado['Source'].str.strip().str.lower() == 'humboldt'
        df_combinado.loc[cond_humboldt, 'bioma_final'] = 'Tropical dry forest'
        
        # 6. Análisis de la combinación
        print("\n🔍 Análisis de combinación:")
        print(f"- Registros en InsideWood: {len(df_insidewood)}")
        print(f"- Registros combinados: {len(df_combinado)}")
        
        # Estadísticas después de aplicar reglas
        print("\n📊 Estadísticas de asignación de biomas:")
        print(f"- Asignados por ser Humboldt: {cond_humboldt.sum()}")
        
        match_stats = df_combinado['bioma_final'].notna().value_counts(normalize=True) * 100
        print(f"\n- Porcentaje con bioma asignado: {match_stats.get(True, 0):.2f}%")
        print(f"- Porcentaje sin bioma asignado: {match_stats.get(False, 0):.2f}%")
        
        # 7. Mostrar ejemplos de datos combinados
        print("\n🔬 Muestra de datos combinados:")
        print(df_combinado.head(3).to_markdown(index=False))
        
        # 8. Guardar resultados
        archivo_salida = "retrieve_biome/insidewood_con_biomas_final.csv"
        df_combinado.to_csv(archivo_salida, index=False, sep=';', encoding='utf-8')
        
        # 9. Guardar especies sin match para análisis
        sin_match = df_combinado[df_combinado['bioma_final'].isna()]['Species_1'].unique()
        pd.Series(sin_match).to_csv(
            "retrieve_biome/especies_insidewood_sin_bioma_final.csv", 
            index=False
        )
        
        print(f"\n💾 Resultados guardados:")
        print(f"- Datos completos: '{archivo_salida}'")
        print(f"- Especies sin match: 'retrieve_biome/especies_insidewood_sin_bioma_final.csv'")
        
        # 10. Distribución de biomas finales
        print("\n🌍 Distribución de biomas finales:")
        print(df_combinado['bioma_final'].value_counts(dropna=False).to_markdown())
        
        return df_combinado

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return None

if __name__ == "__main__":
    resultado = procesar_insidewood_con_biomas()
    if resultado is not None:
        print("\n✅ ¡Proceso completado exitosamente!")