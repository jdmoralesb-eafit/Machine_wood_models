import pandas as pd

def unir_tropical_con_biomas_final():
    try:
        print("📂 Cargando archivos...")
        
        # 1. Cargar archivo TropicalAnatomyBiome
        df_tropical = pd.read_csv("Modelos_conjunto_datos_inicial/TropicalAnatomyBiome.csv")
        
        # 2. Cargar archivo de ocurrencias con bioma final
        df_biomas = pd.read_csv("retrieve_biome/ocurrencias_con_bioma_final.csv")
        
        # 3. Limpieza de columnas en TropicalAnatomyBiome
        columnas_a_eliminar = [
            'sp_clean', 'species', 'error_relativo', 
            'info_bioma', 'ranking_bioma', 'species_clean'
        ]
        df_tropical = df_tropical.drop(
            columns=[col for col in columnas_a_eliminar if col in df_tropical.columns], 
            errors='ignore'
        )
        
        print("\nDatos de TropicalAnatomy cargados y limpiados")
        print(f"Columnas restantes: {list(df_tropical.columns)}")
        
        # 4. Realizar LEFT JOIN (manteniendo Tropical como tabla principal)
        print("\n🔗 Uniendo archivos (Tropical como base)...")
        df_combinado = df_tropical.merge(
            df_biomas[['species', 'bioma_final']],
            left_on='sp',
            right_on='species',
            how='left'
        ).drop(columns=['species'])  # Eliminar columna duplicada
        
        # 5. Aplicar reglas especiales de asignación de biomas
        print("\n🔧 Aplicando reglas especiales de biomas...")
        
        # Regla 1: Si 'gen' está vacío -> Tropical dry forest
        cond_gen_vacio = df_combinado['gen'].isna() | (df_combinado['gen'].str.strip() == '')
        df_combinado.loc[cond_gen_vacio, 'bioma_final'] = 'Tropical dry forest'
        
        # Regla 2: Especies específicas -> Mangrove
        especies_manglar = [
            'Laguncularia racemosa', 
            'Rhizophora mangle',
            'Conocarpus erectus', 
            'Avicennia germinans'
        ]
        cond_especies_manglar = df_combinado['sp'].str.strip().str.lower().isin(
            [s.lower() for s in especies_manglar]
        )
        df_combinado.loc[cond_especies_manglar, 'bioma_final'] = 'Mangrove'
        
        # 6. Análisis de la combinación
        print("\n🔍 Análisis de combinación:")
        print(f"- Registros en Tropical Anatomy: {len(df_tropical)}")
        print(f"- Registros combinados: {len(df_combinado)}")
        
        # Estadísticas después de aplicar reglas
        print("\n📊 Estadísticas de asignación de biomas:")
        print(f"- Asignados por gen vacío: {cond_gen_vacio.sum()}")
        print(f"- Asignados como Mangrove: {cond_especies_manglar.sum()}")
        
        match_stats = df_combinado['bioma_final'].notna().value_counts(normalize=True) * 100
        print(f"\n- Porcentaje con bioma asignado: {match_stats.get(True, 0):.2f}%")
        print(f"- Porcentaje sin bioma asignado: {match_stats.get(False, 0):.2f}%")
        
        # 7. Mostrar ejemplos de datos combinados
        print("\n🔬 Muestra de datos combinados:")
        print(df_combinado.head(3).to_markdown(index=False))
        
        # 8. Guardar resultados
        archivo_salida = "retrieve_biome/tropical_con_biomas_final_v2.csv"
        df_combinado.to_csv(archivo_salida, index=False)
        
        # 9. Guardar especies sin match para análisis
        sin_match = df_combinado[df_combinado['bioma_final'].isna()]['sp'].unique()
        pd.Series(sin_match).to_csv(
            "retrieve_biome/especies_tropical_sin_bioma_final_v2.csv", 
            index=False
        )
        
        print(f"\n💾 Resultados guardados:")
        print(f"- Datos completos: '{archivo_salida}'")
        print(f"- Especies sin match: 'retrieve_biome/especies_tropical_sin_bioma_final_v2.csv'")
        
        # 10. Distribución de biomas finales
        print("\n🌍 Distribución de biomas finales:")
        print(df_combinado['bioma_final'].value_counts(dropna=False).to_markdown())
        
        return df_combinado

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return None

if __name__ == "__main__":
    resultado = unir_tropical_con_biomas_final()
    if resultado is not None:
        print("\n✅ ¡Proceso completado exitosamente!")