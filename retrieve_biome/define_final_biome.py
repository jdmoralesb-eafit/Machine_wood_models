import pandas as pd

def determinar_bioma_final(umbral_porcentaje=60):
    try:
        # 1. Cargar el archivo con los porcentajes
        df = pd.read_csv("retrieve_biome/ocurrencias_por_bioma_con_porcentaje_4_etiquetas.csv")
        
        # 2. Identificar columnas de porcentaje
        porcent_cols = [col for col in df.columns if col.startswith('PORCENTAJE_')]
        
        # 3. Determinar bioma final para cada especie
        df['bioma_final'] = None
        
        for idx, row in df.iterrows():
            max_porcent = row[porcent_cols].max()
            if max_porcent > umbral_porcentaje:
                # Encontrar el bioma con el porcentaje máximo
                bioma = [col.replace('PORCENTAJE_', '') 
                         for col in porcent_cols 
                         if row[col] == max_porcent][0]
                df.at[idx, 'bioma_final'] = bioma
        
        # 4. Guardar resultados
        output_path = "retrieve_biome/ocurrencias_con_bioma_final_60.csv"
        df.to_csv(output_path, index=False)
        
        print(f"\nArchivo guardado en: {output_path}")
        print(f"Total especies: {len(df)}")
        print(f"Especies con bioma definido (>{umbral_porcentaje}%): {df['bioma_final'].notna().sum()}")
        print("\nDistribución de biomas finales:")
        print(df['bioma_final'].value_counts(dropna=False))
        
        return df

    except Exception as e:
        print(f"\nError en el procesamiento: {str(e)}")
        return None

if __name__ == "__main__":
    # Puedes ajustar el umbral porcentual (por defecto 60%)
    determinar_bioma_final(umbral_porcentaje=60)