# Instalar paquetes si no están instalados
if (!require("BIEN")) install.packages("BIEN")
if (!require("sf")) install.packages("sf")
if (!require("dplyr")) install.packages("dplyr")
if (!require("readxl")) install.packages("readxl")
if (!require("tidyr")) install.packages("tidyr")
if (!require("readr")) install.packages("readr")

# Cargar librerías
library(BIEN)
library(sf)
library(dplyr)
library(readxl)
library(tidyr)
library(readr)

# 📍 Configuración de archivos
input_xlsx <- "obtencion_data/lista_especies_filtrada.csv"
output_csv <- "obtencion_data/species_coordinates.csv"
not_found_txt <- "not_found_species.txt"

# 1. Cargar lista de especies
species_list <- read_csv(input_xlsx) %>% 
  pull(1) %>% 
  unique() %>% 
  na.omit()

# 2. Función para obtener ocurrencias de BIEN con validaciones
get_bien_occurrences <- function(species) {
  tryCatch({
    # Verificar si la especie existe en la taxonomía de BIEN
    taxonomy <- BIEN_taxonomy_species(species)
    if (nrow(taxonomy) == 0) {
      message(paste0("⚠️ ", species, " no encontrado en la taxonomía de BIEN"))
      return(NULL)
    }
    
    # Obtener ocurrencias georreferenciadas
    occs <- BIEN_occurrence_species(
      species = species,
      cultivated = FALSE,
      only.geovalid = TRUE
    )
    
    if (nrow(occs) == 0) {
      message(paste0("⚠️ ", species, " no tiene registros georreferenciados en BIEN"))
      return(NULL)
    }
    
    # Verificar y seleccionar columnas disponibles
    col_rename <- c(
      "scrubbed_species_binomial" = "species",
      "longitude" = "longitude",
      "latitude" = "latitude"
    )
    
    available_cols <- intersect(names(col_rename), colnames(occs))
    
    if (length(available_cols) < 3) {
      message(paste0("❌ Falta(n) columna(s) en ", species, ": ", paste(setdiff(names(col_rename), available_cols), collapse = ", ")))
      return(NULL)
    }
    
    coords <- occs %>%
      select(all_of(available_cols)) %>%
      rename_with(~ col_rename[.x], all_of(available_cols))
    
    return(coords)
    
  }, error = function(e) {
    message(paste0("❌ Error con ", species, ": ", e$message))
    return(NULL)
  })
}

# 3. Configurar archivos de salida (creando copia del original si existe)
if(file.exists(output_csv)) {
  # Crear nombre para el archivo de continuación
  continuation_csv <- gsub("\\.csv$", "_continuation.csv", output_csv)
  
  # Copiar el archivo original si no existe la copia de continuación
  if(!file.exists(continuation_csv)) {
    file.copy(output_csv, continuation_csv)
    message(paste("✂️ Se creó copia del archivo original como:", continuation_csv))
  }
  
  # Leer datos existentes para evitar duplicados
  existing_data <- read_csv(continuation_csv, show_col_types = FALSE)
  existing_species <- unique(existing_data$species)
} else {
  continuation_csv <- output_csv
  existing_species <- character()
  existing_data <- tibble()
}

# 4. Procesar especies comenzando desde Sparattosperma leucanthum
results <- list()
not_found <- character()

# Encontrar posición de inicio
start_index <- which(species_list == "Sparattosperma leucanthum")
if(length(start_index) == 0) start_index <- 1  # Usar posición numérica si no encuentra el nombre

message(paste("♻️ Reanudando proceso desde la posición", start_index, "de", length(species_list)))

for(i in start_index:length(species_list)) {
  species <- species_list[i]
  message(paste0("\n🌿 Procesando (", i, "/", length(species_list), "): ", species))
  
  # Saltar especies ya procesadas
  if(species %in% existing_species) {
    message(paste("⏩", species, "ya existe en los datos - omitiendo"))
    next
  }
  
  occs <- get_bien_occurrences(species)
  
  if(is.null(occs) || nrow(occs) == 0) {
    not_found <- c(not_found, species)
    next
  }
  
  results[[species]] <- occs
  
  # Guardar progreso cada 5 especies
  if(i %% 5 == 0) {
    message("💾 Guardando progreso...")
    new_data <- bind_rows(results) 
    if(nrow(existing_data) > 0) {
      combined_data <- bind_rows(existing_data, new_data)
    } else {
      combined_data <- new_data
    }
    write_csv(combined_data, continuation_csv)
    writeLines(not_found, not_found_txt)
  }
}

# 5. Guardar resultados finales
final_new_data <- bind_rows(results)
if(nrow(existing_data) > 0) {
  final_combined <- bind_rows(existing_data, final_new_data)
} else {
  final_combined <- final_new_data
}

write_csv(final_combined, continuation_csv)
writeLines(not_found, not_found_txt)

message("\n✅ Proceso completado:")
message(paste("• Total de especies en archivo:", length(unique(final_combined$species))))
message(paste("• Nuevas especies añadidas:", length(unique(final_new_data$species))))
message(paste("• Especies no encontradas:", length(not_found)))