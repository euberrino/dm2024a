# Cargar las bibliotecas necesarias
require(data.table)
require(missForest)
library(doParallel)

# Cargar el dataset
dataset <- fread("/home/eberrino/buckets/b1/expw/CA-0001/dataset.csv.gz")

# Definir las variables a imputar
to_impute_variables <- c(
  "ctrx_quarter",
  "mpasivos_margen",
  "mrentabilidad_annual",
  "mactivos_margen",
  "mtransferencias_recibidas",
  "mtarjeta_visa_consumo",
  "Visa_mfinanciacion_limite",
  "chomebanking_transacciones",
  "mrentabilidad"
)

# Fijar la semilla para reproducibilidad
set.seed(123)

# Seleccionar una muestra del dataset
n <- 5000
sampled_rows <- dataset[sample(.N, n)]

# Filtrar las columnas a imputar
sampled_rows <- sampled_rows[, ..to_impute_variables]

# Inicializar una lista para almacenar los resultados
results_list <- list()

# Configurar la paralelización
num_cores <- detectCores() - 2
cl <- makeCluster(num_cores)
registerDoParallel(cl)

# Valores de ntree para probar
ntree_values <- c(10, 20, 30, 50)

# Inicializar una data.table para almacenar los resultados
results_dt <- data.table(ntree = integer(), estimated_error = numeric(), time_elapsed = numeric())

# Iterar sobre los valores de ntree
for (ntree in ntree_values) {
  cat("Imputando con", ntree, "árboles\n")
  
  # Medir el tiempo de inicio
  start_time <- Sys.time()
  
  # Aplicar missForest con el valor actual de ntree
  imputed_data <- missForest(sampled_rows,
                             verbose = TRUE,
                             ntree = ntree,
                             parallelize = "forests")
  
  # Medir el tiempo de finalización
  end_time <- Sys.time()
  
  # Calcular el tiempo transcurrido
  time_elapsed <- as.numeric(difftime(end_time, start_time, units = "secs"))
  
  # Extraer el error estimado
  estimated_error <- imputed_data$OOBerror
  
  # Agregar los resultados a la data.table
  results_dt <- rbind(results_dt, data.table(ntree = ntree, estimated_error = estimated_error, time_elapsed = time_elapsed))
  
  # Imprimir los resultados en la consola
  cat("ntree:", ntree, "- Estimated Error:", estimated_error, "- Time Elapsed (seconds):", time_elapsed, "\n")
}

# Detener el clúster
stopCluster(cl)

# Guardar los resultados finales en un archivo CSV
fwrite(results_dt, file = "imputation_results_summary.csv")

cat("Imputación completada para todos los valores de ntree. Resultados guardados en 'imputation_results_summary.csv'.\n")