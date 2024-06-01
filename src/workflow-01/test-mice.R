# Cargar las librerías necesarias
install.packages("data.table")
library(data.table)
library(caret)
library(mice)
library(parallel)

# Leer el dataset (asegúrate de ajustar la ruta al archivo correcto)
dataset <- fread("/home/eberrino/buckets/b1/expw/CA-0020/dataset.csv.gz")
start_time <- Sys.time()
n <- 1000 # Número de columnas a seleccionar
sampled_rows <- dataset[sample(.N, n)]


# Definir las columnas a imputar
impute_columns <-  c(
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


data_impute <- sampled_rows[, ..impute_columns]


predictor_matrix <- quickpred(sampled_rows, mincor = 0.1, include = names(data_impute))
predictor_matrix[, impute_columns] <- 0
num_cores = detectCores() - 1
methods <- make.method(sampled_rows)
methods[impute_columns] <- "rf"
methods[setdiff(names(sampled_rows), impute_columns)] <- ""
imputed_data <- mice(sampled_rows, m = 5, method = methods, predictorMatrix = predictor_matrix, parallel = "snow", n.core = num_cores)
iter = 1
save(imputed_data,iter,methods,predictor_matrix,file="mice.RDATA")
print(paste0("saved iter",str(iter)))
load("mice.RDATA")
print("Checkpoint encontrado, reanudando la imputación.")
#correr otra iteracion para probar
imputed_data = mice.mids(imputed_data,  method = methods, predictorMatrix = predictor_matrix, 
                         parallel = "snow",maxit=1, n.core = num_cores )
iter = iter + 1
save(imputed_data,iter,methods,predictor_matrix,file="mice.RDATA")
print(paste0("saved iter",str(iter)))

completed_data <- complete(imputed_data)

# Reemplazar las columnas imputadas en el data.table original
sampled_rows[, (impute_columns) := completed_data[, impute_columns]]
end_time <- Sys.time()
execution_time <- end_time - start_time
print(paste("Tiempo de ejecución:", execution_time))
                                    