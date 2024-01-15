library(readr)
library(readODS)


pastaRaiz <- "/home/joaquim/Downloads/lote_5/"

nome_arq <- "teste_joaquim"

arquivo_entrada <- paste(pastaRaiz, nome_arq, ".txt", sep = "")

arquivo_saida <- paste(pastaRaiz, nome_arq, ".ods", sep = "")

scanfwf <- fwf_empty(arquivo_entrada)

tabela_faces <- read_fwf(arquivo_entrada, col_positions = scanfwf, col_types = "cccccccnnnnnnnnnnnnnnnnnnnnnnnn")

write_ods(tabela_faces, arquivo_saida)
