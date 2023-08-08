# <- isso indica um comentário

## protótipo do script de automatização 
##  - pré-processamento dos dados da DI,
##  - processamento com bases 
##  - criação dos kits para delimitação da BATER

## carrega a bibioteca "Read Rectangular Text Data" para leitura de tabelas e dados matriciais
library(readr)

## carrega a biblioteca "Simple Features" para geoprocessamento.
## ela utiliza algumas bibliotecas consagradas, como o GDAL, e é útil até para abrir arquivos de formatos variados
library(sf)

## carrega a biblioteca de API do Rstudio para utilizar os diálogos de seleçào de arquivo
library(rstudioapi)

# define o modo de teste
teste = FALSE
## roda o comando readr::fwf_empty para detectar automaticamente as quebras das colunas a partir de um único arquivo.
## esse arquivo deve garantir a existência de pelo menos um valor em cada coluna. Não utilize o Distrito Federal.
scanfwf <- fwf_empty(selectFile(caption = "Selecionar arquivo com conteúdo típico", filter = "CSV files (*.csv)"))

## endereço da pasta raiz onde estão as subpastas com os arquivos de entrada e de saída
# pastaRaiz <- ifelse(teste == TRUE, "C:\\ACELERADOR\\Lote5\\teste\\", "C:\\ACELERADOR\\Lote5\\")
pasta_entrada <- selectDirectory()

## cria a lista de arquivos lendo o conteúdo da pasta dos arquivos originais.
## o parâmetro pattern permite definir um string para restringir os arquivos de interesse.
## nesse caso, todos os arquivos de interesse, e somente eles, devem conter .csv no nome
lista_arq <- list.files(paste(pasta_entrada, sep = ""), pattern = ".csv")

lista_arq[1:3]
## reduz a lista de arquivos de entrada para rodar o teste
if(teste == TRUE) {
  lista_arq <- lista_arq[1:2]
}

## endereço do geopackage onde serão salvos as camadas e tabelas
# arquivo_saida <- paste(pastaRaiz, "_GPKG\\lote5_faces.gpkg", sep = "" )
arquivo_saida <- selectFile(caption = "Arquivo de saída - geopackage", label = "Save", filter = "GPKG files (*.gpkg)")

## cria lista vazia pra receber numero de linhas de cada tabela processada
lista_total <- list()

## loop de processamento dos arquivos da DI
for(i in lista_arq) {
  
  # constrói o endereço do arquivo de entrada
  arquivo_entrada <- paste(pasta_entrada, "//", i, sep = "")
  
  # lê o arquivo da DI de largura fixa, utilizando os parâmetros computados anteriormente
  # define os tipos das colunas usando a notação compacta: c = character; n = numeric
  tabela_faces <- read_fwf(arquivo_entrada, col_positions = scanfwf, col_types = "cccccccnnnnnnnnnnnnnnnnnnnnnnnn")
  
  # adiciona uma coluna com o nome do arquivo de origem para controle
  tabela_faces$origem <- i
  
  # adiciona o item com o nome do arquivo e total de faces na tabela para controle
  lista_total[[i]] <- nrow(tabela_faces)
  
  # acrescenta a tabela na camada indicada dentro do gpkg, sem apagar o que já existe
  write_sf(tabela_faces, arquivo_saida, layer = "dados_faces_lote5", append = TRUE)
  
}

## adiciona item total com valor do somatório dos items dos arquivos
lista_total[["total"]] <- sum(unlist(lista_total, use.names = FALSE))

## conta o número de feições na camada produzida usando SQL
ver_geral <- read_sf(arquivo_saida,
                       query = "SELECT count(*) as total FROM dados_faces_lote5")


## conta o número de feições para cada arquivo na camada produzida usando SQL
ver_arq <- read_sf(arquivo_saida,
                       query = "SELECT origem, count(*) as total FROM dados_faces_lote5 GROUP BY origem")

## carrega os primeiros 50 elementos da camada produzida para verificação
columns <- read_sf(arquivo_saida,
                   query = "SELECT * FROM dados_faces_lote5 LIMIT 50")

## cria a lista de Municípios a partir do geocódigo das faces do arquivo
lista_mun <- read_sf(arquivo_saida,
               query = "SELECT DISTINCT SUBSTR(X1, 1, 7) as geomun, count(*) as total_faces FROM dados_faces_lote5 GROUP BY geomun")

