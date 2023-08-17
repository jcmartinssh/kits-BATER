library(sf)
library(rstudioapi)
library(dplyr)
library(tidyr)

# escolhe diretrório de saída
output <- selectDirectory(caption = "diretório de saída:", label = "Select", path = "./data/")

# seleciona o arquivo geopackage com a base áreas de risco
AR_gpkg <- selectFile(caption = "selecionar arquivo de áreas de risco:", label = "Select")

# seleciona o arquivo geopackage com a base de faces com variáveis de mapeamento
tabfaces_gpkg <- selectFile(caption = "selecionar arquivo de variáveis de faces:", label = "Select")

# seleciona o arquivo geopackage com a base de setores censitários
setores_gpkg <- selectFile(caption = "selecionar arquivo de setores:", label = "Select")

# seleciona o arquivo geopackage com a base de faces com geometria
faces_gpkg <- selectFile(caption = "selecionar arquivo de base de faces:", label = "Select")


# cria lista de camadas para cada arquivo de base
lotes <- st_layers(AR_gpkg)
setores_ver <- st_layers(setores_gpkg)
faces_ver <- st_layers(faces_gpkg)
tabfaces_ver <- st_layers(tabfaces_gpkg)

# seleciona a camada para cada arquivo de base
lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "dados de faces:", graphics = TRUE)
setor_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)

# cria lista de colunas e seleciona a de interesse para cada camada base

# áreas de risco - geocódigo do município
col_AR <- read_sf(AR_gpkg, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()
cod_AR <- select.list(col_AR, title = "código dos municípios:", graphics = TRUE)

# setores censitários - geocódigo dos setores 
col_set <- read_sf(setores_gpkg, query = paste("SELECT * from ", setor_layer, " LIMIT 0", sep = "")) |> colnames()
cod_set <- select.list(col_set, title = "código dos setores:", graphics = TRUE)

# faces (geometria) - geocódigo das faces
col_face <- read_sf(faces_gpkg, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_face <- select.list(col_face, title = "código das faces:", graphics = TRUE)

# faces (dados) - geocódigo das faces
col_tabface <- read_sf(tabfaces_gpkg,  query = paste("SELECT * from ", tabfaces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_tabface <- select.list(col_tabface, title = "código das faces", graphics = TRUE)

#
# tem que resolver a questão dos municípios novos não presentes na base de 2010 - são só dois.
#

# cria lista de municípios das áreas de risco
lista_mun <- read_sf(AR_gpkg,
                     query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |> pull() |> as.character()

# para testar
lista_mun <- lista_mun[1:10]

### agora que começam as paradas

# lista para armazenar informações sobre a associação de dados com geometria das faces
aval_quali <- list()

# anotar o tempo decorrido
inicio <- Sys.time()

#
##
###
#### loop de criação de kits por município
for (i in 1:length(lista_mun)) {
  
  # carrega as áreas de risco do município
  areas_risco <- read_sf(AR_gpkg, query = paste("SELECT * FROM ", lote_layer, " WHERE ", cod_AR, " = '", lista_mun[i], "'", sep = ""))
  
  # carrega os setores do município
  setores <- read_sf(setores_gpkg, query = paste("SELECT * FROM ", setor_layer, " WHERE substr(", cod_set, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  #
  # tem que carregar as variáveis de mapeamento nos setores 
  #
  
  # carrega as faces com geometria do município
  faces_geo <- read_sf(faces_gpkg, query = paste("SELECT * FROM ", faces_layer, " WHERE substr(", cod_face, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  
  # carrega a tabela de variáveis das faces do município
  tabela_faces <- read_sf(tabfaces_gpkg, query = paste("SELECT * FROM ", tabfaces_layer, " WHERE substr(", cod_tabface, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  
  # junta a geometria com os dados das faces - mantendo todas as feições das duas camadas
  faces <- full_join(faces_geo, tabela_faces, by = setNames(nm = cod_face, cod_tabface), keep = TRUE)
  
  # preenche a lista de avaliação com os dados do município - faces associadas e não associadas
  aval_quali[[i]] <- data.frame(
    # código do município
    municipio = lista_mun[i],
    # áreas de risco
    areas_de_risco = areas_risco |> nrow(),
    # faces com geometria e variáveis
    faces_com_dado = faces |> drop_na(cod_face, cod_tabface) |> nrow(),
    # faces sem variáveis
    faces_sem_dado = sum(is.na(faces[cod_tabface])),
    #faces sem geometria
    faces_sem_geo = sum(is.na(faces[cod_face])))
  
  # remove as faces sem geometria da camada
  faces <- faces |> drop_na(cod_face)
  
  # cria diretório de saída
  dir.create(paste(output, "/", lista_mun[i], sep = ""))
  
  # nome raiz de saída
  saida <- paste(output, "/", lista_mun[i], "/", lista_mun[i], sep = "")
  
  # exporta as áreas de risco
  write_sf(areas_risco, paste(saida, "_areas_risco", ".shp", sep = ""))
  
  # exporta os setores
  write_sf(setores, paste(saida, "_setores", ".shp", sep = ""))
  
  # exporta as faces de logradouro
  write_sf(faces, paste(saida, "_faces", ".shp", sep = ""))
}
#### FIM DO LOOP
###
##
#


fim <- Sys.time()
tempo <- fim - inicio

# consolida a lista de tabelas de avaliação das faces por município em uma úniica tabela
aval_quali <- bind_rows(aval_quali)

# exporta a tabela de avaliação
write_sf(aval_quali, dsn = paste(output, "/", "avaliacao.ods", sep = ""))

