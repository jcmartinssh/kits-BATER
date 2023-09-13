library(sf)
library(stringr)
library(arrow)
library(dplyr)
library(tidyr)
library(data.table)

filtro_gpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
                      2, 2,
                      byrow = TRUE
)

filtro_parquet <- matrix(c("Parquet", "*.parquet", "All files", "*"),
                     2, 2,
                     byrow = TRUE
)

# escolhe diretrório de saída
# output <- selectDirectory(caption = "diretório de saída:", label = "Select", path = "./data/")
output <- tcltk::tk_choose.dir(caption = "selecionar diretório de saída:")

# seleciona o arquivo geopackage com a base áreas de risco
# AR_gpkg <- selectFile(caption = "selecionar arquivo de áreas de risco:", label = "Select")
AR_gpkg <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de áreas de risco:",
  multi = FALSE,
  filters = filtro_gpkg
)

# seleciona o arquivo geopackage com a base de faces com variáveis de mapeamento
# tabfaces_gpkg <- selectFile(caption = "selecionar arquivo de variáveis de faces:", label = "Select")
tabfaces_gpkg <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de variáveis de faces:",
  multi = FALSE,
  filters = filtro_gpkg
)

# seleciona o arquivo geopackage com a base de setores censitários
# setores_gpkg <- selectFile(caption = "selecionar arquivo de setores:", label = "Select")
setores_gpkg <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de setores:",
  multi = FALSE,
  filters = filtro_gpkg
)

# seleciona o arquivo geopackage com a base de faces com geometria
# faces_gpkg <- selectFile(caption = "selecionar arquivo de base de faces:", label = "Select")
faces_gpkg <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de base de faces::",
  multi = FALSE,
  filters = filtro_gpkg
)

faces_aval_parquet <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de avaliação das faces com geometria:",
  multi = FALSE,
  filters = filtro_parquet
)

municipios_gpkg <- tcltk::tk_choose.files(
  caption = "selecionar municípios:",
  multi = FALSE,
  filters = filtro_gpkg
)


# cria lista de camadas para cada arquivo de base
lotes <- st_layers(AR_gpkg)
setores_ver <- st_layers(setores_gpkg)
faces_ver <- st_layers(faces_gpkg)
tabfaces_ver <- st_layers(tabfaces_gpkg)
municipios_ver <- st_layers(municipios_gpkg)

# seleciona a camada para cada arquivo geopackage de base
lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "dados de faces:", graphics = TRUE)
setor_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)
municipios_layer <- select.list(municipios_ver[[1]], title = "base de municípios:", graphics = TRUE)


# cria lista de colunas e seleciona a de interesse para cada camada base

# áreas de risco - geocódigo do município
col_AR <- read_sf(AR_gpkg, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()
cod_AR <- select.list(col_AR, title = "código dos municípios:", graphics = TRUE)

# setores censitários - geocódigo dos setores 
col_set <- read_sf(setores_gpkg, query = paste("SELECT * from ", setor_layer, " LIMIT 0", sep = "")) |> colnames()
cod_set <- select.list(col_set, title = "código dos setores:", graphics = TRUE)

# faces (geometria) - geocódigo das faces
col_face <- read_sf(faces_gpkg, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_face <- select.list(col_face, title = "geocódigo das faces:", graphics = TRUE)
id_face <- select.list(col_face, title = "identificador único das faces:", graphics = TRUE)

# faces (dados) - geocódigo das faces
col_tabface <- read_sf(tabfaces_gpkg,  query = paste("SELECT * from ", tabfaces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_tabface <- select.list(col_tabface, title = "código das faces", graphics = TRUE)

# faces (avaliacao) - geocódigo das faces
col_faces_aval <- open_dataset(faces_aval_parquet)$schema$names
cod_faces_aval <-  select.list(col_faces_aval, title = "código das faces", graphics = TRUE)

col_mun <- read_sf(municipios_gpkg, query = paste("SELECT * from ", municipios_layer, " LIMIT 0", sep = "")) |> colnames()
cod_mun <-  select.list(col_mun, title = "código dos municípios", graphics = TRUE)
#
# tem que resolver a questão dos municípios novos não presentes na base de 2010 - são só dois.
#

# cria lista de municípios das áreas de risco
lista_mun <- read_sf(AR_gpkg,
                     query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |> pull() |> as.character()

# para testar
lista_mun <- lista_mun[5:7]

### agora que começam as paradas

# lista para armazenar informações sobre a associação de dados com geometria das faces
aval_quali <- list()

# carrega o arquivo parquet de avaliacao das faces com geometria como dataset, para lazy evaluation
faces_aval_geo <- open_dataset(faces_aval_parquet)

teste <- faces_aval_geo |> filter(substr(get(cod_faces_aval), 1, 7) == lista_mun[i]) |> collect()
# teste2 <- faces_aval_geo |> collect() |> filter(substr(CD_GEO, 1, 7) == lista_mun[i])

# prooduz padrao de erro no geocodigo das faces
cod_erro <- lapply(0:9, rep, 3) |> lapply(paste, collapse = "") |> unlist()

# anotar o tempo decorrido
inicio <- Sys.time()

##
###
#### loop de criação de kits por município
for (i in seq_along(lista_mun)) {
  # carrega o municipio, repara a geometria e converte para WKT
  municipio <- st_read(municipios_gpkg, query = paste("SELECT * FROM ", municipios_layer, " WHERE ", cod_mun, " = '", lista_mun[i], "'", sep = "")) |> st_make_valid()
  
  mun_geom <- municipio |> st_geometry() |> st_as_text()
  
  # carrega as áreas de risco do município
  areas_risco <- st_read(AR_gpkg, query = paste("SELECT * FROM ", lote_layer, " WHERE ", cod_AR, " = '", lista_mun[i], "'", sep = ""))
  
  # carrega os setores do município
  setores <- st_read(setores_gpkg, query = paste("SELECT * FROM ", setor_layer, " WHERE substr(", cod_set, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  #
  # tem que carregar as variáveis de mapeamento nos setores 
  #
  # carrega as faces com geometria do município
  # faces_geo <- read_sf(faces_gpkg, query = paste("SELECT * FROM ", faces_layer, " WHERE substr(", cod_face, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  faces_geo <- st_read(faces_gpkg, layer = faces_layer, wkt_filter = mun_geom)
  
  faces_id <- faces_geo[id_face] |> pull(get(id_face)) |> as.character() #|> paste(collapse = ", ")
  
  # carrega a tabela de avaliacao das faces com geometria
  faces_aval <- faces_aval_geo |> filter(ID %in% faces_id) |> select(ID, status_geo) |> collect()
  
  # junta a geometria com a avaliacao
  faces <- full_join(faces_geo, faces_aval, by = "ID")
  
  rm(faces_geo, faces_aval)
  gc()

  # carrega a tabela de variáveis das faces do município
  tabela_faces <- st_read(tabfaces_gpkg, query = paste("SELECT * FROM ", tabfaces_layer, " WHERE substr(", cod_tabface, ", 1, 7) = '", lista_mun[i], "'", sep = "")) |> setDT()
  
  olcol <- colnames(tabela_faces)[8:31]
  neocol <- paste("V", str_pad(1:24, 3, pad = "0"), sep = "")
  setnames(tabela_faces, olcol, neocol)
  
  (
    tabela_faces
    [, CONT := .N, by = X4]
    [is.na(X4), status_tab := "geocodigo nulo"] # nenhuma face nessa condicao
    [!is.na(X4) & (nchar(X4) != 21 | substr(X4, 19, 21) %in% cod_erro), status_tab := "erro geocodigo"] # 830540 faces nessa condicao
    [!is.na(X4) & !(status_tab %in% c("geocodigo nulo", "erro geocodigo")) & CONT > 1, status_tab := "geocodigo duplicado"]
    [!is.na(X4) & nchar(X4) == 21 & !(substr(X4, 19, 21) %in% cod_erro) & CONT == 1, status_tab := "perfeita"]
  )

  
  # junta a geometria com os dados das faces - mantendo todas as feições das duas camadas
  faces <- full_join(faces, tabela_faces, by = setNames(nm = cod_face, cod_tabface), keep = TRUE)
  
  rm(tabela_faces)
  gc()
  
  # preenche a lista de avaliação com os dados do município - faces associadas e não associadas
  aval_quali[[i]] <- data.frame(
    # código do município
    municipio = lista_mun[i],
    
    # áreas de risco
    areas_de_risco = areas_risco |> nrow(),
    
    # faces com geometria e variáveis
    faces_com_dado = faces |> filter(status_geo == "perfeita" & status_tab == "perfeita") |> nrow(),
    
    # faces sem variáveis
    faces_sem_dado = faces |> filter(!(is.na(status_geo)) & status_tab != "perfeita") |> nrow(),
    # faces_sem_dado = sum(is.na(faces[cod_tabface])),
    
    #faces sem geometria 
    faces_sem_geo = faces |> filter(status_geo != "perfeita" & !(is.na(status_tab))) |> nrow()
    )
  
  # remove as faces sem geometria da camada
  faces <- faces |> drop_na(status_geo)
  
  # faz a classificacao final das faces
  faces <- faces |> mutate(status_final = if_else(status_geo == "perfeita" & (!(is.na(status_tab)) & status_tab == "perfeita"), "com dados", "sem dados"))
  
  # cria diretório de saída
  dir.create(paste(output, "/", lista_mun[i], sep = ""))
  
  # nome raiz de saída
  saida <- paste(output, "/kits/", lista_mun[i], "/", lista_mun[i], sep = "")
  
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

