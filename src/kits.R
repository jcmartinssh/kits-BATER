# script para producao das pastas contendos os arquivos necessarios para mapeamento da BATER por municipio

library(sf)
# library(rstudioapi)
library(stringr)
library(dplyr)
library(tidyr)
library(data.table)
library(arrow) # nao da pra tirar, ver QUESTAO DOS METODOS: SF (GDAL) / ARROW
library(rlang)

#
# tem que resolver a questão dos municípios novos não presentes na base de 2010 - são só dois.
#

############################################
## QUESTAO DOS METODOS: SF (GDAL) / ARROW ##
############################################

## Tentei carregar as faces avaliadas utilizando o sf (gdal), mas para inserir a lista numa clausula IN tive que quebrar a lista em listas
## com numero menor de elementos separados por OR. Esse metodo aparentemente conseguiu evitar o problema de limite de caracteres no GDAL, mas
## ficou processando eternamente. Tive que retornar o codigo usando a biblioteca ARROW e sua interface com DPLYR.

# funcoes para processar lista de ids de maneira a inserir em query SQL
# sem estourar limite de elementos na clausula IN

# sql_list <- function(character_vector, seq_block, block_size) {
#   if (seq_block == (length(character_vector) %/% block_size) + 1) {
#     group <- paste(
#       character_vector[(((seq_block - 1) * block_size) + 1):(((seq_block - 1) * block_size) + (length(character_vector) %% 1000))],
#       collapse = ", "
#     )
#   } else {
#     group <- paste(
#       character_vector[(((seq_block - 1) * block_size) + 1):(seq_block * block_size)],
#       collapse = ", "
#     )
#   }
#   text <- paste("(", group, ")", sep = "")
#   return(text)
# }

# sql_huge_list <- function(character_vector, block_size, col_id) {
#   lista_id <- list()
#   for (i in 1:((length(character_vector) %/% block_size) + 1)) {
#     lista_id[i] <- sql_list(character_vector, i, block_size)
#   }
#   text <- paste(lista_id, collapse = paste(" OR ", col_id, " IN "))
#   return(text)
# }

############################################
## QUESTAO DOS METODOS: SF (GDAL) / ARROW ##
############################################

# definicao dos filtros para os tipos de arquivos - opcional.
# comentar se desejar carregar de outro formato, p. ex. shapefile
filtro_gpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
  2, 2,
  byrow = TRUE
)

filtro_parquet <- matrix(c("Parquet", "*.parquet", "All files", "*"),
  2, 2,
  byrow = TRUE
)

# desativa a engine para calculos geometricos esfericos - acelera o processamento
# sf_use_s2(FALSE)

# seleciona qual operacao vai realizar
# para avaliar sem produzir os kits
op1 <- "Produzir os kits e tabela de avaliação das faces"

op2 <- "Produzir somente a tabela de avaliação das faces"

proc <- select.list(c(op1, op2), preselect = op2, multiple = FALSE, title = "Seleção de procedimento", graphics = TRUE)

if (proc == op1) {
  prod_kits <- TRUE
} else {
  prod_kits <- FALSE
}

# escolhe diretrório de saída
# output <- selectDirectory(caption = "diretório de saída:", label = "Select", path = "./data/")
output <- tcltk::tk_choose.dir(caption = "selecionar diretório de saída:")

# seleciona a tabela de faces pre avaliadas em formato parquet
faces_aval_arq <- tcltk::tk_choose.files(
  caption = "selecionar tabela de faces avaliadas:",
  multi = FALSE,
  filters = filtro_parquet
)

# seleciona o arquivo geopackage com a base áreas de risco
AR_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de áreas de risco:", multi = FALSE,
  filters = filtro_parquet
)

# seleciona o arquivo geopackage com a base de faces com variáveis de mapeamento
tabfaces_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de variáveis de faces:", multi = FALSE,
  filters = filtro_parquet
)

# seleciona o arquivo geopackage com a base de setores censitários
setores_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de setores:", multi = FALSE,
  filters = filtro_parquet
)

# seleciona o arquivo geopackage com a base de faces com geometria
# faces_gpkg <- tcltk::tk_choose.files(caption = "selecionar arquivo de base de faces:", multi = FALSE)
faces_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de base de faces:",
  multi = FALSE,
  filters = filtro_parquet
)

# seleciona o arquivo da base de municipios
municipios_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de base de municípios:",
  multi = FALSE,
  filters = filtro_parquet
)

# cria lista de camadas para cada arquivo de base
faces_geo_aval <- st_layers(faces_aval_arq)
lotes <- st_layers(AR_arq)
setores_ver <- st_layers(setores_arq)
faces_ver <- st_layers(faces_arq)
tabfaces_ver <- st_layers(tabfaces_arq)
municipios_ver <- st_layers(municipios_arq)

# seleciona a camada para cada arquivo geopackage de base
faces_aval_layer <- select.list(faces_geo_aval[[1]], title = "faces avaliadas:", graphics = TRUE)
lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "dados de faces:", graphics = TRUE)
setor_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)
municipios_layer <- select.list(municipios_ver[[1]], title = "base de municípios:", graphics = TRUE)

# cria lista de colunas e seleciona as de interesses para cada camada base
# faces avaliadas - identificador unico
# faces (avaliacao) - geocódigo das faces
# col_faces_aval <- open_dataset(faces_aval_parquet)$schema$names
col_faces_aval <- read_sf(faces_aval_arq, query = paste("SELECT * from ", faces_aval_layer, " LIMIT 0", sep = "")) |> colnames()
id_faces_aval <- select.list(col_faces_aval, title = "identificador único das faces avaliadas:", graphics = TRUE)
# cod_faces_aval <- select.list(col_faces_aval, title = "geocódigo das faces", graphics = TRUE)


# áreas de risco - geocódigo do município
col_AR <- read_sf(AR_arq, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()
cod_AR <- select.list(col_AR, title = "código dos municípios:", graphics = TRUE)

# setores censitários - geocódigo dos setores
col_set <- read_sf(setores_arq, query = paste("SELECT * from ", setor_layer, " LIMIT 0", sep = "")) |> colnames()
cod_set <- select.list(col_set, title = "código dos setores:", graphics = TRUE)

# faces (geometria) - geocódigo das faces
col_face <- read_sf(faces_arq, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_face <- select.list(col_face, title = "geocódigo das faces:", graphics = TRUE)
id_face <- select.list(col_face, title = "identificador único da base de faces:", graphics = TRUE)

# faces (dados) - geocódigo das faces
col_tabface <- read_sf(tabfaces_arq, query = paste("SELECT * from ", tabfaces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_tabface <- select.list(col_tabface, title = "código das faces", graphics = TRUE)

# municipio - geocódigo
col_mun <- read_sf(municipios_arq, query = paste("SELECT * from ", municipios_layer, " LIMIT 0", sep = "")) |> colnames()
cod_mun <- select.list(col_mun, title = "código dos municípios:", graphics = TRUE)
nom_mun <- select.list(col_mun, title = "nome dos municípios:", graphics = TRUE)

############################################
## QUESTAO DOS METODOS: SF (GDAL) / ARROW ##
############################################

# carrega o arquivo parquet de avaliacao das faces com geometria como dataset, para lazy evaluation
# nao consegui substituir pelo pacote sf para remover dependencia no arrow
faces_aval_geo <- open_dataset(faces_aval_arq)

############################################
## QUESTAO DOS METODOS: SF (GDAL) / ARROW ##
############################################

# produz padrao de erro no geocodigo das faces
cod_erro <- lapply(0:9, rep, 3) |>
  lapply(paste, collapse = "") |>
  unlist()

### agora que começam as paradas

# cria lista de municípios das áreas de risco
lista_mun_tot <- read_sf(AR_arq, query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |>
  pull() |>
  as.character()

# para testar
lista_mun <- lista_mun_tot[145:146]
# i <- 1

# lista para armazenar informações sobre a associação de dados com geometria das faces
aval_quali <- list()

# anotar o tempo decorrido
inicio <- Sys.time()


##
###
#### loop de criação de kits por município
for (i in seq_along(lista_mun)) {
  # carrega o municipio, repara a geometria e converte para WKT
  municipio <- st_read(municipios_arq, query = paste("SELECT * FROM ", municipios_layer, " WHERE ", cod_mun, " = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

  mun_geom <- municipio |>
    st_geometry() |>
    st_as_text()

  # carrega as áreas de risco do município
  areas_risco <- st_read(AR_arq, query = paste("SELECT * FROM ", lote_layer, " WHERE ", cod_AR, " = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

  # carrega os setores do município
  setores <- st_read(setores_arq, query = paste("SELECT * FROM ", setor_layer, " WHERE substr(", cod_set, ", 1, 7) = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

  # carrega as faces com geometria do município - tá com algum problema
  # faces_geo <- read_sf(faces_gpkg, query = paste("SELECT * FROM ", faces_layer, " WHERE substr(", cod_face, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  faces_geo <- st_read(faces_arq, layer = faces_layer, wkt_filter = mun_geom) |>
    st_make_valid()

  # cria lista de id de faces em formato sql
  faces_id <- faces_geo[id_face] |>
    pull(get(id_face)) |>
    as.character()

  # cria lista de ids das faces em SQL separados em grupos de 1000 elementos - metodo pelo GDAL
  # separados por OR para evitar estourar limites da clausula IN
  # faces_id_sql <- sql_huge_list(faces_id, 1000, id_face)

  # carrega a tabela de avaliacao da base de faces - metodo pelo GDAL
  # faces_aval <- st_read(
  #   faces_aval_parquet,
  #   query = paste("SELECT * FROM ", faces_aval_layer, " WHERE ", id_faces_aval, " IN ", faces_id_sql, sep = "")
  # )

  # expressao de filtro para usar com ARROW (por causa de bug)
  filter_exp <- paste(id_faces_aval, " %in% c(", paste(faces_id, collapse = ", "), ")", sep = "")

  # carrega as faces avaliadas
  faces_aval <- faces_aval_geo |>
    filter(!!rlang::parse_expr(filter_exp)) |>
    select(!!id_faces_aval, status_geo) |>
    collect()

  # junta a geometria com a avaliacao
  # o campo id esta hardcoded, mudar para a variavel que recebe o nome da coluna
  faces <- full_join(faces_geo, faces_aval, by = join_by(!!id_face == !!id_faces_aval), na_matches = "never")
  # faces <- full_join(faces_geo, faces_aval, by = c(!!id_face, !!id_faces_aval), na_matches = "never")
  rm(faces_geo, faces_aval)
  gc()

  # carrega a tabela de variáveis das faces do município e nomeia as colunas com as variaveis agregadas a BATER
  tabela_faces <- st_read(tabfaces_arq, query = paste("SELECT * FROM ", tabfaces_layer, " WHERE substr(", cod_tabface, ", 1, 7) = '", lista_mun[i], "'", sep = "")) |> setDT()

  # pega o nome das colunas com variavies das faces CNEFE
  olcol <- colnames(tabela_faces)[9:32]

  # cria os codigos das variaveis da publicacao proceduralmente
  neocol <- paste("V", str_pad(1:24, 3, pad = "0"), sep = "")

  # substitui o nome das colunas pelos da publicacao
  setnames(tabela_faces, olcol, neocol)

  # cria a tabela com as variaveis agregadas por setor censitário
  tabela_setor <- tabela_faces |>
    mutate({{ cod_set }} := str_sub(.data[[cod_tabface]], 1, 15)) |> # essa porra e muito louca
    summarise(across(all_of(neocol), sum), .by = {{ cod_set }})

  # classifica as faces segundo os erros encontrados no geocodigo da tabela de faces CNEFE
  (
    tabela_faces
    [, CONT := .N, by = X4]
    [is.na(X4), status_tab := "geocodigo nulo"] # nenhuma face nessa condicao
    [!is.na(X4) & (nchar(X4) != 21 | substr(X4, 19, 21) %in% cod_erro), status_tab := "erro geocodigo"] # 830540 faces nessa condicao
    [!is.na(X4) & !(status_tab %in% c("geocodigo nulo", "erro geocodigo")) & CONT > 1, status_tab := "geocodigo duplicado"]
    [!is.na(X4) & nchar(X4) == 21 & !(substr(X4, 19, 21) %in% cod_erro) & CONT == 1, status_tab := "perfeita"]
  )


  # junta a geometria com os dados das faces CNEFE - mantendo todas as feições das duas camadas
  # faces <- full_join(faces, tabela_faces, by = setNames(nm = cod_face, cod_tabface), keep = TRUE)
  faces <- full_join(faces, tabela_faces, by = join_by(!!cod_face == !!cod_tabface), keep = TRUE)

  rm(tabela_faces)
  gc()

  # faz a classificacao final das faces
  faces <- faces |>
    mutate(
      status_final = case_when(
        is.na(status_geo) & status_tab == "erro geocodigo" ~ "sem associacao geo",
        is.na(status_geo) & status_tab == "geocodigo duplicado" ~ "sem associacao geo",
        is.na(status_geo) & status_tab == "perfeita" ~ "sem associacao geo",
        status_geo == "duplicadas" & is.na(status_tab) ~ "sem associacao tab",
        status_geo == "invalidas" & is.na(status_tab) ~ "erro geocodigo",
        status_geo == "perfeitas" & is.na(status_tab) ~ "sem associacao tab",
        status_geo == "recuperadas" & is.na(status_tab) ~ "sem associacao tab",
        status_geo == "duplicadas" & status_tab == "geocodigo duplicado" ~ "duplicada",
        status_geo == "duplicadas" & status_tab == "perfeita" ~ "geo duplicada",
        status_geo == "invalidas" & status_tab == "erro geocodigo" ~ "erro geocodigo",
        status_geo == "invalidas" & status_tab == "geocodigo duplicado" ~ "erro geocodigo",
        status_geo == "invalidas" & status_tab == "perfeita" ~ "erro geocodigo geo",
        status_geo == "perfeitas" & status_tab == "erro geocodigo" ~ "erro geocodigo tab",
        status_geo == "perfeitas" & status_tab == "geocodigo duplicado" ~ "tab duplicada",
        status_geo == "perfeitas" & status_tab == "perfeita" ~ "perfeita",
        .default = "outro"
      )
    )

  # preenche a lista de avaliação com os dados do município - faces associadas e não associadas
  aval_quali[[i]] <- data.frame(
    # código do município
    CD_GEOCODM = lista_mun[i],

    # áreas de risco
    areas_de_risco = areas_risco |>
      nrow(),

    # faces com geometria e variáveis
    faces_com_dado = faces |>
      filter((!(is.na(status_tab)) & status_geo == "perfeita") & (!(is.na(status_tab)) & status_tab == "perfeita")) |>
      nrow(),

    # faces sem variáveis
    faces_sem_dado = faces |>
      filter((status_geo == "perfeita" & (status_tab != "perfeita" | is.na(status_tab))) | status_geo != "perfeita") |>
      nrow(),

    # faces com dado em área de risco
    faces_com_dado_AR = faces |>
      st_filter(areas_risco, .predicate = st_intersects) |>
      filter(status_geo == "perfeita" & status_tab == "perfeita") |>
      nrow(),

    # faces sem variáveis em área de risco
    faces_sem_dado_AR = faces |>
      st_filter(areas_risco, .predicate = st_intersects) |>
      filter((status_geo == "perfeita" & (status_tab != "perfeita" | is.na(status_tab))) | status_geo != "perfeita") |>
      nrow(),

    # faces sem geometria
    faces_sem_geo = faces |> filter(status_geo != "perfeita" & !(is.na(status_tab))) |>
      nrow()
  )

  if (prod_kits == TRUE) {
    # remove as faces sem geometria da camada
    # acho melhor deixar, fica na tabela como feicao sem geometria
    # faces <- faces |> drop_na(status_geo)

    # associa as variaveis aos setores censitarios
    setores <- setores |>
      left_join(tabela_setor, by = {{ cod_set }})

    # cria diretório de saída
    dir.create(paste(output, "/kits", sep = ""))
    dir.create(paste(output, "/kits/", lista_mun[i], sep = ""))

    # nome raiz de saída
    saida <- paste(output, "/kits/", lista_mun[i], "/", lista_mun[i], sep = "")

    # exporta as áreas de risco
    write_sf(areas_risco, paste(saida, "_areas_risco", ".shp", sep = ""))

    # exporta os setores
    write_sf(setores, paste(saida, "_setores", ".shp", sep = ""))

    # exporta as faces de logradouro
    write_sf(faces, paste(saida, "_faces", ".shp", sep = ""))
  }

  # finaliza a tabela de avalia
}
#### FIM DO LOOP
###
##
#

fim <- Sys.time()
tempo <- fim - inicio

sql_mun <- paste("'", lista_mun, "'", sep = "", collapse = ", ")

nm_municipios <- st_read(municipios_arq, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", municipios_layer, " WHERE ", cod_mun, " in (", sql_mun, ")", sep = ""))

# consolida a lista de tabelas de avaliação das faces por município em uma unica tabela e adiciona os nomes
aval_quali <- bind_rows(aval_quali)
aval_quali <- left_join(nm_municipios, aval_quali, by = join_by(!!cod_mun))

# exporta a tabela de avaliação
write_sf(aval_quali, dsn = paste(output, "/avaliacao.ods", sep = ""))

# obs <- faces |>
#   select(ID, CD_GEO, status_geo, status_tab, status_final)

################################################
### ANALIZANDO CATEGORIAS DE ERROS NAS FACES ###
################################################
# comentar antes de rodar uma primeira vez
# verificando combinacoes de status


# faces_geo_tot <- open_dataset(faces_arq)

# faces_tab_geo_cols <- setdiff(col_face, "geometry")

# faces_tab_geo <- faces_geo_tot |>
#   select(!!faces_tab_geo_cols) |>
#   collect()

# faces_tab_aval_geo <- faces_aval_geo |>
#   select(!!id_faces_aval, status_geo) |>
#   collect()

# tabela_faces_db <- open_dataset(tabfaces_arq)

# tabela_faces <- tabela_faces_db |>
#   collect()

# setDT(tabela_faces)

# (
#   tabela_faces
#   [, CONT := .N, by = X4]
#   [is.na(X4), status_tab := "geocodigo nulo"] # nenhuma face nessa condicao
#   [!is.na(X4) & (nchar(X4) != 21 | substr(X4, 19, 21) %in% cod_erro), status_tab := "erro geocodigo"] # 830540 faces nessa condicao
#   [!is.na(X4) & !(status_tab %in% c("geocodigo nulo", "erro geocodigo")) & CONT > 1, status_tab := "geocodigo duplicado"]
#   [!is.na(X4) & nchar(X4) == 21 & !(substr(X4, 19, 21) %in% cod_erro) & CONT == 1, status_tab := "perfeita"]
# )

# class_faces <- faces_tab_geo |>
#   full_join(
#     tabela_faces,
#     by = join_by(!!cod_face == !!cod_tabface),
#     na_matches = "never",
#     relationship = "many-to-many"
#   ) |>
#   full_join(
#     faces_tab_aval_geo,
#     by = join_by(!!id_face == !!id_faces_aval),
#     na_matches = "never",
#     relationship = "many-to-many"
#   ) |>
#   mutate(
#     status_final = case_when(
#       is.na(status_geo) & status_tab == "erro geocodigo" ~ "sem associacao geo",
#       is.na(status_geo) & status_tab == "geocodigo duplicado" ~ "sem associacao geo",
#       is.na(status_geo) & status_tab == "perfeita" ~ "sem associacao geo",
#       status_geo == "duplicadas" & is.na(status_tab) ~ "sem associacao tab",
#       status_geo == "invalidas" & is.na(status_tab) ~ "erro geocodigo",
#       status_geo == "perfeitas" & is.na(status_tab) ~ "sem associacao tab",
#       status_geo == "recuperadas" & is.na(status_tab) ~ "sem associacao tab",
#       status_geo == "duplicadas" & status_tab == "geocodigo duplicado" ~ "duplicada",
#       status_geo == "duplicadas" & status_tab == "perfeita" ~ "geo duplicada",
#       status_geo == "invalidas" & status_tab == "erro geocodigo" ~ "erro geocodigo",
#       status_geo == "invalidas" & status_tab == "geocodigo duplicado" ~ "erro geocodigo",
#       status_geo == "invalidas" & status_tab == "perfeita" ~ "erro geocodigo geo",
#       status_geo == "perfeitas" & status_tab == "erro geocodigo" ~ "erro geocodigo tab",
#       status_geo == "perfeitas" & status_tab == "geocodigo duplicado" ~ "tab duplicada",
#       status_geo == "perfeitas" & status_tab == "perfeita" ~ "perfeita",
#       .default = "outro"
#     )
#   ) |>
#   summarise(n = n(), .by = status_final)

# geral <- class_faces |>
#   summarise(n = n(), .by = status_final)


# combinacoes <- class_faces |>
#   group_by(status_geo, status_tab) |>
#   summarise(n = n())

# st_write(combinacoes, dsn = paste(output, "/combinacoes_status_geo_tab.ods", sep = ""))

# # verificando faces recuperadas
# proc_recuperada <- faces_aval_geo |>
#   filter(status_geo == "recuperadas") |>
#   collect() |>
#   mutate(CD_MUN = str_sub(CD_GEO, 1, 7)) |>
#   summarise(n_faces_rec = n(), .by = CD_MUN)

# lista_mun <- lista_mun_tot[lista_mun_tot %in% proc_recuperada$CD_MUN]

# rec_list <- proc_recuperada |>
#   filter(CD_MUN %in% lista_mun)

################################################
### ANALIZANDO CATEGORIAS DE ERROS NAS FACES ###
################################################
