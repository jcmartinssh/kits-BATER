## SCRIPT para producao das pastas contendos os arquivos necessarios para mapeamento da BATER por municipio
## deve ser executado apos a correcao da base de areas de risco

############################
## configuracoes iniciais ##
############################

# variavel com hora de inicio para medir o tempo de execucao
inicio <- Sys.time()

# carrega as bibliotecas - versoes utilizadas anotadas em comentario
# R versao 4.3.2
library(sf)
library(stringr)
library(dplyr)
library(tidyr)
library(arrow)
library(rlang)
library(data.table)

# define diretorio de trabalho
# necessario para carregar as funcoes externas
# so funciona se estiver rodando o script inteiro
# em caso de rodar passo a passo, definir manualmente
setwd(
  getSrcDirectory(function(x) {
    x
  })
)

# carrega funcoes externas
source("funcoes_comuns.R")

# desativa geometria esferica
sf_use_s2(TRUE)

# produz padrao de erro no geocodigo das faces
cod_erro <- lapply(0:9, rep, 3) |>
  lapply(paste, collapse = "") |>
  unlist()

# # # define opcao de produzir kits e tabela de avaliacao
# # op1 <- "Produzir os kits e tabela de avaliação das faces"

# # # define opcao de produzir tabela de avaliacao sem produzir kits
# # op2 <- "Produzir somente a tabela de avaliação das faces"

# # # seleciona operacao a ser realizada
# # proc <- select.list(c(op1, op2), preselect = op2, multiple = FALSE, title = "Seleção de procedimento", graphics = TRUE)

# # if (proc == op1) {
# #   prod_kits <- TRUE
# # } else {
# #   prod_kits <- FALSE
# # }

# pra desativar a opcao de so produzir a tabela
prod_kits <- TRUE


################################################
## selecao de arquivos e colunas de interesse ##
################################################


# filtro de formatos de arquivos
filtro <- matrix(c("Geopackage", "*.gpkg", "Shapefile", "*.shp", "All files", "*"),
  3, 2,
  byrow = TRUE
)

# seleciona o diretrório de saída
output <- choose_directory(caption = "diretório de saída:")


################################
## faces - SISMAP - avaliacao ##


# seleciona a tabela de avaliacao da base de faces do SISMAP em formato Parquet
faces_aval_arq <- choose_file(
  caption = "arquivo de avaliação faces:",
  multi = FALSE,
  filters = matrix(c("Parquet", "*.parquet"),
    1, 2,
    byrow = TRUE
  )
)

# carrega o arquivo parquet de avaliacao das faces com geometria como dataset, para lazy evaluation.
faces_aval_geo <- open_dataset(faces_aval_arq)

# produz lista com nomes das colunas da camada
col_faces_aval <- faces_aval_geo$schema$names

# seleciona o campo com o identificador unico da avaliacao das faces do SISMAP
id_faces_aval <- select.list(col_faces_aval, title = "avaliação faces - ID:", graphics = TRUE)


####################
## areas de risco ##


# seleciona o arquivo com a base áreas de risco
AR_arq <- choose_file(
  caption = "arquivo de áreas de risco:", multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
lotes <- st_layers(AR_arq)

# seleciona a camada de interesse
lote_layer <- select.list(lotes[[1]], title = "base de áreas de risco:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_AR <- read_sf(AR_arq, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo municipal da area de risco
cod_AR <- select.list(col_AR, title = "AR - geocódigo município:", graphics = TRUE)


################################
## faces - SISMAP - geometria ##


# seleciona o arquivo com a base de faces com geometria do SISMAP
faces_arq <- choose_file(
  caption = "arquivo de faces - SISMAP:",
  multi = FALSE,
  filters = filtro
)

# # carrega o arquivo parquet de avaliacao das faces com geometria como dataset, para lazy evaluation.
# faces_sismap <- open_dataset(faces_aval_arq)

# # produz lista com nomes das colunas da camada
# col_face <- faces_sismap$schema$names

# cria lista de camadas do arquivo
faces_ver <- st_layers(faces_arq)

# seleciona a camada de interesse
faces_layer <- select.list(faces_ver[[1]], title = "base de faces SISMAP:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_face <- read_sf(faces_arq, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo das faces do SISMAP
cod_face <- select.list(col_face, title = "faces SISMAP - geocódigo:", graphics = TRUE)

# seleciona o campo com o identificador unico das faces do SISMAP
id_face <- select.list(col_face, title = "faces SISMAP – ID:", graphics = TRUE)


###################
## faces - CNEFE ##


# seleciona o arquivo de faces do CNEFE com variaveis de mapeamento
tabfaces_arq <- choose_file(
  caption = "arquivo das faces CNEFE:", multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
tabfaces_ver <- st_layers(tabfaces_arq)

# seleciona a camada de interesse
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "base de faces CNEFE:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_tabface <- read_sf(tabfaces_arq, query = paste("SELECT * from ", tabfaces_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo das faces do CNEFE
cod_tabface <- select.list(col_tabface, title = "faces CNEFE - geocódigo:", graphics = TRUE)

# seleciona o campo com primeira variavel de mapeamento das faces do CNEFE (V001)
p_var_tabface <- select.list(col_tabface, title = "faces CNEFE - V001:", graphics = TRUE)

# seleciona o campo com a ultima variavel de mapeamento das faces do CNEFE (V024)
u_var_tabface <- select.list(col_tabface, title = "faces CNEFE - V024:", graphics = TRUE)


#########################
## setores censitarios ##


# seleciona o arquivo com a base de setores censitários
setores_arq <- choose_file(
  caption = "arquivo dos setores:", multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
setores_ver <- st_layers(setores_arq)

# seleciona a camada de interesse
setor_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_set <- read_sf(setores_arq, query = paste("SELECT * from ", setor_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo dos setores censitarios
cod_set <- select.list(col_set, title = "setores – geocódigo:", graphics = TRUE)


################
## municipios ##


# seleciona o arquivo da base de municipios
municipios_arq <- choose_file(
  caption = "arquivo de municípios:",
  multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
municipios_ver <- st_layers(municipios_arq)

# seleciona a camada de interesse
municipios_layer <- select.list(municipios_ver[[1]], title = "base de municípios:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_mun <- read_sf(municipios_arq, query = paste("SELECT * from ", municipios_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo dos municipios
cod_mun <- select.list(col_mun, title = "municípios – geocódigo:", graphics = TRUE)

# seleciona o campo com o nome dos municipios
nom_mun <- select.list(col_mun, title = "municípios – nome:", graphics = TRUE)


###########################
## loop de processamento ##
##  e producao dos kits  ##
###########################


# cria lista de municípios das áreas de risco
lista_mun <- read_sf(AR_arq, query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |>
  pull() |>
  as.character()


# para testar
lista_mun <- lista_mun[145:146]
i <- 1

# carrega a camada completa das areas de risco
# evita problema de carregar por municipio e esses acabarem sem feicoes
areas_risco_tot <- st_read(AR_arq, layer = lote_layer) |>
  st_make_valid()

# cria lista vazia para armazenar informações sobre a associação de dados com geometria das faces
aval_quali <- list()


##
###
#### loop de criação de kits por município
for (i in seq_along(lista_mun)) {
  # carrega as áreas de risco do município pelo geocodigo e valida geometria
  areas_risco <- areas_risco_tot |>
    filter(.data[[cod_AR]] == lista_mun[i]) |>
    st_make_valid()

  # carrega os setores do município e valida geometria
  setores <- st_read(setores_arq, query = paste("SELECT * FROM ", setor_layer, " WHERE substr(", cod_set, ", 1, 7) = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

  # carrega o municipio e valida a geometria
  municipio <- st_read(municipios_arq, query = paste("SELECT * FROM ", municipios_layer, " WHERE ", cod_mun, " = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

  # converte a geometria do municipio para WKT para utilizacao como filtro espacial
  mun_geom <- municipio |>
    st_geometry() |>
    st_as_text()

  # carrega as faces do SISMAP com geometria do município e valida geometria
  faces_geo <- st_read(faces_arq, layer = faces_layer, wkt_filter = mun_geom) |>
    st_make_valid()

  # cria lista de id das faces do SISMAP em formato sql
  faces_id <- faces_geo[id_face] |>
    pull(get(id_face)) |>
    as.character()

  # produz expressao de filtro para usar com interface arrow
  filter_exp <- paste(id_faces_aval, " %in% c(", paste(faces_id, collapse = ", "), ")", sep = "")

  # carrega a avaliacao das faces do SISMAP utilizando a interface arrow
  faces_aval <- faces_aval_geo |>
    filter(!!rlang::parse_expr(filter_exp)) |>
    select(!!id_faces_aval, status_geo) |>
    collect()

  # junta as faces do SISMAP com geometria com a tabela de avaliacao da mesma
  faces <- full_join(faces_geo, faces_aval, by = join_by(!!id_face == !!id_faces_aval), na_matches = "never")
  rm(faces_geo, faces_aval)
  gc()

  # carrega a tabela de variáveis das faces do município e nomeia as colunas com as variaveis agregadas a BATER
  tabela_faces <- st_read(tabfaces_arq, query = paste("SELECT * FROM ", tabfaces_layer, " WHERE substr(", cod_tabface, ", 1, 7) = '", lista_mun[i], "'", sep = "")) |> setDT()

  # pega o nome das colunas com variavies das faces CNEFE
  # no arquivo do CNEFE
  olcol <- colnames(tabela_faces |> select({{ p_var_tabface }}:{{ u_var_tabface }}))

  # cria os codigos das variaveis da publicacao proceduralmente
  # no arquivo do CNEFE
  neocol <- paste("V", str_pad(seq_along(olcol), 3, pad = "0"), sep = "")

  # substitui o nome das colunas pelos da publicacao
  # no arquivo do CNEFE
  setnames(tabela_faces, olcol, neocol)

  # cria a tabela com as variaveis agregadas por setor censitário.
  # essa porra e muito louca, utilizar variaveis
  # do ambiente dentro do dplyr e um saco
  tabela_setor <- tabela_faces |>
    mutate({{ cod_set }} := str_sub(.data[[cod_tabface]], 1, 15)) |>
    summarise(across(all_of(neocol), sum), .by = {{ cod_set }})

  # classifica as faces do CNEFE segundo erros encontrados no geocodigo
  (
    tabela_faces
    [, CONT := .N, by = X4]
    [is.na(X4), status_tab := "geocodigo nulo"] # nenhuma face nessa condicao
    [!is.na(X4) & (nchar(X4) != 21 | substr(X4, 19, 21) %in% cod_erro), status_tab := "erro geocodigo"]
    [!is.na(X4) & !(status_tab %in% c("geocodigo nulo", "erro geocodigo")) & CONT > 1, status_tab := "geocodigo duplicado"]
    [!is.na(X4) & nchar(X4) == 21 & !(substr(X4, 19, 21) %in% cod_erro) & CONT == 1, status_tab := "perfeita"]
  )

  # junta as do SISMAP com as faces CNEFE - mantendo todas as feições das duas camadas
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
      filter(status_final %in% c("perfeita", "geo duplicada")) |>
      nrow(),

    # faces sem variáveis
    faces_sem_dado = faces |>
      filter(!(status_final %in% c("perfeita", "geo duplicada"))) |>
      nrow(),

    # faces com dado em área de risco
    faces_com_dado_AR = faces |>
      st_filter(areas_risco, .predicate = st_intersects) |>
      filter(status_final %in% c("perfeita", "geo duplicada")) |>
      nrow(),

    # faces sem variáveis em área de risco
    faces_sem_dado_AR = faces |>
      st_filter(areas_risco, .predicate = st_intersects) |>
      filter(!(status_final %in% c("perfeita", "geo duplicada"))) |>
      nrow(),

    # faces sem geometria
    faces_sem_geo = faces |> filter(is.na(status_geo) & !(is.na(status_tab))) |>
      nrow()
  )

  if (prod_kits == TRUE) {
    # associa as variaveis aos setores censitarios
    setores <- setores |>
      left_join(tabela_setor, by = {{ cod_set }})

    # cria diretório de saida principal
    dir.create(paste(output, "/kits", sep = ""))

    # cria diretorio de saida para o municipio
    dir.create(paste(output, "/kits/", lista_mun[i], sep = ""))

    # nome raiz dos arquivos de saida
    saida <- paste(output, "/kits/", lista_mun[i], "/", lista_mun[i], sep = "")

    # exporta as áreas de risco
    write_sf(areas_risco, paste(saida, "_areas_risco", ".shp", sep = ""))

    # exporta os setores
    write_sf(setores, paste(saida, "_setores", ".shp", sep = ""))

    # exporta as faces de logradouro
    write_sf(faces, paste(saida, "_faces", ".shp", sep = ""))
  }
}
#### FIM DO LOOP
###
##
#


###############################
## consolidacao e exportacao ##
##   da tabela de avaliacao  ##
###############################


# cria filtro sql geocodigo dos municipios
sql_mun <- paste("'", lista_mun, "'", sep = "", collapse = ", ")

# carrega a tabela dos municipios com geocodigo e nome oficial do IBGE
nm_municipios <- st_read(municipios_arq, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", municipios_layer, " WHERE ", cod_mun, " in (", sql_mun, ")", sep = ""))

# consolida a lista de tabelas de avaliação das faces por município em uma unica tabela
aval_quali <- bind_rows(aval_quali)

# adiciona os nomes oficiais dos municipios a tabela de avaliacao
aval_quali <- left_join(nm_municipios, aval_quali, by = join_by(!!cod_mun))

# exporta a tabela de avaliação
write_sf(aval_quali, dsn = paste(output, "/avaliacao.ods", sep = ""))

# variavel com hora do fim para medir o tempo de execucao
fim <- Sys.time()

# calcula tempo decorrido no processamento
tempo <- fim - inicio
