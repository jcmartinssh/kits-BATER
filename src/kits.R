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
library(data.table)
library(arrow)
library(rlang)

# desativa geometria esferica
sf_use_s2(FALSE)

# produz padrao de erro no geocodigo das faces
cod_erro <- lapply(0:9, rep, 3) |>
  lapply(paste, collapse = "") |>
  unlist()

# define opcao de produzir kits e tabela de avaliacao
op1 <- "Produzir os kits e tabela de avaliação das faces"

# define opcao de produzir tabela de avaliacao sem produzir kits
op2 <- "Produzir somente a tabela de avaliação das faces"

# seleciona operacao a ser realizada
proc <- select.list(c(op1, op2), preselect = op2, multiple = FALSE, title = "Seleção de procedimento", graphics = TRUE)

if (proc == op1) {
  prod_kits <- TRUE
} else {
  prod_kits <- FALSE
}


################################################
## selecao de arquivos e colunas de interesse ##
################################################


# filtro de formatos de arquivos
filtro <- matrix(c("Geopackage", "*.gpkg", "Parquet", "*.parquet", "Shapefile", "*.shp", "All files", "*"),
  4, 2,
  byrow = TRUE
)

# seleciona o diretrório de saída
output <- tcltk::tk_choose.dir(caption = "selecionar diretório de saída:")


################################
## faces - SISMAP - avaliacao ##


# seleciona a tabela de avaliacao da base de faces do SISMAP em formato Parquet
faces_aval_arq <- tcltk::tk_choose.files(
  caption = "selecionar tabela de avaliacao das faces do SISMAP em formato Parquet:",
  multi = FALSE,
  filters = matrix(c("Parquet", "*.parquet"),
    1, 2,
    byrow = TRUE
  )
)

# cria lista de camadas do arquivo
faces_geo_aval <- st_layers(faces_aval_arq)

# seleciona a camada de interesse
faces_aval_layer <- select.list(faces_geo_aval[[1]], title = "faces avaliadas:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_faces_aval <- read_sf(faces_aval_arq, query = paste("SELECT * from ", faces_aval_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o identificador unico da avaliacao das faces do SISMAP
id_faces_aval <- select.list(col_faces_aval, title = "identificador único da avaliação das faces do SISMAP:", graphics = TRUE)

# carrega o arquivo parquet de avaliacao das faces com geometria como dataset, para lazy evaluation.
# o filtro de IDs das faces resulta em um SQL muito grande, ultrapassando o limite do GDAL/OGR2OGR e
# inviabilizando o uso do pacote sf, sendo necessario usar a interface com Parquet oferecida
# pelo pacote arrow para a recuperacao das faces SISMAP avaliadas por municipio
faces_aval_geo <- open_dataset(faces_aval_arq)


####################
## areas de risco ##


# seleciona o arquivo com a base áreas de risco
AR_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de áreas de risco:", multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
lotes <- st_layers(AR_arq)

# seleciona a camada de interesse
lote_layer <- select.list(lotes[[1]], title = "camada de áreas de risco:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_AR <- read_sf(AR_arq, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo municipal da area de risco
cod_AR <- select.list(col_AR, title = "geocódigo municipal da área de risco:", graphics = TRUE)


################################
## faces - SISMAP - geometria ##


# seleciona o arquivo com a base de faces com geometria do SISMAP
faces_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de base de faces do SISMAP:",
  multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
faces_ver <- st_layers(faces_arq)

# seleciona a camada de interesse
faces_layer <- select.list(faces_ver[[1]], title = "base de faces do SISMAP:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_face <- read_sf(faces_arq, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo das faces do SISMAP
cod_face <- select.list(col_face, title = "geocódigo das faces do SISMAP:", graphics = TRUE)

# seleciona o campo com o identificador unico das faces do SISMAP
id_face <- select.list(col_face, title = "identificador único das faces do SISMAP:", graphics = TRUE)


###################
## faces - CNEFE ##


# seleciona o arquivo de faces do CNEFE com variaveis de mapeamento
tabfaces_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de faces do CNEFE com variáveis de mapeamento:", multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
tabfaces_ver <- st_layers(tabfaces_arq)

# seleciona a camada de interesse
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "faces CNEFE:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_tabface <- read_sf(tabfaces_arq, query = paste("SELECT * from ", tabfaces_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo das faces do CNEFE
cod_tabface <- select.list(col_tabface, title = "geocódigo das faces do CNEFE", graphics = TRUE)


#########################
## setores censitarios ##


# seleciona o arquivo com a base de setores censitários
setores_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de setores:", multi = FALSE,
  filters = filtro
)

# cria lista de camadas do arquivo
setores_ver <- st_layers(setores_arq)

# seleciona a camada de interesse
setor_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)

# produz lista com nomes das colunas da camada
col_set <- read_sf(setores_arq, query = paste("SELECT * from ", setor_layer, " LIMIT 0", sep = "")) |> colnames()

# seleciona o campo com o geocodigo dos setores censitarios
cod_set <- select.list(col_set, title = "geocódigo dos setores:", graphics = TRUE)


################
## municipios ##


# seleciona o arquivo da base de municipios
municipios_arq <- tcltk::tk_choose.files(
  caption = "selecionar arquivo de base de municípios:",
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
cod_mun <- select.list(col_mun, title = "geocódigo dos municípios:", graphics = TRUE)

# seleciona o campo com o nome dos municipios
nom_mun <- select.list(col_mun, title = "nome dos municípios:", graphics = TRUE)


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
  # carrega o municipio e valida a geometria
  municipio <- st_read(municipios_arq, query = paste("SELECT * FROM ", municipios_layer, " WHERE ", cod_mun, " = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

  # converte a geometria do municipio para WKT para utilizacao como filtro espacial
  mun_geom <- municipio |>
    st_geometry() |>
    st_as_text()

  # carrega as áreas de risco do município pelo geocodigo e valida geometria
  areas_risco <- areas_risco_tot |>
    filter(.data[[cod_AR]] == lista_mun[i]) |>
    st_make_valid()

  # carrega os setores do município e valida geometria
  setores <- st_read(setores_arq, query = paste("SELECT * FROM ", setor_layer, " WHERE substr(", cod_set, ", 1, 7) = '", lista_mun[i], "'", sep = "")) |>
    st_make_valid()

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
  # essa etapa e fragil, o ideal e remover isso e renomear as colunas
  # no arquivo do CNEFE
  olcol <- colnames(tabela_faces)[9:32]

  # cria os codigos das variaveis da publicacao proceduralmente
  # essa etapa e fragil, o ideal e remover isso e renomear as colunas
  # no arquivo do CNEFE
  neocol <- paste("V", str_pad(1:24, 3, pad = "0"), sep = "")

  # substitui o nome das colunas pelos da publicacao
  # essa etapa e fragil, o ideal e remover isso e renomear as colunas
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
