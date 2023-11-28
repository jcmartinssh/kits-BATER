# script para verificar e remover Area de Risco (AR) duplicadas

library(sf)
library(dplyr)
library(arrow)

sf_use_s2(FALSE)

# seleciona o arquivo geopackage com a base AR
AR_file <- tcltk::tk_choose.files(caption = "selecionar arquivo de áreas de risco:", multi = FALSE)

Mun_file <- tcltk::tk_choose.files(caption = "selecionar arquivo de municípios:", multi = FALSE)

saida <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# cria lista de camadas para cada arquivo de base
lotes <- st_layers(AR_file)

ver_mun <- st_layers(Mun_file)

# seleciona a camada para cada arquivo de base
lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)

mun_layer <- select.list(ver_mun[[1]], title = "municípios:", graphics = TRUE)

# áreas de risco - colunas com identificadores únicos e dados dos municípios
col_AR <- st_read(AR_file, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |>
    colnames()

fid_AR <- select.list(col_AR, title = "id unico:", graphics = TRUE)

cod_AR <- select.list(col_AR, title = "código municipal das áreas de risco:", graphics = TRUE)

# geo_AR <- select.list(col_AR, title = "coluna da geometria:", graphics = TRUE)

mun_AR <- select.list(col_AR, title = "nome municipal das áreas de risco:", graphics = TRUE)

col_mun <- st_read(Mun_file, query = paste("SELECT * from ", mun_layer, " LIMIT 0", sep = "")) |>
    colnames()

cod_mun <- select.list(col_mun, title = "geocódigo dos municipio:", graphics = TRUE)

nom_mun <- select.list(col_mun, title = "nomes dos municípios:", graphics = TRUE)

# cria lista de municípios da AR
lista_mun <- st_read(AR_file, query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |>
    pull() |>
    as.character()

# carrega o arquivo de AR
AR_full <- AR_file |> st_read(layer = lote_layer)

# cria cópia sem as feições com identificador repitido
AR_nodup <- AR_full |> distinct(across(setdiff(col_AR, fid_AR)), .keep_all = TRUE)

# cria cópia das camadas e remove a geometria
T_AR_full <- AR_full

st_geometry(T_AR_full) <- NULL

T_AR_nodup <- AR_nodup

st_geometry(T_AR_nodup) <- NULL

# compila os dados da camada original
original <- T_AR_full |>
    group_by(!!as.symbol(cod_AR)) |>
    summarize(n_ar = n())

# compila os dados da camada sem duplicadas e compoe com dados da camada original
avaliacao_dp <- T_AR_nodup |>
    group_by(!!as.symbol(cod_AR)) |>
    summarize(n_ar = n()) |>
    full_join(original, join_by(!!cod_AR), suffix = c("_no_dup", "_full")) |>
    rename(cod_mun = !!cod_AR) |>
    mutate(cod_mun = as.character(cod_mun), AR_original = n_ar_full, AR_duplicadas = n_ar_full - n_ar_no_dup) |>
    filter(AR_duplicadas > 0)

# carrega a tabela dos municipios do IBGE coom geocodigo e nome
municipios <- st_read(Mun_file, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", mun_layer, sep = "")) |>
    rename(cod_mun = !!cod_mun, nom_mun = !!nom_mun)

# cria a tabela de avaliacao com o nome dos municipios na base do IBGE e o total de AR original e duplicada
avaliacao_dp <- avaliacao_dp |>
    left_join(municipios, join_by(cod_mun)) |>
    select(c(cod_mun, nom_mun, AR_original, AR_duplicadas))

# corrige a geometria da camada de AR sem duplicada
AR_final <- st_make_valid(AR_nodup)

# associa espacialmente as AR aos municipios
AR_final_mun <- st_join(AR_final, municipios, join = st_intersects, largest = TRUE)

# separa as AR cujo registro do municipio difere da sua localizacao espacial
AR_mun_dif <- AR_final_mun |>
    rename(cod_orig = !!cod_AR, mun_orig = !!mun_AR, cod_geop = cod_mun, mun_geop = nom_mun) |>
    mutate(cod_orig = as.character(cod_orig)) |>
    filter(cod_orig != cod_geop)

# compila a tabela de municipios com total por municipio de registro / localizacao
Mun_dif <- AR_mun_dif |>
    select(cod_orig, mun_orig, cod_geop, mun_geop) |>
    group_by(cod_orig, cod_geop, mun_geop) |>
    summarise(mun_orig = first(mun_orig), n_ar = n()) |>
    select(cod_orig, mun_orig, cod_geop, mun_geop, n_ar)

# remove a geometria da tabela de municipios
st_geometry(Mun_dif) <- NULL

## exporta a camada de AR com registro do municipio diferente da localizacao
st_write(AR_mun_dif, paste(saida, "/AR_aval_mun.shp", sep = ""))

## exporta tabela com numero de AR duplicada por municipio
st_write(Mun_dif, paste(saida, "/Mun_dif.ods", sep = ""))

## exportar a camada de AR limpas
st_write(AR_nodup, paste(saida, "/AR_Lote5_semDup.shp", sep = ""))
