# script para verificar e remover Area de Risco (AR) duplicadas

library(sf)
library(tidyr)
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

# carrega o arquivo de AR e corrige a geometria
AR_full <- AR_file |>
    st_read(layer = lote_layer) |>
    st_make_valid()

# cria cópia sem as feições com identificador repitido
AR_nodup_cols <- AR_full |> distinct(across(setdiff(col_AR, fid_AR)), .keep_all = TRUE)
AR_nodup_geo <- AR_full |> distinct(geometry, .keep_all = TRUE)

AR_nodup_diff <- setdiff(AR_nodup_cols, AR_nodup_geo) |>
    mutate(geo_dupli = "sim") |>
    select(geo_dupli)


AR_nodup <- st_join(
    AR_nodup_cols,
    AR_nodup_diff,
    join = st_equals
) |>
    mutate(geo_dupli = if_else(
        is.na(geo_dupli),
        "nao",
        geo_dupli
    ))


# length(AR_nodup$geo_dupli[AR_nodup$geo_dupli == "sim"])
# length(AR_nodup$geo_dupli[AR_nodup$geo_dupli == "nao"])

###################################################
## Prepara tabela de avaliacao de areas de risco ##
## duplicadas por municipios de registro         ##
###################################################

# cria cópia das camadas e remove a geometria
T_AR_full <- AR_full

st_geometry(T_AR_full) <- NULL

T_AR_nodup <- AR_nodup

st_geometry(T_AR_nodup) <- NULL

gc()

# compila os dados da camada original
original <- T_AR_full |>
    group_by(!!as.symbol(cod_AR)) |>
    summarize(n_ar_orig = n())

# compila os dados da camada sem duplicadas e compoe com dados da camada original
avaliacao_dp <- T_AR_nodup |>
    group_by(!!as.symbol(cod_AR), geo_dupli) |>
    summarize(n_ar_nodup = n()) |>
    pivot_wider(
        id_cols = !!as.symbol(cod_AR),
        names_from = geo_dupli,
        values_from = n_ar_nodup,
        names_prefix = "n_ar_nodup_geodp_",
        values_fill = 0
    ) |>
    full_join(original, join_by(!!cod_AR)) |>
    rename(
        cod_mun = !!cod_AR,
        AR_original = n_ar_orig,
        AR_unicas = n_ar_nodup_geodp_nao,
        AR_geodup = n_ar_nodup_geodp_sim
    ) |>
    mutate(
        cod_mun = as.character(cod_mun),
        AR_duplicadas = AR_original - (AR_unicas + AR_geodup)
    ) |>
    filter(AR_unicas != AR_original)

# carrega a tabela dos municipios do IBGE coom geocodigo e nome
municipios <- st_read(Mun_file, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", mun_layer, sep = "")) |>
    rename(cod_mun = !!cod_mun, nom_mun = !!nom_mun)

# cria a tabela de avaliacao com o nome dos municipios na base do IBGE e o total de AR original e duplicada
avaliacao_dp <- avaliacao_dp |>
    left_join(municipios, join_by(cod_mun)) |>
    select(c(cod_mun, nom_mun, AR_original, AR_duplicadas, AR_geodup, AR_unicas))

###################################################
## Prepara a camada para avaliacao de areas de   ##
## risco deslocadas do municipios de registro    ##
###################################################

# associa espacialmente as AR aos municipios
AR_final_mun <- st_join(AR_nodup, municipios, join = st_intersects, largest = TRUE)

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

############################################
## Exporta as tabelas e as areas de risco ##
############################################

## exporta a camada de AR com registro do municipio diferente da localizacao
st_write(
    AR_mun_dif,
    paste(saida, "/AR_aval_mun.shp", sep = ""),
    append = FALSE
)

## exportar a camada de AR limpas
st_write(
    AR_nodup,
    paste(saida, "/AR_Lote5_semDup.shp", sep = ""),
    append = FALSE
)

## exporta tabela com numero de AR com registro do municipio diferente da localizacao
st_write(
    Mun_dif,
    paste(saida, "/Mun_dif.ods", sep = ""),
    append = FALSE
)

## exporta tabela com numero de AR duplicada por municipio
st_write(
    avaliacao_dp,
    paste(saida, "/avaliacao_dp.ods", sep = ""),
    append = FALSE
)
