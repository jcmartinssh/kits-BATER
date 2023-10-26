library(sf)
library(dplyr)
# library(microbenchmark)
# library(data.table)

sf_use_s2(FALSE)

# seleciona o arquivo geopackage com a base áreas de risco
AR_file <- tcltk::tk_choose.files(caption = "selecionar arquivo de áreas de risco:", multi = FALSE)

Mun_file <- tcltk::tk_choose.files(caption = "selecionar arquivo de municípios:", multi = FALSE)

saida <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# cria lista de camadas para cada arquivo de base
lotes <- st_layers(AR_file)

ver_mun <- st_layers(Mun_file)

# seleciona a camada para cada arquivo de base
lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)

mun_layer <- select.list(ver_mun[[1]], title = "municípios:", graphics = TRUE)

# áreas de risco - geocódigo do município
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

# cria lista de municípios das áreas de risco
lista_mun <- st_read(AR_file, query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |>
    pull() |>
    as.character()

AR_full <- AR_file |> st_read(layer = lote_layer)

AR_nodup <- AR_full |> distinct(across(setdiff(col_AR, fid_AR)), .keep_all = TRUE)

T_AR_full <- AR_full

st_geometry(T_AR_full) <- NULL

T_AR_nodup <- AR_nodup

st_geometry(T_AR_nodup) <- NULL

original <- T_AR_full |>
    group_by(!!as.symbol(cod_AR)) |>
    summarize(n_ar = n())

avaliacao_dp <- T_AR_nodup |>
    group_by(!!as.symbol(cod_AR)) |>
    summarize(n_ar = n()) |>
    full_join(original, join_by(!!cod_AR), suffix = c("_no_dup", "_full")) |>
    rename(cod_mun = !!cod_AR) |>
    mutate(cod_mun = as.character(cod_mun), AR_original = n_ar_full, AR_duplicadas = n_ar_full - n_ar_no_dup) |>
    filter(AR_duplicadas > 0)

municipios <- st_read(Mun_file, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", mun_layer, sep = "")) |>
    rename(cod_mun = !!cod_mun, nom_mun = !!nom_mun)

avaliacao_dp <- avaliacao_dp |>
    left_join(municipios, join_by(cod_mun)) |>
    select(c(cod_mun, nom_mun, AR_original, AR_duplicadas))

AR_final <- st_make_valid(AR_nodup)

# se a base de 2021, descomentar abaixo
st_crs(municipios) <- "EPSG:4674"

AR_final_mun <- st_join(AR_final, municipios, join = st_intersects, largest = TRUE)


# st_geometry(AR_final_mun) <- NULL

AR_mun_dif <- AR_final_mun |>
    rename(cod_orig = !!cod_AR, mun_orig = !!mun_AR, cod_geop = cod_mun, mun_geop = nom_mun) |>
    mutate(cod_orig = as.character(cod_orig)) |>
    filter(cod_orig != cod_geop)

Mun_dif <- AR_mun_dif |>
    select(cod_orig, mun_orig, cod_geop, mun_geop) |>
    group_by(cod_orig, cod_geop, mun_geop) |>
    summarise(mun_orig = first(mun_orig), n_ar = n()) |>
    select(cod_orig, mun_orig, cod_geop, mun_geop, n_ar)

st_geometry(Mun_dif) <- NULL

# AR_mun_dif_2010 <- AR_mun_dif

# Mun_dif_2010 <- Mun_dif

# AR_mun_dif_2021 <- AR_mun_dif |>
#     select(fid, cod_geop, mun_geop) |>
#     rename(cod_2021 = cod_geop, mun_2021 = mun_geop)

# st_geometry(AR_mun_dif_2021) <- NULL

# AR_mun_dif_aval <- AR_mun_dif_2010 |>
#     full_join(AR_mun_dif_2021, join_by(fid))

# st_write(AR_mun_dif_aval, paste(saida, "/AR_aval_mun.shp", sep = ""))

# st_write(Mun_dif_2010, paste(saida, "/Mun_dif.ods", sep = ""))

# Mun_dif_2021 <- Mun_dif

# lista_mun_dif_2010 <- AR_mun_dif_2010$fid
# lista_mun_dif_2021 <- AR_mun_dif_2021$fid

# mun_dif_2010_2021 <- setdiff(lista_mun_dif_2010, lista_mun_dif_2021)

# mun_dif_2010_2021
# st_write(avaliacao_dp, paste(saida, "/avaliacao_duplicadas.ods", sep = ""))
