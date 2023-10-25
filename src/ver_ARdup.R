library(sf)
library(dplyr)
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
col_AR <- st_read(AR_file, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()
cod_AR <- select.list(col_AR, title = "código municipal das áreas de risco:", graphics = TRUE)
# geo_AR <- select.list(col_AR, title = "coluna da geometria:", graphics = TRUE)
fid_AR <- select.list(col_AR, title = "id unico:", graphics = TRUE)

col_mun <- st_read(Mun_file, query = paste("SELECT * from ", mun_layer, " LIMIT 0", sep = "")) |> colnames()
cod_mun <- select.list(col_mun, title = "geocódigo dos municipio:", graphics = TRUE)
nom_mun <- select.list(col_mun, title = "nomes dos municípios:", graphics = TRUE)

# cria lista de municípios das áreas de risco
lista_mun <- st_read(AR_file,
                     query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |> pull() |> as.character()

# AR_nodup <- AR_file |> st_read(layer = lote_layer) |> distinct()

AR_full <- AR_file |> st_read(layer = lote_layer)

AR_nodup <- AR_full |> distinct(across(setdiff(col_AR, fid_AR)), .keep_all = TRUE)

T_AR_full <- AR_full
st_geometry(T_AR_full) <- NULL

T_AR_nodup <- AR_nodup
st_geometry(T_AR_nodup) <- NULL

# fid_col <- c("fid", "FID_1", "OID_", "OID1", "ID")

# AR_nodup2 <- AR_full |> select(-any_of(fid_col)) |> distinct()

# mun_dups <- c(1300060, 1303700, 1303908, 1304237, 1506005, 2307502, 2905602)

original <- T_AR_full |> group_by(!!as.symbol(cod_AR)) |> summarize(n_ar = n())
avaliacao_dp <- T_AR_nodup |> group_by(!!as.symbol(cod_AR)) |> summarize(n_ar = n()) |> full_join(original, join_by(!!cod_AR), suffix = c("_no_dup", "_full")) |> rename(cod_mun = !!cod_AR) |> mutate(cod_mun = as.character(cod_mun), AR_original = n_ar_full, AR_duplicadas = n_ar_full - n_ar_no_dup) |> filter(AR_duplicadas > 0)

municipios <- st_read(Mun_file, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", mun_layer, sep = "")) |> rename(cod_mun = !!cod_mun)

avaliacao_dp <- avaliacao_dp |> left_join(municipios, join_by(cod_mun)) |> select(c(cod_mun, !!nom_mun, AR_original, AR_duplicadas))

# st_write(avaliacao_dp, paste(saida, "/avaliacao_duplicadas.ods", sep = ""))


# AR_dup <- 

