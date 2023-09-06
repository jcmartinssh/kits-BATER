library(sf)
# library(rstudioapi)
# library(dplyr)
library(tidyr)
library(data.table)
library(arrow)
library(parallel)


filtro_gpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
                      2, 2,
                      byrow = TRUE
)

# escolhe diretrório de saída
output <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# seleciona o arquivo geopackage com a base de faces com geometria
faces_gpkg <- tcltk::tk_choose.files(
  caption = "arquivo das faces com geometria:",
  multi = FALSE,
  filters = filtro_gpkg
)

setores_gpkg <- tcltk::tk_choose.files(
  caption = "arquivo dos setores censitários:",
  multi = FALSE,
  filters = filtro_gpkg
)

# cria lista de camadas para cada arquivo de base
faces_ver <- st_layers(faces_gpkg)

setores_ver <- st_layers(setores_gpkg)

# seleciona a camada para cada arquivo de base
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)
setores_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)

faces_col_sql <- read_sf(faces_gpkg, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |>
  colnames()

faces_col_sql <- faces_col_sql[! faces_col_sql == "geom"] |> paste(collapse = ", ")

# testando as relações "one-to-many" nas faces com suas variáveis

faces <- read_sf(faces_gpkg, query = paste("SELECT ", faces_col_sql, " FROM ", faces_layer))

setDT(faces)

setkey(faces, CD_GEO)

faces[, CONT := .N, by = CD_GEO]

faces_integra <- (
  faces
  [nchar(CD_GEO) == 21 | (nchar(CD_QUADRA) == 3 & nchar(CD_FACE) == 3)]
  [nchar(CD_GEO) == 21 & !(substr(CD_GEO, 19, 19) == " " | (substr(CD_GEO, 16, 16) == " " & substr(CD_GEO, 20, 20) == " "))]
  [substr(CD_GEO, 16, 21) != "000000" | (CD_QUADRA != "000" & CD_FACE != "000")]
  [substr(CD_GEO, 16, 21) != "999999" | (CD_QUADRA != "999" & CD_FACE != "999")]
  [substr(CD_GEO, 16, 21) != "888888" | (CD_QUADRA != "888" & CD_FACE != "888")]
  [CD_QUADRA != "777"]
  [!(CD_QUADRA == "00" & (is.na(CD_GEO) | CD_FACE == "00"))]
  [!(CD_FACE == "999" | CD_FACE == "888" | CD_FACE == "777")]
  [CD_GEO == paste(CD_SETOR, CD_QUADRA, CD_FACE, sep = "")]
)


faces_perf <- (
  faces_integra
  [CONT == 1]
)

faces_dp <- fsetdiff(faces_integra, faces_perf, all = TRUE)

faces_problema <- fsetdiff(faces, faces_integra, all = TRUE)

rm(faces)
rm(faces_integra)
gc()

faces_georec <- (
  faces_problema
  [is.na(CD_SETOR) & (nchar(CD_QUADRA) == 3 & nchar(CD_FACE) == 3)]
  [!(CD_QUADRA %in% c("\n00", "000", "777", "888", "999") | CD_FACE %in% c("\n00", "000", "777", "888", "999"))]
)

faces_irrec <- fsetdiff(faces_problema, faces_georec, all = TRUE)

rm(faces_problema)
gc()

faces_georec[, status := "recuperaveis"]
faces_irrec[, status := "invalidas"]
faces_dp[, status := "duplicadas"]
faces_perf[, status := "perfeitas"]

id_geom <- faces_georec[, ID]
id_geom_sql <- id_geom |> paste(collapse = ", ")

faces_geom <- read_sf(faces_gpkg,
                      query = paste("SELECT ID, geom FROM ",
                      faces_layer, " WHERE ID IN (", id_geom_sql, ")")) |> setDT()

faces_georec[faces_geom, on = "ID", geom := geom]

rm(faces_geom)
gc()

faces_georec <- faces_georec |> st_as_sf() |> st_make_valid()

setores_censitarios <- read_sf(setores_gpkg,
                               query = paste("SELECT CD_GEOCODI, geom FROM ", setores_layer)) |> 
  st_make_valid() |>
  st_filter(faces_georec)

gc()

func_map_sf <- function(id_face, face_geo, base_setor) {
  face <- face_geo[face_geo$ID == id_face, ]
  setor <- base_setor[face, op = st_intersects]
  face <- st_join(face, setor, join  = st_intersects, left = TRUE, largest = TRUE)
  st_geometry(face) <- NULL
  return(face)
}

num_cores <- detectCores()

# cria o cluster para processamento
cl_proc <- makeCluster(num_cores)

# exporta as variaveis para os clusters
# clusterExport(cl_proc, c("pr_albers_br", "lmun", "grd_gpkg", "base_gpkg", "grd_layer", "grd_vars", "grd_id", "crs", "lista_mun_map"))
clusterExport(cl_proc, c("id_geom", "func_map_sf", "faces_georec", "setores_censitarios"))

# carrega as bibliotecas utilizadas no cluster
clusterEvalQ(cl_proc, {
  library(sf)
})

# aplica a funcao de geoprocessamento de maneira paralelizada para construir a base de calculo dos coeficientes
faces_georec <- parLapply(cl = cl_proc, id_geom, func_map_sf) |> rbindlist()

# finaliza o cluster
stopCluster(cl_proc)

# # consolida a lista em uma tabela de base de calculo dos coeficientes
# tabela_calc_coef <- rbindlist(lista_calc_coef, fill = TRUE)

# write_parquet(faces_aval, sink = paste(output, "/", "faces_geo_aval.parquet", sep = ""))
# # st_write(faces_aval, paste(output, "/", "faces_geo_aval.ods", sep = ""), append = FALSE)

