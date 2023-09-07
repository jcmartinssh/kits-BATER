library(sf)
# library(tidyr)
library(data.table)
library(arrow)
library(parallel)

inicio <- Sys.time()

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

faces_georec[, status_geo := "recuperaveis"]
faces_irrec[, status_geo := "invalidas"]
faces_dp[, status_geo := "duplicadas"]
faces_perf[, status_geo := "perfeitas"]

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

func_map_sf <- function(id_face, face_geo = faces_georec, base_setor = setores_censitarios) {
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
clusterExport(cl_proc, c("id_geom", "func_map_sf", "faces_georec", "setores_censitarios"))

# carrega as bibliotecas utilizadas no cluster
clusterEvalQ(cl_proc, {
  library(sf)
})

# aplica a funcao de geoprocessamento de maneira paralelizada para construir a base de calculo dos coeficientes
faces_georec <- parLapply(cl = cl_proc, X = id_geom, fun = func_map_sf) |> rbindlist()

# finaliza o cluster
stopCluster(cl_proc)

fim <- Sys.time()
tempo <- fim - inicio

## verificacao das falhas de associacao com geocodigo do setor - foram duas, problemas na geometria das faces - irrecuperavel
# faces_geoorec_falha_SQL <- faces_georec[is.na(CD_GEOCODI), ID] |> paste(collapse = ", ")
# 
# faces_geoorec_falha <- read_sf(faces_gpkg,
#                       query = paste("SELECT * FROM ",
#                                     faces_layer, " WHERE ID IN (", faces_geoorec_falha_SQL, ")"))
# 
# falha_setores <- setores_censitarios[faces_geoorec_falha, op = st_intersects]

faces_georec[is.na(CD_GEOCODI), status_geo := "invalidas"]

faces_georec[status_geo == "recuperaveis", ':=' (CD_SETOR = CD_GEOCODI, CD_GEO = paste(CD_SETOR, CD_QUADRA, CD_FACE, sep = ""))]

setkey(faces_georec, CD_GEO)

faces_georec[, CONT := .N, by = CD_GEO]

facesGeocod_ex <- c(faces_perf$CD_GEO, unique(faces_dp$CD_GEO))

(
  faces_georec
  [status_geo == "recuperaveis" & (CONT > 1 | CD_GEO %in% FacesGeocod_ex), status_geo := "duplicadas"]
  [status_geo == "recuperaveis", status_geo := "recuperadas"]
  [, CD_GEOCODI := NULL]
)

faces_geo_aval <- rbindlist(list(faces_georec, faces_irrec, faces_dp, faces_perf))

rm(faces_georec, faces_irrec, faces_dp, faces_perf)
gc()

write_parquet(faces_geo_aval, sink = paste(output, "/", "faces_geo_aval.parquet", sep = ""))

