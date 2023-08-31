library(sf)
# library(rstudioapi)
# library(dplyr)
library(tidyr)
library(data.table)

filtrogpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
                     2, 2,
                     byrow = TRUE
)

# escolhe diretrório de saída
output <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# seleciona o arquivo geopackage com a base de faces com geometria
faces_gpkg <- tcltk::tk_choose.files(
  caption = "arquivo das faces com geometria:",
  multi = FALSE,
  filters = filtrogpkg
)

setores_gpkg <- tcltk::tk_choose.files(
  caption = "arquivo dos setores censitarios:",
  multi = FALSE,
  filters = filtrogpkg
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
)


faces_perf <- (
  faces_integra
  [CD_GEO == paste(CD_SETOR, CD_QUADRA, CD_FACE, sep = "")]
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

id_geom <- faces_georec$ID |> paste(collapse = ", ")

faces_irrec <- fsetdiff(faces_problema, faces_georec, all = TRUE)

rm(faces_problema)
gc()

faces_geom <- read_sf(faces_gpkg,
                      query = paste("SELECT ID, geom FROM ", faces_layer, " WHERE ID IN (", id_geom, ")")) |> setDT()

faces_georec[faces_geom, on = "ID", geom := geom]

rm(faces_geom)
gc()

setores <- read_sf(setores_gpkg,
                   query = paste("SELECT CD_GEOCODI, geom FROM ", setores_layer))

faces_georec <- st_as_sf(faces_georec)

# rm(faces_dp)
# rm(faces_perf)
# rm(faces_irrec)
# gc()

setores <- st_make_valid(setores)
faces_georec <- st_make_valid(faces_georec)

setores <- st_filter(setores, faces_georec)

faces_georec <- st_join(faces_georec,
                        setores, 
                        join = st_intersects,
                        left = TRUE, 
                        largest = TRUE)

st_geometry(faces_georec) <- NULL
setDT(faces_georec)
rm(setores)
gc()

faces_georec[, ':=' (CD_SETOR = CD_GEOCODI,
                     CD_GEO = paste(CD_SETOR, CD_QUADRA, CD_FACE, sep = ""),
                     status = "recuperadas")]

faces_irrec[, status := "invalidas"]
faces_dp[, status := "duplicadas"]
faces_perf[, status := "perfeitas"]

faces_aval <- rbindlist(list(faces_irrec, faces_georec, faces_dp, faces_perf))

rm(list = list(faces_irrec, faces_georec, faces_dp, faces_perf))
gc()

(
  faces_aval
  [, CONT := .N, by = CD_GEO]
  [status %in% c("perfeitas", "recuperadas") & CONT > 1, status := "duplicadas"]
  [, .(ID, CD_GEO, CD_SETOR, CD_QUADRA, CD_FACE, CONT, status)]
)

st_write(faces_aval, paste(output, "/", "faces_geo_aval.ods", append = FALSE))

