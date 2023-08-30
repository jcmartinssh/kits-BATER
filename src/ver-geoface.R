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

# testando as relações "one-to-many" nas faces com suas variáveis

faces <- read_sf(faces_gpkg,
                 layer = faces_layer
)

setDT(faces)

setkey(faces, CD_GEO)

faces_geom <- faces[, .(ID, geom)]

st_geometry(faces) <- NULL

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

gc()

faces_problema <- fsetdiff(faces, faces_integra, all = TRUE)

rm(faces)
rm(faces_integra)
gc()

faces_georec <- (
  faces_problema
  [is.na(CD_SETOR) & (nchar(CD_QUADRA) == 3 & nchar(CD_FACE) == 3)]
  [!(CD_QUADRA %in% c("\n00", "000", "888", "999") | CD_FACE %in% c("\n00", "000", "888", "999"))]
)

faces_irrec <- fsetdiff(faces_problema, faces_georec, all = TRUE)

rm(faces_problema)
gc()

setores <- read_sf(setores_gpkg,
                   layer = setores_layer)

# setDF(faces_georec)

faces_georec[faces_geom, on = "ID", CD_SETOR := CD_GEOCODI]

faces_georec <- st_join(faces_georec,
                        setores, 
                        join = st_intersects,
                        left = TRUE, 
                        largest = TRUE)

st_geometry(faces_georec) <- NULL

# setDT(faces_georec)

faces_irrec[, status := "invalidas"]
faces_georec[, status := "recuperadas"]
faces_dp[, status := "duplicadas"]
faces_perf[, status := "perfeitas"]

faces_aval <- rbindlist(list(faces_irrec, faces_georec, faces_dp, faces_perf))

rm(list = list(faces_irrec, faces_georec, faces_dp, faces_perf))
gc()

