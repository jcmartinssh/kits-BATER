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
  # cria lista de camadas para cada arquivo de base
faces_ver <- st_layers(faces_gpkg)

# seleciona a camada para cada arquivo de base
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)

# testando as relações "one-to-many" nas faces com suas variáveis


faces <- read_sf(faces_gpkg,
  layer = faces_layer
)

st_geometry(faces) <- NULL

setDT(faces)

setkey(faces, CD_GEO)

faces_salvaveis <- (
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
  faces_salvaveis
  [CD_GEO == paste(CD_SETOR, CD_QUADRA, CD_FACE, sep = "")]
)

faces_perf_uq <- unique(faces_perf)

faces_perf_dp <- fsetdiff(faces_perf, faces_perf_uq, all = TRUE)

faces_naosalvaveis <- fsetdiff(faces, faces_salvaveis, all = TRUE)

faces_uq <- unique(faces_salvaveis, by = "CD_GEO")

faces_dp <- fsetdiff(faces_salvaveis, faces_uq, all = TRUE)

