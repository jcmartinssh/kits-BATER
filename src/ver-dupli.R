library(sf)
library(rstudioapi)
# library(dplyr)
library(tidyr)
library(data.table)

# escolhe diretrório de saída
output <- selectDirectory(caption = "diretório de saída:", label = "Select", path = "./data/")

# seleciona o arquivo geopackage com a base de faces com variáveis de mapeamento
tabfaces_gpkg <- selectFile(caption = "selecionar arquivo de variáveis de faces:", label = "Select")

# seleciona o arquivo geopackage com a base de faces com geometria
faces_gpkg <- selectFile(caption = "selecionar arquivo de base de faces:", label = "Select")


# cria lista de camadas para cada arquivo de base
faces_ver <- st_layers(faces_gpkg)
tabfaces_ver <- st_layers(tabfaces_gpkg)

# seleciona a camada para cada arquivo de base
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "dados de faces:", graphics = TRUE)


# testando as relações "one-to-many" nas faces com suas variáveis


faces <- read_sf(faces_gpkg,
                 layer = faces_layer) 

st_geometry(faces) <- NULL

setDT(faces)

tabfaces <- read_sf(tabfaces_gpkg,
                    layer = tabfaces_layer) |> setDT()

setkey(faces, CD_GEO)
setkey(tabfaces, X4)

tabfaces_uq <- unique(tabfaces, by = "X4")

tabfaces_dp <- fsetdiff(tabfaces, tabfaces_uq, all = TRUE)



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
