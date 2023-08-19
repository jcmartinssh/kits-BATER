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
# output <- selectDirectory(caption = "diretório de saída:", label = "Select", path = "./data/")
output <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# seleciona o arquivo geopackage com a base de faces com variáveis de mapeamento
# tabfaces_gpkg <- selectFile(caption = "selecionar arquivo de variáveis de faces:", label = "Select")
tabfaces_gpkg <- tcltk::tk_choose.files(
  caption = "arquivo das faces com variáveis:",
  multi = FALSE,
  filters = filtrogpkg
)

 # cria lista de camadas para cada arquivo de base
tabfaces_ver <- st_layers(tabfaces_gpkg)

# seleciona a camada para cada arquivo de base
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "dados de faces:", graphics = TRUE)

tabfaces <- read_sf(tabfaces_gpkg,
  layer = tabfaces_layer
) |> setDT()

setkey(tabfaces, X4)

tabfaces_uq <- unique(tabfaces, by = "X4")

tabfaces_dp <- fsetdiff(tabfaces, tabfaces_uq, all = TRUE)

rm(tabfaces)

gc()
