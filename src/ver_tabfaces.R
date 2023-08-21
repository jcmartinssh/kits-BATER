library(sf)
library(tidyr)
library(data.table)
library(stringr)

filtrogpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
  2, 2,
  byrow = TRUE
)

# escolhe diretório de saída
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

cod_erro <- lapply(0:9, rep, 3) |> lapply(paste, collapse = "") |> unlist()

tabfaces <- read_sf(tabfaces_gpkg, layer = tabfaces_layer) |> setDT()

tam_orig <- nrow(tabfaces)

olcol <- colnames(tabfaces)[8:31]

neocol <- paste("V", str_pad(1:24, 3, pad = "0"), sep = "")

setnames(tabfaces, olcol, neocol)

setkey(tabfaces, X4)

tabfaces <- (
  tabfaces
  [!is.na(X4) & nchar(X4) == 21 & !(substr(X4, 19, 21) %in% cod_erro)]
  [V001 != 0 & V004 != 0]
  )

all_dup <- tabfaces[, dup := .N > 1, by = key(tabfaces)][dup == TRUE]

setkey(all_dup, X4, V001, V004)
alldup_unq <- duplicated(all_dup)

tabfaces_pt <- fsetdiff(tabfaces, all_dup, all = TRUE)

rm(tabfaces)

gc()
