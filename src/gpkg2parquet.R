library(sf)
library(arrow)

filtro_gpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
    2, 2,
    byrow = TRUE
)

entrada_gpkg <- tcltk::tk_choose.files(
    caption = "arquivo geopackage:",
    multi = FALSE,
    filters = filtro_gpkg
)

entrada_ver <- st_layers(entrada_gpkg)

entrada_layer <- select.list(
    entrada_ver[[1]],
    title = "camada do geopackage:",
    graphics = TRUE
)

saida <- tcltk::tk_choose.dir(caption = "diretório de saída:")

entrada <- read_sf(entrada_gpkg, layer = entrada_layer)

write_parquet(
    entrada,
    sink = paste(saida, "/", entrada_layer, ".parquet", sep = "")
)