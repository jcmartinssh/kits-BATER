### separando a parte de associacao espacial com setor censitario para recuperacao do geocodigo das faces

library(arrow)
library(sf)
# library(rstudioapi)
# library(dplyr)
library(tidyr)
library(data.table)

filtro_gpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
                     2, 2,
                     byrow = TRUE
)

filtro_pqt <- matrix(c("Parquet", "*.parquet", "All files", "*"),
                     2, 2,
                     byrow = TRUE
)

# escolhe diretrório de saída
output <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# seleciona o arquivo geopackage com a base de faces com geometria
faces_parquet <- tcltk::tk_choose.files(
  caption = "arquivo das faces com avaliação:",
  multi = FALSE,
  filters = filtro_pqt
)

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

faces_aval <- read_parquet(faces_parquet)

faces_georec <- faces_aval[status == "recuperadas"]

id_geom <- faces_georec$ID |> paste(collapse = ", ")

faces_geom <- read_sf(faces_gpkg,
                      query = paste("SELECT ID, geom FROM ", faces_layer, " WHERE ID IN (", id_geom, ")")) |> setDT()

faces_georec[faces_geom, on = "ID", geom := geom]

rm(faces_geom)
gc()

faces_georec <- faces_georec |> st_as_sf() |> st_make_valid()

setores <- read_sf(setores_gpkg,
                   query = paste("SELECT CD_GEOCODI, geom FROM ", setores_layer)) |> 
  st_make_valid() |>
  st_filter(faces_georec)

gc()

faces_georec <- st_join(faces_georec,
                        setores, 
                        # join = st_intersects,
                        left = TRUE, 
                        largest = TRUE)

st_geometry(faces_georec) <- NULL
setDT(faces_georec)
rm(setores)
gc()

faces_georec[, ':=' (CD_SETOR = CD_GEOCODI,
                     CD_GEO = paste(CD_SETOR, CD_QUADRA, CD_FACE, sep = ""),
                     status = "recuperadas")]

# (
#   faces_aval
#   [, CONT := .N, by = CD_GEO]
#   [status %in% c("perfeitas", "recuperadas") & CONT > 1, status := "duplicadas"]
#   [, .(ID, CD_GEO, CD_SETOR, CD_QUADRA, CD_FACE, CONT, status)]
# )