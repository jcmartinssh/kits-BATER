
library(arrow)
library(sf)
library(data.table)
library(microbenchmark)
library(purrr)
library(ggplot2)
library(parallel)

filtro_gpkg <- matrix(c("Geopackage", "*.gpkg", "All files", "*"),
                      2, 2,
                      byrow = TRUE
)

filtro_parquet <- matrix(c("Parquet", "*.parquet", "All files", "*"),
                     2, 2,
                     byrow = TRUE
)

# seleciona o arquivo geopackage com a base de faces com geometria
faces_parquet <- tcltk::tk_choose.files(
  caption = "arquivo das faces com avaliação:",
  multi = FALSE,
  filters = filtro_parquet
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

faces_col <- colnames(faces_georec)

rm(faces_aval)
gc()

id_geom <- faces_georec$ID
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

lista_faces_ID <- id_geom[runif(15, min = 1, max = 45509)]

func_map_sf <- function (id_face, face_geo = faces_georec, base_setor = setores_censitarios) {
  face <- face_geo[face_geo$ID == id_face, ]
  setor <- base_setor[face, op = st_intersects]
  face <- st_join(face, setor, join  = st_intersects, left = TRUE, largest = TRUE)
  st_geometry(face) <- NULL
  return(face)
}

func_for_sf <- function (lista_faces, face_geo = faces_georec, base_setor = setores_censitarios) {
  lista_teste <- list()
  for (i in seq_along(lista_faces)) {
    face <- face_geo[face_geo$ID == lista_faces[i], ]
    setor <- base_setor[face, op = st_intersects]
    face <- st_join(face, setor, join  = st_intersects, left = TRUE, largest = TRUE)
    st_geometry(face) <- NULL
    lista_teste[[i]] <- face
  }
  rbindlist(lista_teste)
}

num_cores <- detectCores()

cl_proc <- makeCluster(num_cores)

clusterExport(cl_proc, c("lista_faces_ID", "func_map_sf", "faces_georec", "setores_censitarios"))

clusterEvalQ(cl_proc, {
  library(sf)
})

# teste <- lapply(lista_faces_ID, func_map_sf) |> rbindlist()
# teste2 <- map(lista_faces_ID, func_map_sf) |> rbindlist()

list_benchmk <- microbenchmark(
  func_map = map(lista_faces_ID, func_map_sf) |> rbindlist(),
  func_for = func_for_sf(lista_faces = lista_faces_ID),
  func_apply = lapply(lista_faces_ID, func_map_sf) |> rbindlist(),
  func_par = parLapply(cl = cl_proc, lista_faces_ID, func_map_sf) |> rbindlist(),
  times = 5
)

list_benchmk
