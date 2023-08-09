library(sf)
library(rstudioapi)
library(dplyr)

output <- "./data/"

municipios_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/MUNICIPIOS.gpkg"
AR_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/AreasDeRisco.gpkg"
faces_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/Faces.gpkg"
tabfaces_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/Faces_CNEFE.gpkg"
setores_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/Setores.gpkg"
BATER_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/PRODUTOS/BATER.gpkg"

lotes <- st_layers(AR_gpkg)
setores_ver <- st_layers(setores_gpkg)
faces_ver <- st_layers(faces_gpkg)
tabfaces_ver <- st_layers(tabfaces_gpkg)
municipios_ver <- st_layers(municipios_gpkg)

lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)
tabfaces_layer <- select.list(tabfaces_ver[[1]], title = "dados de faces:", graphics = TRUE)
setor_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)
municipios_layer <- select.list(municipios_ver[[1]], title = "base de municipios:", graphics = TRUE)

col_AR <- read_sf(AR_gpkg, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |> colnames()
cod_AR <- select.list(col_AR, title = "código dos municípios:", graphics = TRUE)

col_set <- read_sf(setores_gpkg, query = paste("SELECT * from ", setor_layer, " LIMIT 0", sep = "")) |> colnames()
codmun_set <- select.list(col_set, title = "código dos municípios:", graphics = TRUE)

col_face <- read_sf(faces_gpkg, query = paste("SELECT * from ", faces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_face <- select.list(col_face, title = "código das faces:", graphics = TRUE)

col_tabface <- read_sf(tabfaces_gpkg,  query = paste("SELECT * from ", tabfaces_layer, " LIMIT 0", sep = "")) |> colnames()
cod_tabface <- select.list(col_tabface, title = "código das faces", graphics = TRUE)

# tem que resolver a questão dos municípios novos não presentes na base de 2010 - são só dois.
lista_mun <- read_sf(AR_gpkg,
                     query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |> pull() |> as.character()

lista_mun <- lista_mun[1]
### agora que começam as paradas

aval_quali <- list()

for (i in 1:length(lista_mun)) {
  areas_risco <- read_sf(AR_gpkg, query = paste("SELECT * FROM ", lote_layer, " WHERE ", cod_AR, " = '", lista_mun[i], "'", sep = ""))
  setores <- read_sf(setores_gpkg, query = paste("SELECT * FROM ", setor_layer, " WHERE ", codmun_set, " = '", lista_mun[i], "'", sep = ""))
  faces <- read_sf(faces_gpkg, query = paste("SELECT * FROM ", faces_layer, " WHERE substr(", cod_face, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  tabela_faces <- read_sf(tabfaces_gpkg, query = paste("SELECT * FROM ", tabfaces_layer, " WHERE substr(", cod_tabface, ", 1, 7) = '", lista_mun[i], "'", sep = ""))
  faces <- full_join(faces, tabela_faces, by = setNames(nm = cod_face, cod_tabface), keep = TRUE)
  aval_quali[[i]] <- data.frame(
    municipio = lista_mun[i], 
    faces_com_dado = sum(!is.na(faces[cod_face]) && !is.na(faces[cod_tabface])),#nrow(filter(faces, !is.na(cod_face))), 
    faces_sem_dado = sum(is.na(faces[cod_tabface])),
    faces_sem_geo = sum(is.na(faces[cod_face])))
  faces2 <- faces |> filter(!is.na(cod_face))
  # gpkg <- paste(output, lista_mun[i], ".gpkg", sep = "")
  # write_sf(AR, dsn = gpkg, layer = areas_risco)
  # write_sf(setores, dsn = gpkg, layer = setores)
  # write_sf(faces, dsn = gpkg, layer = faces)
}

gpkg <- paste(output, lista_mun[i], "/", lista_mun[i], sep = "")
write_sf(areas_risco, paste(gpkg, "_areas_risco", ".shp", sep = ""))
write_sf(setores, paste(gpkg, "_setores", ".shp", sep = ""))
write_sf(faces, paste(gpkg, "_faces", ".shp", sep = ""))

