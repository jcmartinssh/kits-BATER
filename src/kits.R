library(sf)
library(rstudioapi)

municipios_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/MUNICIPIOS.gpkg"
AR_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/AreasDeRisco.gpkg"
faces_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/Faces.gpkg"
setores_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/"
BATER_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/PRODUTOS/BATER.gpkg"

lotes <- st_layers(AR_gpkg)
setores_gpkg <- st_layers(setores_gpkg)
faces <- st_layers(faces_gpkg)
municipios <- st_layers(municipios_gpkg)

lote_layer <- select.list(lotes[[1]], title = "selecione o lote:", graphics = TRUE)
face_layer <- select.list(faces[[1]], title = "selecione a base de faces:", graphics = TRUE)
setor_layer <- select.list(setores_gpkg[[1]], title = "selecione a base de setores:", graphics = TRUE)
municipios_layer <- select.list(municipios[[1]], title = "selecione a base de municipios:", graphics = TRUE)

query_mun <- paste("SELECT DISTINCT ")
lista_mun <- read_sf()