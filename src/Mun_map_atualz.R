# script para construir lista de municipios mapeados por lote

library(sf)
library(dplyr)

lista_mun <- function() {
    file <- tcltk::tk_choose.files(caption = "selecionar arquivo de áreas de risco:", multi = FALSE)

    lote_layer <- st_layers(file)

    lote <- select.list(lote_layer[[1]], title = "selecionar lote de áreas de risco:", graphics = TRUE)

    cols <- st_read(file, query = paste("SELECT * from ", lote, " LIMIT 0", sep = "")) |>
        colnames()

    cod_mun <- select.list(cols, title = "selecionar coluna do código do município", graphics = TRUE)

    dado <- st_read(file, query = paste("SELECT DISTINCT ", cod_mun, " from ", lote, sep = "")) |>
        pull() |>
        as.character()

    return(dado)
}

info_mun <- function() {
    file <- tcltk::tk_choose.files(caption = "selecionar arquivo de municípios:", multi = FALSE)

    layer <- st_layers(file)

    mun <- select.list(layer[[1]], title = "selecionar camada dos municípios:", graphics = TRUE)

    cols <- st_read(file, query = paste("SELECT * from ", mun, " LIMIT 0", sep = "")) |>
        colnames()

    cod_mun <- select.list(cols, title = "selecionar coluna do código do município", graphics = TRUE)

    nom_mun <- select.list(cols, title = "selecionar coluna do nome do município", graphics = TRUE)

    dado <- st_read(file, query = paste("SELECT ", cod_mun, ", ", nom_mun, " from ", mun, sep = ""))

    return(list(cod_mun, dado))
}

## carregar os lotes na ordem cronologica

publicacao <- lista_mun() # escolher o dado consolidado da publicacao

lote_4 <- lista_mun() # escolher o dado do lote 4

lote_5 <- lista_mun() # escolher o dado do lote 5

info_munic <- info_mun()

cod_mun <- info_munic[[1]]

nome_mun <- info_munic[[2]]

todos <- union(publicacao, lote_4) |> union(lote_5)

historico_lotes <- data.frame(municipio = todos) |>
    left_join(nome_mun, join_by(municipio == !!cod_mun)) |>
    mutate(
        publicacao = ifelse(municipio %in% publicacao, TRUE, FALSE),
        lote_4 = ifelse(municipio %in% lote_4, TRUE, FALSE),
        lote_5 = ifelse(municipio %in% lote_5, TRUE, FALSE)
    ) |>
    filter((publicacao & lote_4) | (publicacao & lote_5) | (lote_4 & lote_5))

st_write(historico_lotes, dsn = paste(tcltk::tk_choose.dir(caption = "selecionar pasta:", ), "/municipios_atualizados.ods", sep = ""))

historico_lotes <- histo

novos_4 <- setdiff(lote_4, publicacao)

atualiza_4 <- intersect(lote_4, publicacao)

consolida_4 <- union(lote_4, publicacao)

novos_5 <- setdiff(lote_5, consolida_4)

atualiza_5 <- intersect(lote_5, consolida_4)

consolida_5 <- union(lote_5, consolida_4)
