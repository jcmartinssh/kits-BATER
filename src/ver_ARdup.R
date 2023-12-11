# script para verificar e remover Areas de Risco (AR) duplicadas,
# normalizar a classificacao e verificar erros no registro municipal


library(sf)
library(tidyr)
library(dplyr)
library(arrow)
library(stringr)
library(purrr)



# funcao para remover acentuacao e compatibilizar com ASCII usando base R
rm_accent <- function(str, pattern = "all") {
    # Rotinas e funções úteis V 1.0
    # rm.accent - REMOVE ACENTOS DE PALAVRAS
    # Função que tira todos os acentos e pontuações de um vetor de strings.
    # Parâmetros:
    # str - vetor de strings que terão seus acentos retirados.
    # patterns - vetor de strings com um ou mais elementos indicando quais acentos deverão ser retirados.
    #            Para indicar quais acentos deverão ser retirados, um vetor com os símbolos deverão ser passados.
    #            Exemplo: pattern = c("´", "^") retirará os acentos agudos e circunflexos apenas.
    #            Outras palavras aceitas: "all" (retira todos os acentos, que são "´", "`", "^", "~", "¨", "ç")
    if (!is.character(str)) {
        str <- as.character(str)
    }

    pattern <- unique(pattern)

    if (any(pattern == "Ç")) {
        pattern[pattern == "Ç"] <- "ç"
    }

    symbols <- c(
        acute = "áéíóúÁÉÍÓÚýÝ",
        grave = "àèìòùÀÈÌÒÙ",
        circunflex = "âêîôûÂÊÎÔÛ",
        tilde = "ãõÃÕñÑ",
        umlaut = "äëïöüÄËÏÖÜÿ",
        cedil = "çÇ"
    )

    nudeSymbols <- c(
        acute = "aeiouAEIOUyY",
        grave = "aeiouAEIOU",
        circunflex = "aeiouAEIOU",
        tilde = "aoAOnN",
        umlaut = "aeiouAEIOUy",
        cedil = "cC"
    )

    accentTypes <- c("´", "`", "^", "~", "¨", "ç")

    if (any(c("all", "al", "a", "todos", "t", "to", "tod", "todo") %in% pattern)) { # opcao retirar todos
        return(chartr(paste(symbols, collapse = ""), paste(nudeSymbols, collapse = ""), str))
    }

    for (i in which(accentTypes %in% pattern)) {
        str <- chartr(symbols[i], nudeSymbols[i], str)
    }

    return(str)
}

sf_use_s2(FALSE)

###############################################
## seleca de arquivos e colunas de interesse ##
###############################################

# seleciona o arquivo geopackage com a base AR
AR_file <- tcltk::tk_choose.files(caption = "selecionar arquivo de áreas de risco:", multi = FALSE)

Mun_file <- tcltk::tk_choose.files(caption = "selecionar arquivo de municípios:", multi = FALSE)

saida <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# cria lista de camadas para cada arquivo de base
lotes <- st_layers(AR_file)

ver_mun <- st_layers(Mun_file)

# seleciona a camada para cada arquivo de base
lote_layer <- select.list(lotes[[1]], title = "áreas de risco:", graphics = TRUE)

mun_layer <- select.list(ver_mun[[1]], title = "municípios:", graphics = TRUE)

# áreas de risco - colunas com identificadores únicos e dados dos municípios
col_AR <- st_read(AR_file, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = "")) |>
    colnames()

fid_AR <- select.list(col_AR, title = "id unico:", graphics = TRUE)

cod_AR <- select.list(col_AR, title = "código municipal das áreas de risco:", graphics = TRUE)

mun_AR <- select.list(col_AR, title = "nome municipal das áreas de risco:", graphics = TRUE)

col_mun <- st_read(Mun_file, query = paste("SELECT * from ", mun_layer, " LIMIT 0", sep = "")) |>
    colnames()

col_AR_cprm <- select.list(col_AR, multiple = TRUE, title = "colunas de tipoologia da CPRM:", graphics = TRUE)

col_AR_outro <- select.list(col_AR, multiple = TRUE, title = "colunas de tipoologia de outras fontes:", graphics = TRUE)

cl_AR_tipos <- append(col_AR_cprm, col_AR_outro)

cod_mun <- select.list(col_mun, title = "geocódigo dos municipio:", graphics = TRUE)

nom_mun <- select.list(col_mun, title = "nomes dos municípios:", graphics = TRUE)

############################################
## eliminaca de areas de risco duplicadas ##
############################################

# cria lista de municípios da AR
lista_mun <- st_read(AR_file, query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |>
    pull() |>
    as.character()

# carrega o arquivo de AR e corrige a geometria
AR_full <- AR_file |>
    st_read(layer = lote_layer) |>
    st_make_valid()

# cria cópia sem as feições com identificador repitido
AR_nodup_cols <- AR_full |> distinct(across(setdiff(col_AR, fid_AR)), .keep_all = TRUE)
AR_nodup_geo <- AR_full |> distinct(geometry, .keep_all = TRUE)

AR_nodup_diff <- setdiff(AR_nodup_cols, AR_nodup_geo) |>
    mutate(geo_dupli = "sim") |>
    select(geo_dupli)


AR_nodup <- st_join(
    AR_nodup_cols,
    AR_nodup_diff,
    join = st_equals
) |>
    unite(
        col = tipo_unite,
        !!cl_AR_tipos,
        sep = ", ",
        remove = FALSE,
        na.rm = TRUE
    ) |>
    mutate(
        geo_dupli = if_else(
            is.na(geo_dupli),
            "nao",
            geo_dupli
        )
    )


################################
##  classificacao da tipolgia ##
################################

sep_char <- c(
    "\\n",
    ",",
    ";",
    "/",
    "([:space:])(d[aeo])([:space:])",
    "([:space:])(e)([:space:])",
    "([:space:])",
    "([:space:])(em)([:space:])"
)
sep_pat <- paste0(paste(sep_char, collapse = "|"))

tipologias <- AR_nodup |>
    select(c(!!col_AR_cprm, !!col_AR_outro)) |>
    as.data.frame() |>
    summarise(n = n(), .by = c(!!col_AR_cprm, !!col_AR_outro)) #|>

lista_tipos <- list()

for (i in seq_along(cl_AR_tipos)) {
    lista_tipos[[i]] <- tipologias |>
        pull(!!cl_AR_tipos[i]) |>
        na.omit()
}

tipos <- unlist(lista_tipos, use.names = FALSE) |>
    sapply(FUN = rm_accent, USE.NAMES = FALSE) |>
    str_to_lower() |>
    str_split(pattern = sep_pat) |>
    unlist(use.names = FALSE) |>
    unique() |>
    str_trim() |>
    str_subset(".+")

tipos_geo <- select.list(tipos, multiple = TRUE, title = "selecionar palavras associadas a risco geológico", graphics = TRUE)

tipos_hidro <- select.list(tipos, multiple = TRUE, title = "selecionar palavras associadas a risco hidrológico", graphics = TRUE)

pat_geo <- paste("(", paste(tipos_geo, collapse = ")|("), ")", sep = "")

pat_hidro <- paste("(", paste(tipos_hidro, collapse = ")|("), ")", sep = "")

AR_nodup <- AR_nodup |>
    mutate(
        tipo_unite = tipo_unite |> rm_accent() |> str_to_lower(),
        geo_ctrl = if_else(str_detect(tipo_unite, pat_geo), TRUE, FALSE),
        hidro_ctrl = if_else(str_detect(tipo_unite, pat_hidro), TRUE, FALSE),
        tipo_norm = case_when(
            geo_ctrl & hidro_ctrl ~ "misto",
            geo_ctrl & !hidro_ctrl ~ "geologico",
            !geo_ctrl & hidro_ctrl ~ "hidrologico",
            .default = "erro"
        )
    )


###################################################
## Prepara tabela de avaliacao de areas de risco ##
## duplicadas por municipios de registro         ##
###################################################

# cria cópia das camadas e remove a geometria
T_AR_full <- AR_full

st_geometry(T_AR_full) <- NULL

T_AR_nodup <- AR_nodup

st_geometry(T_AR_nodup) <- NULL

gc()

# compila os dados da camada original
original <- T_AR_full |>
    group_by(!!as.symbol(cod_AR)) |>
    summarise(AR_original = n())

# compila os dados da camada sem duplicadas e compoe com dados da camada original
avaliacao <- T_AR_nodup |>
    group_by(!!as.symbol(cod_AR), geo_dupli, tipo_norm) |>
    summarise(n_ar_nodup = n()) |>
    pivot_wider(
        id_cols = c(!!as.symbol(cod_AR), tipo_norm),
        names_from = geo_dupli,
        values_from = n_ar_nodup,
        names_prefix = "ndp_geodp_",
        values_fill = 0
    ) |>
    pivot_wider(
        id_cols = !!as.symbol(cod_AR),
        names_from = tipo_norm,
        values_from = c(ndp_geodp_nao, ndp_geodp_sim),
        values_fill = 0
    ) |>
    full_join(original, join_by(!!cod_AR)) |>
    ungroup() %>%
    mutate(
        cod_mun = as.character(!!as.symbol(cod_AR)),
        AR_unicas = select(., matches("ndp_geodp_nao")) |> rowSums(na.rm = TRUE),
        AR_geodp = select(., starts_with("ndp_geodp_sim")) |> rowSums(na.rm = TRUE),
        AR_duplicadas = AR_original - (AR_unicas + AR_geodp)
    ) |>
    rename(
        AR_geo = ndp_geodp_nao_geologico,
        AR_hidro = ndp_geodp_nao_hidrologico,
        AR_misto = ndp_geodp_nao_misto
    ) |>
    select(
        cod_mun,
        AR_original,
        AR_duplicadas,
        AR_unicas,
        AR_geodp,
        AR_geo,
        AR_hidro,
        AR_misto
    ) #|>
# View()

# carrega a tabela dos municipios do IBGE com geocodigo e nome
municipios <- st_read(Mun_file, query = paste("SELECT ", cod_mun, ", ", nom_mun, " FROM ", mun_layer, sep = "")) |>
    rename(cod_mun = !!cod_mun, nom_mun = !!nom_mun)

# cria a tabela de avaliacao com o nome dos municipios na base do IBGE e o total de AR original e duplicada
avaliacao <- avaliacao |>
    left_join(
        municipios,
        join_by(cod_mun)
    ) |>
    select(
        cod_mun,
        nom_mun,
        AR_original,
        AR_unicas,
        AR_duplicadas,
        AR_geodp,
        AR_geo,
        AR_hidro,
        AR_misto
    )

###################################################
## Prepara a camada para avaliacao de areas de   ##
## risco deslocadas do municipios de registro    ##
###################################################

# associa espacialmente as AR aos municipios
AR_final_mun <- st_join(AR_nodup, municipios, join = st_intersects, largest = TRUE)

# separa as AR cujo registro do municipio difere da sua localizacao espacial
AR_mun_dif <- AR_final_mun |>
    rename(cod_orig = !!cod_AR, mun_orig = !!mun_AR, cod_geop = cod_mun, mun_geop = nom_mun) |>
    mutate(cod_orig = as.character(cod_orig)) |>
    filter(cod_orig != cod_geop)

# compila a tabela de municipios com total por municipio de registro / localizacao
Mun_dif <- AR_mun_dif |>
    select(cod_orig, mun_orig, cod_geop, mun_geop) |>
    group_by(cod_orig, cod_geop, mun_geop) |>
    summarise(mun_orig = first(mun_orig), n_ar = n()) |>
    select(cod_orig, mun_orig, cod_geop, mun_geop, n_ar)

# remove a geometria da tabela de municipios
st_geometry(Mun_dif) <- NULL

############################################
## Exporta as tabelas e as areas de risco ##
############################################

## exporta a camada de AR com registro do municipio diferente da localizacao
st_write(
    AR_mun_dif,
    paste(saida, "/AR_aval_mun.shp", sep = ""),
    append = FALSE
)

## exportar a camada de AR limpas
st_write(
    AR_nodup,
    paste(saida, "/AR_Lote5_semDup.shp", sep = ""),
    append = FALSE
)

## exporta tabela com numero de AR com registro do municipio diferente da localizacao
st_write(
    Mun_dif,
    paste(saida, "/Mun_dif.ods", sep = ""),
    append = FALSE
)

## exporta tabela com numero de AR duplicada por municipio
st_write(
    avaliacao,
    paste(saida, "/avaliacao_dp.ods", sep = ""),
    append = FALSE
)
