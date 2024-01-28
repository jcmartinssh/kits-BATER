## SCRIPT para verificacao e remocao de feicoes de areas de risco duplicadas,
## normalizacao da classificacao e verificacao de erros no registro municipal


############################
## configuracoes iniciais ##
############################

# variavel com hora de inicio para medir o tempo de execucao
inicio <- Sys.time()

# carrega as bibliotecas - versoes utilizadas anotadas em comentario
# R versao 4.3.2
library(sf)
library(tidyr)
library(dplyr)
library(stringr)
library(purrr)
library(ggplot2)

# define diretorio de trabalho
# necessario para carregar as funcoes externas
# so funciona se estiver rodando o script inteiro
# em caso de rodar passo a passo, definir manualmente
setwd(
    getSrcDirectory(function(x) {
        x
    })
)

# carrega funcoes externas
source("funcoes_comuns.R")

# desativa geometria esferica
sf_use_s2(FALSE)


################################################
## selecao de arquivos e colunas de interesse ##
################################################


# filtro de formatos de arquivos
filtro <- matrix(c("Geopackage", "*.gpkg", "Shapefile", "*.shp", "All files", "*"),
    3, 2,
    byrow = TRUE
)

# seleciona o diretorio de saida
saida <- choose_directory(caption = "diretório de saída:")


####################
## areas de risco ##


# seleciona o arquivo com a base de areas de risco
AR_file <- choose_file(
    caption = "arquivo de áreas de risco:",
    multi = FALSE,
    filters = filtro
)

# cria lista de camadas do arquivo de areas de risco
lotes <- st_layers(AR_file)

# seleciona a camada de areas de risco
lote_layer <- select.list(lotes[[1]], title = "base de áreas de risco:", graphics = TRUE)

# carrega a estrutura da camada de areas de risco
col_AR <- st_read(AR_file, query = paste("SELECT * from ", lote_layer, " LIMIT 0", sep = ""))

# determina nome da coluna com geometria das ARs
geom_AR <- attr(col_AR, "sf_column")

# extrai o nome das colunas das ARs
col_AR <- col_AR |>
    colnames()

# seleciona campos com identificador unico das areas de risco
fid_AR <- select.list(col_AR, title = "AR - campos de IDs:", multiple = TRUE, graphics = TRUE)

# seleciona a coluna com o geocodigo do municipio na area de risco
cod_AR <- select.list(col_AR, title = "AR - geocódigo município:", graphics = TRUE)

# seleciona a coluna com o nome do municipio na area de risco
mun_AR <- select.list(col_AR, title = "AR - nome município:", graphics = TRUE)

# seleciona as colunas principais de tipologia de risco do CPRM
col_AR_cprm <- select.list(col_AR, multiple = TRUE, title = "AR - tipologia CPRM:", graphics = TRUE)

# seleciona colunas de tipologia de risco de outras fontes
col_AR_outro <- select.list(col_AR, multiple = TRUE, title = "AR - outras tipologias:", graphics = TRUE)

# cria a lista de colunas com tipologias de risco
cl_AR_tipos <- append(col_AR_cprm, col_AR_outro)


################
## municipios ##


# seleciona o arquivo com a base de municipios
Mun_file <- choose_file(
    caption = "arquivo de municípios:",
    multi = FALSE,
    filters = filtro
)

# cria lista de camadas do arquivo municipios
ver_mun <- st_layers(Mun_file)

# seleciona a camada de municipios
mun_layer <- select.list(ver_mun[[1]], title = "base de municípios:", graphics = TRUE)

# carrega a estrutura da camada de municipios
col_mun <- st_read(Mun_file, query = paste("SELECT * from ", mun_layer, " LIMIT 0", sep = ""))

# determina nome da coluna com geometria das ARs
geom_mun <- attr(col_mun, "sf_column")

# extrai o nome das colunas dos municipios
col_mun <- col_mun |>
    colnames()

# seleciona a coluna com o geocodigo dos municipios
cod_mun <- select.list(col_mun, title = "municípios - geocódigo:", graphics = TRUE)

# seleciona a coluna com o nome dos municipios
nom_mun <- select.list(col_mun, title = "municípios - nome:", graphics = TRUE)


############################################
## eliminaca de areas de risco duplicadas ##
## e separacao de multipoligonos          ##
############################################


# cria lista de municípios no registro das ARs
lista_mun <- st_read(AR_file, query = paste("SELECT DISTINCT ", cod_AR, " FROM ", lote_layer)) |>
    pull() |>
    as.character()

# carrega o arquivo de ARs, corrige a geometria
AR_full <- AR_file |>
    st_read(layer = lote_layer) |>
    st_make_valid()

# separa areas de risco sem geometria
AR_empty <- AR_full %>%
    filter(st_is_empty(.)) |>
    mutate(geom_orig = "vazia")

# remove areas de risco sem geometria e explode multipoligonos em poligonos
AR_full <- AR_full %>%
    filter(!st_is_empty(.)) %>%
    mutate(geom_orig = case_when(
        st_geometry_type(.) == "POLYGON" ~ "poligono",
        st_geometry_type(.) == "MULTIPOLYGON" ~ "multipoligono",
        .default = "invalida"
    )) |>
    st_cast("MULTIPOLYGON") |>
    st_cast("POLYGON", do_split = TRUE)

# retorna as feicoes sem geometria as areas de risco
AR_full <- AR_full |>
    bind_rows(AR_empty)

# remove feicoes duplicadas verificando todos os campos - menos o identificador unico
AR_nodup_cols <- AR_full |> distinct(across(setdiff(col_AR, fid_AR)), .keep_all = TRUE)

# remove feicoes com geometria duplicada
AR_nodup_geo <- AR_full |> distinct(.data[[geom_AR]], .keep_all = TRUE)

# classifica as feicoes com somente geometria duplicada
# pega pela diferenca entre as feicoes sem as duplicadas
# em todos os campos pelas feicoes sem geometria duplicada
AR_nodup_diff <- setdiff(AR_nodup_cols, AR_nodup_geo) |>
    mutate(geo_dupli = "sim") |>
    select(geo_dupli)

# une a camada de ARs nao duplicadas com as com geometria duplicada demarcadas
# atraves de uniao espacial, dessa maneira todas as instancias das feicoes com
# geometria duplicada serao classificadas como tal - uma vez que nao sabemos qual
# seria a verdadeira -  e classifica as nao duplicadas
AR_nodup <- st_join(
    AR_nodup_cols,
    AR_nodup_diff,
    join = st_equals
) |>
    mutate(
        geo_dupli = if_else(
            is.na(geo_dupli),
            "nao",
            geo_dupli
        )
    )

# cria um campo concatenando todos os registros de tipologia das ARs e normaliza
AR_nodup <- AR_nodup |>
    unite(
        col = tipo_unite,
        !!cl_AR_tipos,
        sep = ", ",
        remove = FALSE,
        na.rm = TRUE
    ) |>
    mutate(
        tipo_unite = tipo_unite |> rm_accent() |> str_to_lower()
    )


################################
##  classificacao da tipolgia ##
################################


# define caracteres e expressoes para separar palavras-chave - em regex
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

# agrega em um padrao regex
sep_pat <- paste0(paste(sep_char, collapse = "|"))

# cria tabela com as combinacoes unicas dos campos de tipologia
# isso aqui pode jogar pro bloco de selecao de arquivos e colunas
# e puxar as combinacoes das tipologias com o SELECT DISTINCT do SQL
tipologias <- AR_nodup |>
    select(c(!!col_AR_cprm, !!col_AR_outro)) |>
    as.data.frame() |>
    summarise(n = n(), .by = c(!!col_AR_cprm, !!col_AR_outro)) #|>

# cria lista vazia para receber as palavras-chave para classificacao
lista_tipos <- list()

# extrai todos os valores dos campos da tipologia para a lista
for (i in seq_along(cl_AR_tipos)) {
    lista_tipos[[i]] <- tipologias |>
        pull(!!cl_AR_tipos[i]) |>
        na.omit()
}

# reune os valores da lista, normaliza, separa em termos usando
# o padrao regex definido e mantem apenas os valores unicos
tipos <- unlist(lista_tipos, use.names = FALSE) |>
    sapply(FUN = rm_accent, USE.NAMES = FALSE) |>
    str_to_lower() |>
    str_split(pattern = sep_pat) |>
    unlist(use.names = FALSE) |>
    unique() |>
    str_trim() |>
    str_subset(".+")

# seleciona palavras-chave exclusivas de risco geologico
tipos_geo <- select.list(tipos, multiple = TRUE, title = "GEOLÓGICO:", graphics = TRUE)

# seleciona palavras-chave exclusivas de risco hidrologico
tipos_hidro <- select.list(tipos, multiple = TRUE, title = "HIDROLÓGICO:", graphics = TRUE)

# transforma as palavras-chave de risco geologico em padrao regex
pat_geo <- paste("(", paste(tipos_geo, collapse = ")|("), ")", sep = "")

# transforma as palavras-chave de risco hidrologico em padrao regex
pat_hidro <- paste("(", paste(tipos_hidro, collapse = ")|("), ")", sep = "")

# registra se as ARs apresentam risco geologico e/ou hidrologico e classifica
AR_nodup <- AR_nodup |>
    mutate(
        geo_ctrl = if_else(str_detect(tipo_unite, pat_geo), TRUE, FALSE),
        hidro_ctrl = if_else(str_detect(tipo_unite, pat_hidro), TRUE, FALSE),
        tipo_norm = case_when(
            geo_ctrl & hidro_ctrl ~ "misto",
            geo_ctrl & !hidro_ctrl ~ "geologico",
            !geo_ctrl & hidro_ctrl ~ "hidrologico",
            .default = "erro"
        )
    )


# ###################################################
# ## Prepara tabela de avaliacao de areas de risco ##
# ## duplicadas por municipios de registro         ##
# ###################################################


# cria cópia da camada original
T_AR_full <- AR_full

# remove a geometria da copia
st_geometry(T_AR_full) <- NULL

# cria cópia da camada sem duplicadas
T_AR_nodup <- AR_nodup

# remove a geometria da copia
st_geometry(T_AR_nodup) <- NULL

gc()

# agrega os dados da camada original por municipio
original <- T_AR_full |>
    group_by(!!as.symbol(cod_AR)) |>
    summarise(AR_original = n())

# agrega os dados da camada sem duplicadas por municipio e compoe com dados da camada original
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
    )

# carrega a tabela dos municipios do IBGE com geocodigo e nome
municipios <- st_read(Mun_file, query = paste("SELECT ", cod_mun, ", ", nom_mun, ", ", geom_mun, " FROM ", mun_layer, sep = "")) |>
    rename(cod_mun = !!cod_mun, nom_mun = !!nom_mun)

# cria a tabela de avaliacao com o nome dos municipios na base do IBGE
# e os agregados da camada de AR original e da camada sem duplicadas
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


# ###################################################
# ## Prepara a camada para avaliacao de areas de   ##
# ## risco deslocadas dos municipios de registro   ##
# ###################################################


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


# ## exporta a camada de AR com registro do municipio diferente para analise
st_write(
    AR_mun_dif,
    paste(saida, "/", lote_layer, "_erro_mun.gpkg", sep = ""),
    append = FALSE
)

## exportar a camada de AR limpas para producao
st_write(
    AR_nodup,
    paste(saida, "/", lote_layer, "_proc.gpkg", sep = ""),
    append = FALSE
)

## exporta tabela com numero de ARs com registro do municipio diferente da localizacao
st_write(
    Mun_dif,
    paste(saida, "/", lote_layer, "_erro_mun.ods", sep = ""),
    append = FALSE
)

## exporta tabela com numero de ARs duplicadas por municipio
st_write(
    avaliacao,
    paste(saida, "/", lote_layer, "_duplicadas.ods", sep = ""),
    append = FALSE
)
