# library(fuzzyjoin)
library(dplyr)
library(stringr)
library(sf)
# library(arrow)

# Script  para carregar o geocodigo do municipio pela UF e nome

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
  if (!is.character(str))
    str <- as.character(str)
  
  pattern <- unique(pattern)
  
  if (any(pattern == "Ç"))
    pattern[pattern == "Ç"] <- "ç"
  
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
  
  if (any(c("all", "al", "a", "todos", "t", "to", "tod", "todo") %in% pattern))
    # opcao retirar todos
    return(chartr(
      paste(symbols, collapse = ""),
      paste(nudeSymbols, collapse = ""),
      str
    ))
  
  for (i in which(accentTypes %in% pattern))
    str <- chartr(symbols[i], nudeSymbols[i], str)
  
  return(str)
}

AR_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/AreasDeRisco.gpkg"
lotes <- st_layers(AR_gpkg)
lote_layer <- select.list(lotes[[1]], title = "lote:", graphics = TRUE)
AR_lote <- st_read(AR_gpkg, layer = lote_layer) |> st_zm(drop = TRUE, what = "ZM")
Ar_lote_col <- colnames(AR_lote)
nome_lote_col <- select.list(Ar_lote_col, title = "nomes dos municípios:", graphics = TRUE)
uf_lote_col <- select.list(Ar_lote_col, title = "sigla das UFs:", graphics = TRUE)

AR_lote <- AR_lote |>
  mutate(chave1 = paste(get(uf_lote_col), trimws(str_to_lower(rm_accent(get(nome_lote_col)))), sep = " "),
         chaveA = trimws(str_to_lower(rm_accent(get(nome_lote_col)))))

municipios_gpkg <- "W:/DGC_ACERVO_CGEO/PROJETOS_EM_ANDAMENTO/Cemaden/DESENVOLVIMENTO/_GPKG/BASES/MUNICIPIOS.gpkg"
municipios_ver <- st_layers(municipios_gpkg)
municipios_layer <- select.list(municipios_ver[[1]], title = "base de municipios:", graphics = TRUE)
municipios <- st_read(municipios_gpkg, layer = municipios_layer)
mun_col <- colnames(municipios)
nome_mun_col <- select.list(mun_col, title = "nomes dos municípios:", graphics = TRUE)
cod_mun_col <- select.list(mun_col, title = "códigos dos municípios:", graphics = TRUE)

municipios <- municipios %>%
  mutate(cod_uf = substr(get(cod_mun_col), 1, 2)) |>
  mutate(sigla_uf = case_when(cod_uf == "11" ~ "RO", cod_uf == "12" ~ "AC", cod_uf == "13" ~ "AM", cod_uf == "14" ~ "RR", cod_uf == "15" ~ "PA",
                              cod_uf == "16" ~ "AP", cod_uf == "17" ~ "TO",
                              cod_uf == "21" ~ "MA", cod_uf == "22" ~ "PI", cod_uf == "23" ~ "CE", cod_uf == "24" ~ "RN", cod_uf == "25" ~ "PB",
                              cod_uf == "26" ~ "PE", cod_uf == "27" ~ "AL", cod_uf == "28" ~ "SE", cod_uf == "29" ~ "BA",
                              cod_uf == "31" ~ "MG", cod_uf == "32" ~ "ES", cod_uf == "33" ~ "RJ", cod_uf == "35" ~ "SP",
                              cod_uf == "41" ~ "PR", cod_uf == "42" ~ "SC", cod_uf == "43" ~ "RS",
                              cod_uf == "50" ~ "MS", cod_uf == "51" ~ "MT", cod_uf == "52" ~ "GO", cod_uf == "53" ~ "DF")) |>
  mutate(chaveA = paste(sigla_uf, trimws(str_to_lower(rm_accent(get(nome_mun_col)))), sep = " ")) |>
  mutate(chaveB = trimws(str_to_lower(rm_accent(get(nome_mun_col)))))

UF <- unique(municipios$sigla_uf)


areas_risco_codmun <- stringdist_left_join(AR_Lote, municipios, by = c("chave1" = "chave2"), max_dist = 0)

##############################################
## tentar implementar como função recursiva ##
##############################################

# areas_risco_assoc1 <- areas_risco_codmun %>%
#   filter(!is.na(chave2))
# 
# areas_risco_N_assoc1 <- areas_risco_codmun %>%
#   filter(is.na(chave2)) %>%
#   select(-mun_col)
# 
# areas_risco_assoc2 <- list()
# 
# for (i in UF) {
#   areas <- filter(areas_risco_N_assoc1, UF == i)
#   municipios <- filter(municipios_2021, SIGLA == i)
#   associado <- stringdist_left_join(areas, municipios, by = c("chaveA" = "chaveB"), max_dist = 1)
#   areas_risco_assoc2[[i]] = associado
# }
# 
# areas_risco_assoc2 <- bind_rows(areas_risco_assoc2)
# 
# areas_risco_N_assoc2 <- areas_risco_assoc2 %>%
#   filter(is.na(chaveB)) %>%
#   select(-all_of(col_municipios)) %>%
#   mutate(chaveA = case_when(chaveA == "vicencia\r\nvicencia" ~ "vicencia",
#                             chaveA == "visconde do rio brancovisconde do rio branco" ~ "visconde do rio branco",
#                             chaveA == "maranacau" ~ "maracanau"))
# 
# areas_risco_assoc2 <- areas_risco_assoc2 %>%
#   filter(!is.na(chaveB))
# 
# areas_risco_assoc3 <- list()
# 
# for (i in UF) {
#   areas <- filter(areas_risco_N_assoc2, UF == i)
#   municipios <- filter(municipios_2021, SIGLA == i)
#   associado <- stringdist_left_join(areas, municipios, by = c("chaveA" = "chaveB"), max_dist = 1)
#   areas_risco_assoc3[[i]] = associado
# }
# 
# areas_risco_assoc3 <- bind_rows(areas_risco_assoc3)
# 
# areas_risco_associadas <- bind_rows(areas_risco_assoc1, areas_risco_assoc2, areas_risco_assoc3)
# 
# colunas_apagar <- c("SIGLA", "CLASSE", "chave1", "chaveA", "chave2", "chaveB")
# 
# areas_risco_hidrologico <- areas_risco_associadas %>%
#   filter(CLASSE == "HIDRO") %>%
#   select(-all_of(colunas_apagar))
# 
# areas_risco_mov_massa <- areas_risco_associadas %>%
#   filter(CLASSE == "GEO") %>%
#   select(-all_of(colunas_apagar))
# 
# st_write(areas_risco_hidrologico, dsn = "../../result/merge_rh_julho2022_ultimaversao_codIBGE.shp", layer_options = "ENCODING=UTF-8", delete_layer = TRUE)
# 
# st_write(areas_risco_mov_massa, dsn = "../../result/merge_mm_julho22_codIBGE.shp", layer_options = "ENCODING=UTF-8", delete_layer = TRUE)