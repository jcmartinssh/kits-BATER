library(fuzzyjoin)
library(dplyr)
library(stringr)
library(sf)
library(arrow)

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

AR_gpkg <- "../../areas_de_risco/areas_risco.gpkg"

municipios_2021 <-
  read_parquet("../../../src/municipios_2021.parquet") %>%
  mutate(chave2 = str_c(SIGLA, trimws(str_to_lower(rm_accent(NM_MUN))), sep = " ")) %>% 
  mutate(chaveB = trimws(str_to_lower(rm_accent(NM_MUN))))

UF <- unique(municipios_2021$SIGLA)

col_municipios <- colnames(municipios_2021)

AR_mov_massa <- st_read(dsn = AR_gpkg, layer = "merge_mm_julho22") %>%
  mutate(chave1 = str_c(UF, trimws(str_to_lower(rm_accent(MUNICIPIO))), sep = " "),
         chaveA = trimws(str_to_lower(rm_accent(MUNICIPIO))),
         CLASSE = "GEO")

AR_mov_massa <- st_zm(AR_mov_massa, drop = TRUE, what = "ZM")

AR_hidrologico <- st_read(dsn = AR_gpkg, layer = "merge_rh_julho2022_ultimaversao") %>%
  mutate(chave1 = str_c(UF, trimws(str_to_lower(rm_accent(MUNICIPIO))), sep = " "),
         chaveA = trimws(str_to_lower(rm_accent(MUNICIPIO))),
         CLASSE = "HIDRO")

AR_hidrologico <- st_zm(AR_hidrologico, drop = TRUE, what = "ZM")

areas_risco <- bind_rows(AR_mov_massa, AR_hidrologico)

areas_risco_codmun <- stringdist_left_join(areas_risco, municipios_2021, by = c("chave1" = "chave2"), max_dist = 0)

areas_risco_assoc1 <- areas_risco_codmun %>%
  filter(!is.na(chave2))

areas_risco_N_assoc1 <- areas_risco_codmun %>%
  filter(is.na(chave2)) %>%
  select(-col_municipios)

areas_risco_assoc2 <- list()

for (i in UF) {
  areas <- filter(areas_risco_N_assoc1, UF == i)
  municipios <- filter(municipios_2021, SIGLA == i)
  associado <- stringdist_left_join(areas, municipios, by = c("chaveA" = "chaveB"), max_dist = 1)
  areas_risco_assoc2[[i]] = associado
}

areas_risco_assoc2 <- bind_rows(areas_risco_assoc2)

areas_risco_N_assoc2 <- areas_risco_assoc2 %>%
  filter(is.na(chaveB)) %>%
  select(-all_of(col_municipios)) %>%
  mutate(chaveA = case_when(chaveA == "vicencia\r\nvicencia" ~ "vicencia",
                            chaveA == "visconde do rio brancovisconde do rio branco" ~ "visconde do rio branco",
                            chaveA == "maranacau" ~ "maracanau"))

areas_risco_assoc2 <- areas_risco_assoc2 %>%
  filter(!is.na(chaveB))

areas_risco_assoc3 <- list()

for (i in UF) {
  areas <- filter(areas_risco_N_assoc2, UF == i)
  municipios <- filter(municipios_2021, SIGLA == i)
  associado <- stringdist_left_join(areas, municipios, by = c("chaveA" = "chaveB"), max_dist = 1)
  areas_risco_assoc3[[i]] = associado
}

areas_risco_assoc3 <- bind_rows(areas_risco_assoc3)

areas_risco_associadas <- bind_rows(areas_risco_assoc1, areas_risco_assoc2, areas_risco_assoc3)

colunas_apagar <- c("SIGLA", "CLASSE", "chave1", "chaveA", "chave2", "chaveB")

areas_risco_hidrologico <- areas_risco_associadas %>%
  filter(CLASSE == "HIDRO") %>%
  select(-all_of(colunas_apagar))

areas_risco_mov_massa <- areas_risco_associadas %>%
  filter(CLASSE == "GEO") %>%
  select(-all_of(colunas_apagar))

st_write(areas_risco_hidrologico, dsn = "../../result/merge_rh_julho2022_ultimaversao_codIBGE.shp", layer_options = "ENCODING=UTF-8", delete_layer = TRUE)

st_write(areas_risco_mov_massa, dsn = "../../result/merge_mm_julho22_codIBGE.shp", layer_options = "ENCODING=UTF-8", delete_layer = TRUE)