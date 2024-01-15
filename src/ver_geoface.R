library(sf)
library(data.table)
library(parallel)

# variavel com hora de inicio para medir o tempo de execucao
inicio <- Sys.time()

# desativa geometria esferica
sf_use_s2(FALSE)

# define o numero de nucleos de processamento
# deixando alguns livres para nao travar a maquina
num_cores <- floor(detectCores() * .875)

# define numero de threads pra ser usado com o data.table
setDTthreads(num_cores)


# filtro de formatos de arquivos
filtro <- matrix(c("Geopackage", "*.gpkg", "Parquet", "*.parquet", "Shapefile", "*.shp", "All files", "*"),
  4, 2,
  byrow = TRUE
)

# escolhe diretório de saida
output <- tcltk::tk_choose.dir(caption = "diretório de saída:")

# seleciona o arquivo com a base de faces com geometria
faces_arq <- tcltk::tk_choose.files(
  caption = "arquivo das faces com geometria:",
  multi = FALSE,
  filters = filtro
)

# seleciona o arquivo com a base de setores censitarios
setores_arq <- tcltk::tk_choose.files(
  caption = "arquivo dos setores censitários:",
  multi = FALSE,
  filters = filtro
)


# cria lista de camadas do arquivo de faces
faces_ver <- st_layers(faces_arq)

# cria lista de camadas do arquivo de setores
setores_ver <- st_layers(setores_arq)

# seleciona a camada de faces
faces_layer <- select.list(faces_ver[[1]], title = "base de faces:", graphics = TRUE)

# seleciona a camada de setores
setores_layer <- select.list(setores_ver[[1]], title = "base de setores:", graphics = TRUE)

# carrega estrutura da camada de faces - sem feicoes
faces_col <- read_sf(faces_arq, query = paste("SELECT * FROM ", faces_layer, " LIMIT 0", sep = ""))

# determina nome da coluna com geometria da camada de faces
faces_geom <- attr(faces_col, "sf_column")

# extrai o nome das colunas da camada de faces
faces_col <- faces_col |>
  colnames()

# seleciona coluna com o identificador das faces da camada de faces
faces_id <- select.list(faces_col, title = "Identificador unico das faces:", graphics = TRUE)

# seleciona coluna com o geocodigo das faces da camada de faces
faces_geocod <- select.list(faces_col, title = "Geocodigo das faces:", graphics = TRUE)

# seleciona coluna com o geocodigo dos setores nas camada de faces
faces_stcod <- select.list(faces_col, title = "codigo dos setores nas faces:", graphics = TRUE)

# seleciona coluna com o geocodigo das faces da camada de faces
faces_qdcod <- select.list(faces_col, title = "codigo seq das quadras:", graphics = TRUE)

# seleciona coluna com o geocodigo das faces da camada de faces
faces_fccod <- select.list(faces_col, title = "codigo seq das faces:", graphics = TRUE)

# carrega estrutura da camada de setores - sem feicoes
setores_col <- st_read(setores_arq, query = paste("SELECT * FROM ", setores_layer, " LIMIT 0", sep = ""))

# determina nome da coluna com geometria da camada de faces
setores_geom <- attr(setores_col, "sf_column")

# extrai lista de colunas da camada de setores
setores_col <- setores_col |>
  colnames()

# seleciona coluna com o geocodigo dos setores da camada de setores
setores_geocod <- select.list(setores_col, title = "Geocodigo dos setores:", graphics = TRUE)

# seleciona todas as colunas menos as com nome tipico de geometria para compor string usada na selecao SQL
faces_col_sql <- faces_col[faces_col != faces_geom] |> paste(collapse = ", ")

# carrega os arquivos de face de logradouro sem a geometria
# usando LIMIT para testar
faces <- read_sf(faces_arq, query = paste("SELECT ", faces_col_sql, " FROM ", faces_layer, " LIMIT 1000000"))

# pra garantir que não tem a coluna com geometria
st_geometry(faces) <- NULL

# transforma num data.table
setDT(faces)

# define a chave
setkeyv(faces, faces_geocod)

# conta numero de faces por geocodigo
faces[, CONT := .N, by = faces_geocod]

## tentar melhorar essa verificacao, pra ficar mais geral
# extrai as faces com os campos de geocodigo bem formados
# pedir os nomes dessas colunas e armazenar em variaveis - get(faces_qdcod), get(faces_fccod)
faces_integra <- (
  faces
  [nchar(get(faces_geocod)) == 21 | (nchar(get(faces_qdcod)) == 3 & nchar(get(faces_fccod)) == 3)]
  [nchar(get(faces_geocod)) == 21 & !(substr(get(faces_geocod), 19, 19) == " " | (substr(get(faces_geocod), 16, 16) == " " & substr(get(faces_geocod), 20, 20) == " "))]
  [substr(get(faces_geocod), 16, 21) != "000000" | (get(faces_qdcod) != "000" & get(faces_fccod) != "000")]
  [substr(get(faces_geocod), 16, 21) != "999999" | (get(faces_qdcod) != "999" & get(faces_fccod) != "999")]
  [substr(get(faces_geocod), 16, 21) != "888888" | (get(faces_qdcod) != "888" & get(faces_fccod) != "888")]
  [get(faces_qdcod) != "777"]
  [!(get(faces_qdcod) == "00" & (is.na(get(faces_geocod)) | get(faces_fccod) == "00"))]
  [!(get(faces_fccod) == "999" | get(faces_fccod) == "888" | get(faces_fccod) == "777")]
  [get(faces_geocod) == paste(get(faces_stcod), get(faces_qdcod), get(faces_fccod), sep = "")]
)

# extrai das faces com geocodigo bem formado aquelas com geocodigo unico - sem duplicadas
faces_perf <- (
  faces_integra
  [CONT == 1]
)

# separa as faces que apresentam geocodigo duplicado das faces com geocodigo bem formado
# seria melhor fazer usando CONT > 1?
faces_dp <- fsetdiff(faces_integra, faces_perf, all = TRUE)

# separa as faces com problemas na formacao do geocodigo
faces_problema <- fsetdiff(faces, faces_integra, all = TRUE)

# remove os objetos nao mais necessarios para liberar memoria
rm(faces)
rm(faces_integra)
gc()

# extrai das faces com problema no geocodigo aquelas com quadra e face bem formados mas sem codigo do setor
faces_georec <- (
  faces_problema
  [is.na(get(faces_stcod)) & (nchar(get(faces_qdcod)) == 3 & nchar(get(faces_fccod)) == 3)]
  [!(get(faces_qdcod) %in% c("\n00", "000", "777", "888", "999") | get(faces_fccod) %in% c("\n00", "000", "777", "888", "999"))]
)

# separa as faces sem codigo de quadra e face bem formados das faces problema
faces_irrec <- fsetdiff(faces_problema, faces_georec, all = TRUE)

# remove os objetos nao mais necessarios para liberar memoria
rm(faces_problema)
gc()

# cria um campo para classificacao e preenche os grupos segundo sua condicao
faces_georec[, status_geo := "recuperavel"]
faces_irrec[, status_geo := "invalida"]
faces_dp[, status_geo := "duplicada"]
faces_perf[, status_geo := "perfeita"]

# cria lista de IDs de faces para carregar geometria
id_geom <- faces_georec[[faces_id]]

# transforma a lista em uma string para ser usada em SQL
id_geom_sql <- id_geom |> paste(collapse = ", ")

# carrega a geometria das faces passiveis de recuperacao do geocodigo
faces_geometria <- read_sf(faces_arq,
  query = paste(
    "SELECT ", faces_id, ", ", faces_geom, " FROM ",
    faces_layer, " WHERE ", faces_id, " IN (", id_geom_sql, ")"
  )
)

# transforma num data.table
setDT(faces_geometria)

# faz o join das faces recuperaveis com a geometria pelo campo de identificador unico
faces_georec <- faces_georec[faces_geometria, on = faces_id]

setDF(faces_georec)

# recupera as funcoes espaciais transformando num sf e corrige problemas de geometria
faces_georec <- faces_georec |>
  st_as_sf() |>
  st_make_valid()

# remove os objetos nao mais necessarios para liberar memoria
rm(faces_geometria)
gc()

# carrega os setores censitarios, corrige a geometria, e filtra para os que interseccionam as faces recuperaveis
setores_censitarios <- read_sf(
  setores_arq,
  query = paste("SELECT ", setores_geocod, ", ", setores_geom, " FROM ", setores_layer)
) |>
  st_make_valid() |>
  st_filter(faces_georec)

# limpa a memoria das ultimas operacoes
gc()


# define a funcao para associar os dados dos setores as faces espacialmente
func_map_sf <- function(id_face, face_geo = faces_georec, base_setor = setores_censitarios) {
  # carrega a face
  face <- face_geo[face_geo[[faces_id]] == id_face, ]

  # carrega o(s) setor(es) que intersecciona(m) a face - em tese so deve ser um
  # separar previamente os setores aparentemente acelera o join espacial
  setor <- base_setor[face, op = st_intersects]

  # carrega os dados do setor na face associando espacialmente pela maior intersecao
  face <- st_join(face, setor, join = st_intersects, left = TRUE, largest = TRUE)

  # remove a geometria para aliviar memoria
  st_geometry(face) <- NULL

  # retorna a face com dados do setor
  return(face)
}

## PROCESSAMENTO PARALELIZADO

# cria o cluster para processamento
cl_proc <- makeCluster(num_cores)

# exporta as variaveis para os clusters
clusterExport(cl_proc, c("faces_id", "id_geom", "func_map_sf", "faces_georec", "setores_censitarios"))

# carrega as bibliotecas utilizadas no cluster
clusterEvalQ(cl_proc, {
  library(sf)
})

# aplica a funcao de geoprocessamento de maneira paralelizada para associar as faces aos setores e reune num data.table
faces_georec <- parLapply(cl = cl_proc, X = id_geom, fun = func_map_sf) |> rbindlist()

# finaliza o cluster
stopCluster(cl_proc)

# variavel com hora do fim para medir o tempo de execucao
fim <- Sys.time()

# calcula tempo decorrido no processamento
tempo <- fim - inicio

# registra classificacao das faces que ficaram sem registro de geocodigo do setor
# em tese nao precisa, todas as faces devem estar associadas a um setor censitario
faces_georec[is.na(setores_geocod), status_geo := "invalida"]

# preenche os campos de geocodigo da face
(
  faces_georec
  [status_geo == "recuperavel", (faces_stcod) := get(setores_geocod)]
  [status_geo == "recuperavel", (faces_geocod) := do.call(paste0, .SD), .SDcols = c(faces_stcod, faces_qdcod, faces_fccod)]
  [, (setores_geocod) := NULL]
)

# reune os quatro grupos de faces classificadas
faces_geo_aval <- rbindlist(list(faces_georec, faces_irrec, faces_dp, faces_perf))

# verifica duplicatas e reclassifica as faces recuperadas
(
  faces_geo_aval
  [, CONT := NULL]
  [, CONT := .N, by = faces_geocod]
  [status_geo == "recuperavel" & CONT == 1, status_geo := "recuperada"]
  [status_geo == "recuperavel" & CONT > 1, status_geo := "duplicada"]

)

# remove os objetos nao mais necessarios para liberar memoria
rm(faces_georec, faces_irrec, faces_dp, faces_perf)
gc()

# salva o arquivo com o nome da camada na pasta de saida no formato parquet
write_sf(faces_geo_aval, paste(output, "/", faces_layer, "_aval.parquet", sep = ""), driver = "Parquet")
