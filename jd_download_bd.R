#############################################
#This code will:
#- Download data from Base dos dados
#############################################

#------------------------------------------------------------------------------------------------
#This code will download Data from Base dos Dados
#------------------------------------------------------------------------------------------------

library(basedosdados)
library(tidyverse)
library(arrow)
library(readxl)
library(beepr)
rm(list=ls())

#Define project
set_billing_id("ra-ubi-informality")

#############################
#Political filiation data
#############################
query =  "
SELECT 
cpf, 
registro_filiacao,
sigla_partido,
id_municipio,
situacao_registro, 
data_filiacao,
data_desfiliacao,
FROM basedosdados.br_tse_filiacao_partidaria.microdados
WHERE data_extracao = '2024-10-21';
"

write_parquet(read_sql(query),"../data/filiacao_partidaria.parquet")

gc()
beep()
