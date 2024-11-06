########################################
#This code will prepare municipality data to send, by:
#1) Calculate mean wages by municipality
########################################

library(tidyverse)
library(arrow)
library(readxl)

##Parameters
min_age = 24
max_age = 50

data_path = "C:/Users/xande/OneDrive/Documentos/Doutorado/Research/Graduating in a recession/Empirico/Tentativa Censo/Data"
data_path_job_displacement= "C:/Users/xande/OneDrive/Documentos/Doutorado/Research/Job Displacement"


#Useful grouping function
#Group by municipality
group_by_munic <- function(x){
  x <- x %>% 
    group_by(munic) %>% 
    mutate(pop_m = sum(weight, na.rm = T)) %>% 
    ungroup() %>% 
    filter(age >= min_age & age <= max_age) %>% 
    group_by(munic) %>% 
    summarise(informal_m = sum(informal*weight, na.rm=T),
              formal_m = sum(formal*weight, na.rm = T),
              empregado_m = sum(employed * weight, na.rm = T),
              auto_emp_m = sum(self_emp * weight, na.rm=T),
              pea_m = sum(in_pea*weight, na.rm = T),
              pop_m = first(pop_m),
              income_m = sum(inc_main_job, na.rm=T)/sum(employed * weight, na.rm = T), 
              tot_income_m = sum(inc_main_job, na.rm=T)) %>% 
    ungroup() %>% 
    mutate(taxa_informal_m = informal_m/empregado_m,
           taxa_desemprego_m = 1- empregado_m/pea_m)
  return(x)
}

#create a panel for the municipalities in given years and join census years
create_panel_munics <- function(data, year_start, year_end){
  munics = data %>% select(munic) %>% pull() %>% unique()
  
  #expand grid
  df <- expand_grid(municipio = munics,
                    ano = year_start:year_end)
  
  #input data for the census
  df <- df %>% 
    left_join(data, join_by(municipio == munic))
}


##################
#1991
##################
setwd(data_path)
df<- read_parquet("census_91_pes_ibge.parquet") %>% 
  select(age, weight, work_condition,activity_condition, position, signed_booklet, munic,uf) %>% 
  mutate(munic = as.numeric(paste0(uf,munic)))

#Define PEA
df<- df %>% 
  #Employed will be the individals that habitually or eventually worked in the year (note that is is what Ponczeck&Ulyssea also do)
  mutate(employed= ifelse(work_condition %in% c(1,2), 1,0)) %>% 
  #Tried job will be individuals that were looking for a job
  mutate(tried_job = ifelse(activity_condition%in%c(1,2),1,0)) %>% 
  #In PEA will be individuals who are either employed or searched for a job
  mutate(in_pea = ifelse((employed==1|tried_job==1)&age>=16,1,0)) 
gc()


#Formality Status
df<- df %>% 
  #Informal will be employees in the private sector without a signed booklet
  mutate(informal = case_when(employed ==1 & position %in% c(4,6) &signed_booklet%in%c(3)~1,
                              is.na(employed)|is.na(position)|is.na(signed_booklet)~NA_real_,
                              T~0)) %>%
  #Formal will be everyone else that is employed
  mutate(formal = 1-informal*employed) %>% 
  #Self employed
  mutate(self_emp = case_when(employed ==1 & position %in% c(5,9)~1,
                              is.na(employed)|is.na(position)~NA_real_,
                              T ~0))

gc()

#Group by municipality
df_91 <- group_by_munic(df)

#Expand these informations from 1991 to 1999
df_91 <- create_panel_munics(df_91, 1991, 1999)

##################
#2000
##################
setwd(data_path)
df<- read_parquet("census_00_pes_ibge.parquet")

#Define PEA
df<- df %>% 
  #Employed will be individuals who worked even if they did not receive in the ref week
  mutate(employed = ifelse(worked_ref_week==1|npaid1==1|npaid2==1|npaid3==1|npaid4==1,1,0)) %>% 
  #In PEA will be individuals who are either employed or searched for a job
  mutate(in_pea = ifelse((employed==1|tried_job==1)&age>=16,1,0)) 
gc()


#Formality Status
df<- df %>% 
  #InformalC will be employees in the private sector without a signed booklet
  mutate(informal = case_when(employed ==1 & position %in% c(2,4) &public_serv%in%c(2) ~ 1,
                              is.na(employed)|is.na(position)|is.na(public_serv)~NA_real_,
                              T~0)) %>%
  #Formal will be everyone else that is employed
  mutate(formal = 1-informal*employed) %>% 
  #Self employed
  mutate(self_emp = case_when(employed ==1 & position==6 ~ 1,
                              is.na(employed)|is.na(position)~NA_real_,
                              T~0))
gc()

df_00 <- group_by_munic(df)
setwd(data_path_job_displacement)
write_parquet(df_00, "munic_data_2000.parquet")

#Expand these informations from 2000 to 2009
df_00 <- create_panel_munics(df_00, 2000, 2009)


##################
#2010
##################
setwd(data_path)
df<- read_parquet("census_10_pes_ibge.parquet") %>% 
  mutate(munic = as.numeric(paste0(uf, munic)))
gc()


#Define PEA
df<- df %>% 
  #Employed will be individuals who worked even if they did not receive in the ref week
  mutate(employed = ifelse(worked_ref_week==1|npaid1==1|npaid2==1|npaid3==1,1,0)) %>% 
  #In PEA will be individuals who are either employed or searched for a job
  mutate(in_pea = ifelse((employed==1|tried_job==1)&age>=16,1,0)) 
gc()



#Formality Status
df<- df %>% 
  #InformalC will be employees in the private sector without a signed booklet
  mutate(informal = case_when(employed ==1 & position %in% c(4)  ~ 1,
                              is.na(employed)|is.na(position) ~ NA_real_,
                              T ~0)) %>%
  #Formal will be everyone else that is employed
  mutate(formal = 1-informal*employed) %>% 
  #Self employed
  mutate(self_emp = ifelse(position==5,1,0))

df_10 <- group_by_munic(df)

#Expand these informations from 2010 to 2017
df_10 <- create_panel_munics(df_10, 2010, 2017)

rm(df)
gc()


##################
#Bind and additional modifications
##################
#Add year column
df <- rbind(df_91, df_00, df_10)

df <- df %>% arrange(municipio, ano)

rm(df_91, df_00, df_10)
gc()


#Take only first 6 digits of municipality column
df <- df %>% 
  mutate(municipio = as.integer(substr(as.character(municipio), 1,6))) %>% 
  #make some columns as integers
  mutate_at(c("ano", "informal_m", "formal_m", "empregado_m", "auto_emp_m", "pea_m"), ~as.integer(.))




##################
#GDP by municipalities
##################

setwd(data_path_job_displacement)
df_deflator <- read_excel("series_nacionais.xlsx", sheet="anual") %>% 
  select(ano, deflator_pib_2010)

#This function will read excel files, create a gdp panel and calculate gdp_growth
create_gdp_panel <- function(filename){
  setwd(data_path_job_displacement)
  data <- read_excel(filename)
  data <- data %>% 
    mutate_all(as.character) %>% 
    pivot_longer(2:ncol(data), names_to = "ano", values_to = "pib_nominal") %>% 
    mutate(pib_nominal = ifelse(pib_nominal== "...", NA_character_, pib_nominal)) %>% 
    mutate(ano = as.integer(ano)) %>% 
    mutate_all(as.integer)
  
  #deflate gdp
  data <- data %>% 
    left_join(df_deflator, by="ano") %>% 
    mutate(pib_real = pib_nominal*deflator_pib_2010)
  
  #Create a balanced panel for all municipalities and years of the file
  munic = data %>% select(municipio) %>% pull() %>% unique()
  year_min = data %>% select(ano) %>% pull() %>% min()
  year_max = data %>% select(ano) %>% pull() %>% max()
  
  df_aux <- expand_grid(municipio = munic, ano = year_min:year_max)
  
  df_aux <- df_aux %>% 
    left_join(data, by=c("municipio","ano"))
  
  #Calculate real gdp growth
  df_aux <- df_aux %>% 
    arrange(municipio, ano) %>% 
    group_by(municipio) %>% 
    mutate(pib_crescimento = pib_real/lag(pib_real)-1) %>% 
    ungroup() %>% 
    #take only the first 6 characters of the municipality
    mutate(municipio = as.character(municipio),
           municipio = substr(municipio,1,6),
           municipio = as.integer(municipio)) %>% 
    select(municipio, ano, pib_crescimento)
}

#Create GDP Panel for file with old years and new years
df_gdp_old <- create_gdp_panel("pib_antigo.xlsx")
df_gdp_new<- create_gdp_panel("pib_novo.xlsx")


#Now we bind these informations. First we take whatever is possible from the new set of data
#But for years without this information, we take from the od set
munic_new = df_gdp_new %>% select(municipio) %>% pull() %>% unique()
munic_old = df_gdp_old %>% select(municipio) %>% pull() %>% unique()
munics = c(munic_new, munic_old) %>% unique()

df_gdp <- expand_grid(municipio = munics, ano = 2000:2017)

df_gdp <- df_gdp %>% 
  left_join(df_gdp_new, by = c("municipio", "ano")) %>% 
  left_join(df_gdp_old, by = c("municipio", "ano"), suffix= c("_new","_old")) %>% 
  mutate(pib_crescimento_m = ifelse(is.na(pib_crescimento_new), pib_crescimento_old, pib_crescimento_new)) %>% 
  select(municipio,ano, pib_crescimento_m)


#Merge GDP information with census information
df <- df %>% 
  left_join(df_gdp, by = c("municipio", "ano"),na_matches = "never")

#save
setwd(data_path_job_displacement)
write_parquet(df, "munic_data.parquet")

rm(list=ls())
gc()


