################################################################################
#This code will aggregate historic pnad data
# - Looking only at people who lost their job and then found a new one
################################################################################
library(tidyverse)
library(haven)
library(arrow)
library(readxl)
library(MatchIt)

setwd("../../PNADC/full_parquet/")

#Work with original PNADC files
files = list.files(path = ".")
pnad = tibble()

for (file in files){
  aux = read_parquet(file)
  
  #Select Variables to keep
  #Note, we will look only at position at the main job
  aux = aux %>% 
    rename(
      ano = Ano, trimestre = Trimestre, capital = Capital, rm = RM_RIDE,
      uf = UF, urb_rural = V1022, peso = V1028, sexo = V2007, idade = V2009,
      cor = V2010, estuda = V3002, anos_estudo = VD3005, grupo_estudo = VD3006,
      pea = VD4001, ocupado = VD4002, posicao = VD4009, dom_id = hous_id,
      rendimento_principal = VD4016, num_entrevista = V1016, 
      temp_emprego1 = V40401, temp_emprego2 = V40402, temp_emprego3 = V40403,
      cat_tempo_emprego = V4040
    ) %>% 
    select(c(ano, trimestre, ind_id, dom_id, num_entrevista, capital, rm, uf,
             urb_rural, peso, sexo, idade, cor, estuda, anos_estudo, 
             grupo_estudo, pea, ocupado, posicao,rendimento_principal,
             temp_emprego1, temp_emprego2, temp_emprego3, cat_tempo_emprego)
           ) %>% 
    #One variable to define period
    mutate(anotri = as.integer(paste0(ano, trimestre))) %>% 
    select(!c(ano, trimestre))
  
  #Filter potential candidates
  #Lets filter individuals with characteristics we want for each interview
  aux = aux %>% 
    #indivuduals that are no longer studying and live in a urban area
    filter(estuda ==2 & urb_rural == 1) %>%
    select(!c(estuda, urb_rural)) %>%
    filter(
      #In the first interview, occupied in a formal position
      (num_entrevista == 1 & ocupado == 1 & posicao == 1) | 
        #In the second interview, the person is unemployed but in pea
        (num_entrevista == 2 & ocupado == 2 & pea == 1) |
        #Occupied in the private sector for at least one of the next interviews
        (
          (num_entrevista ==3)| 
            (num_entrevista ==4) | 
            (num_entrevista ==5 & ocupado == 1 & posicao %in% c(1,2))
          )
      )
  
  #Bind in a unique df
  pnad = rbind(pnad, aux)
  
}
rm(aux)

#Some modifications to make life easier
pnad = pnad %>% 
  mutate(ocupado = case_when(ocupado == 1 ~ 1,
                              T ~ 0),
         pea = case_when(pea == 1 ~ 1,
                         T ~ 0))
pnad = pnad %>% 
  mutate(
    trab_privado = case_when(posicao %in% c(1,2) ~ 1,
                              T ~ 0)
  )

#Filter individuals with the exact characteristics we want
pnad = pnad %>% 
  mutate(ind_id = as.character(ind_id)) %>% 
  #filter individuals that appeared in all 5 interviews
  group_by(ind_id) %>% 
  mutate(count = n()) %>% 
  ungroup() %>% 
  filter(count == 5) %>% 
  #drop individuals who were working outside private sector in 3rd and 4th int.
  mutate(indicador1 = ifelse(num_entrevista == 3 & 
                               !posicao %in% c(NA, 1,2), 1,0),
         indicador2 = ifelse(num_entrevista == 4 & 
                               !posicao %in% c(NA, 1,2), 1,0)
         ) %>% 
  group_by(ind_id) %>% 
  mutate(indicador1 = max(indicador1),
         indicador2 = max(indicador2)) %>% 
  ungroup() %>% 
  filter(indicador1 == 0 & indicador2 == 0) %>% 
  select(!c(indicador1, indicador2))

#Deflate wages
setwd("../../Job Displacement")
deflator = read_excel("deflator_inpc.xlsx", sheet = "trimestral")
deflator = deflator %>% 
  select(anotri, deflator) %>% 
  mutate(anotri = as.integer(anotri))

pnad = pnad %>%
  left_join(deflator, by = "anotri", na_matches = "never") %>% 
  mutate(salario_real = rendimento_principal * deflator)
rm(deflator)

#Create a tenure variable
pnad = pnad %>% 
  #Notes: cat_tempo_emprego ==1 means less than 1 month working
  #temp_emprego1 shows months of emp for people working less than 1 year
  #temp_emprego2 shows months of emp for people working between 1 and 2 years
  #temp_emprego3 shows years of employment for people working for more than 2
  mutate(temp_emprego = case_when(cat_tempo_emprego == 1 ~ 0,
                                  !is.na(temp_emprego1) ~ temp_emprego1/12,
                                  !is.na(temp_emprego2) ~ 1 + temp_emprego2/12,
                                  T ~  temp_emprego3)) %>% 
  select(!c(temp_emprego1, temp_emprego2, temp_emprego3, cat_tempo_emprego))


#Select one observation per individual for the matching part
pnad_filtered = pnad %>% 
  group_by(ind_id) %>% 
  mutate(sal_inicial = salario_real[num_entrevista == 1],
         temp_emprego_inicial = temp_emprego[num_entrevista == 1],
         anos_estudo_inicial = anos_estudo[num_entrevista == 1]) %>% 
  ungroup() %>% 
  filter(num_entrevista == 5) %>% 
  mutate(informal = ifelse(posicao == 2, 1,0)) %>% 
  #drop individuals without wage information in interview 5
  filter(!is.na(salario_real)) %>% 
  #at least 3 years of employment
  #filter(temp_emprego >=3) %>% 
  #Create indexes
  rownames_to_column() %>% 
  rename(index = rowname)


#Matching
m1 = matchit(informal ~ temp_emprego_inicial + anos_estudo_inicial + idade + 
               factor(uf) + factor(cor) + factor(sexo),
             data = pnad_filtered)
summary(m1)

#Match matrix with indices for each match
match_matrix = as.data.frame(m1$match.matrix) %>% 
  rownames_to_column() %>% 
  rename(matched_informal = rowname,
         matched_formal = V1)

match_matrix = match_matrix %>% 
  mutate(pair_index = seq(1:nrow(match_matrix)))

#take column values to rows
match_1 = match_matrix %>% 
  select(!c(matched_formal)) %>% 
  rename(index = matched_informal)

match_2 = match_matrix %>% 
  select(!c(matched_informal)) %>% 
  rename(index = matched_formal)

match = rbind(match_1, match_2)

#Take these informations back to main dataset
pnad_filtered = pnad_filtered %>% 
  left_join(match, by = "index", na_matches = "never")

rm(match, match_1, match_2, match_matrix, m1)

#Create a balance table
pnad_filtered = pnad_filtered %>% 
  mutate(homem = ifelse(sexo == 1, 1, 0),
         branco = ifelse(cor == 1, 1, 0))


table1 = pnad_filtered %>% 
  filter(!is.na(pair_index)) %>% 
  group_by(informal) %>% 
  summarise(salario_real = mean(salario_real),
            salario_inicial = mean(sal_inicial),
            idade = mean(idade),
            anos_estudo_inicial  = mean(anos_estudo_inicial),
            temp_emprego_inicial = mean(temp_emprego_inicial),
            share_homem = mean(homem),
            share_branco = mean(branco)) %>% 
  ungroup() %>% 
  mutate(grupo = case_when(informal == 0 ~ "formal matched",
                           informal == 1 ~ "informal")) %>% 
  select(grupo, everything()) %>% 
  select(!c(informal))

table2 = pnad_filtered %>% 
  filter(informal == 0) %>% 
  group_by(informal) %>% 
  summarise(salario_real = mean(salario_real),
            salario_inicial = mean(sal_inicial),
            idade = mean(idade),
            anos_estudo_inicial  = mean(anos_estudo_inicial),
            temp_emprego_inicial = mean(temp_emprego_inicial),
            share_homem = mean(homem),
            share_branco = mean(branco)) %>% 
  ungroup() %>% 
  mutate(grupo = case_when(informal == 0 ~ "formal all",
                           informal == 1 ~ "informal")) %>% 
  select(grupo, everything()) %>% 
  select(!c(informal))

table = t(rbind(table2, table1))
rm(table1, table2)
  
#################
#T tests
################
#WIll always create first for all sample and second for paired one
#Salarios Reais
m1 = t.test(salario_real ~ informal,
       data = pnad_filtered )
m1p = m1$p.value

m2 = t.test(salario_real ~ informal,
       data = pnad_filtered %>%  filter(!is.na(pair_index)))
m2p = m2$p.value

#Salarios Iniciais
m3 = t.test(sal_inicial ~ informal,
            data = pnad_filtered )
m3p = m3$p.value

m4 = t.test(sal_inicial ~ informal,
            data = pnad_filtered %>%  filter(!is.na(pair_index)))
m4p = m4$p.value

