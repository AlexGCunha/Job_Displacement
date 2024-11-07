#############################################
#This code will:
#- Match Treated and control individuals from PNAD
#############################################

library(tidyverse)
library(MatchIt)
library(arrow)

pnad = read_parquet('../data/potential_sample.parquet') %>% 
  select(-c("__index_level_0__"))


#auxiliary function to retrieve matched pairs after matching procedure
retrieve_pairs = function(df, model){
  #create matching matrix, with info about matching pairs
  match_matrix = as.data.frame(model$match.matrix)
  match_matrix = match_matrix %>% 
    rownames_to_column() %>% 
    rename(paired_treated = rowname,
           paired_control = V1) %>% 
    #Add a label to this pair of matches
    mutate(match_id = seq(1:nrow(match_matrix))) %>% 
    #drop non-matched individuals
    filter(!is.na(paired_control))
  
  #Add matching information back to matching dataset
  df = df %>% 
    left_join(match_matrix %>% select(!paired_control),
              join_by(rowname==paired_treated),
              na_matches = 'never') %>% 
    left_join(match_matrix %>% select(!paired_treated),
              join_by(rowname == paired_control),
              na_matches = 'never') %>% 
    mutate(match_id = ifelse(is.na(match_id.x), match_id.y, match_id.x)) %>% 
    select(ind_id, match_id)
  
  return(df)
}

#Drop individuals with missing info
pnad = pnad %>% 
  filter(!is.na(n_empregos), !is.na(grupo_ocupacao), !is.na(anos_emprego))

#Matching proocedure
df_match = pnad %>% rownames_to_column()

#Coerceded Exact Matching
# m1 = matchit(tratado ~ rendimento_principal + idade  + anos_emprego  +
#                factor(grupo_ocupacao) + factor(ano) + factor(tri)  + factor(sexo) ,
#              method = "cem",
#              cutpoints = list(
#                rendimento_principal = seq(0, max(df_match$rendimento_principal),250),
#                idade = seq(20, 60, 5),
#                anos_emprego = seq(0, max(df_match$anos_emprego), 2)),
#              data = df_match,
#              k2k = TRUE)

#Propensity Score matching
m1 = matchit(tratado ~ rendimento_principal + idade  + anos_emprego  +
               factor(grupo_ocupacao)  + factor(uf),
             exact = c("ano", "tri", "sexo"),
             method = "nearest",
             data = df_match)


print(summary(m1))

match_pairs = retrieve_pairs(df_match, m1)

#add to main df
pnad = pnad %>% 
  left_join(match_pairs, by = "ind_id", na_matches = "never") %>% 
  filter(!is.na(match_id))

match_info = pnad %>% 
  select(ind_id, tratado, match_id)

write_parquet(match_info, "../data/matched_individuals.parquet")
rm(list = ls())
gc()
