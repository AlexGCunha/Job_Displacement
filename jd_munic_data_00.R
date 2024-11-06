########################################
#This code will:
#- Create some variables at the municipality level based on 2000 census
########################################

library(tidyverse)
library(arrow)
library(readxl)


data_path = "C:/Users/xande/OneDrive/Documentos/Doutorado/Research/Graduating in a recession/Empirico/Tentativa Censo/Data/"
data_path_job_displacement= "C:/Users/xande/OneDrive/Documentos/Doutorado/Research/Job Displacement/"

df = read_parquet(paste0(data_path, "census_10_pes_ibge.parquet"))
sample = head(df, 10000)


#Define municipality
df = df %>% 
  mutate(munic = as.numeric(paste0(uf, munic)),
         munic = substr(munic, 1, 6))

#Define PEA
df = df %>% 
  #Employed will be individuals who worked even if they did not receive in the ref week
  mutate(employed = ifelse(worked_ref_week==1|npaid1==1|npaid2==1|npaid3==1,1,0)) %>% 
  #In PEA will be individuals who are either employed or searched for a job
  mutate(in_pea = ifelse((employed==1|tried_job==1)&age>=16,1,0)) 
gc()



#Formality Status
df = df %>% 
  #InformalC will be employees in the private sector without a signed booklet
  mutate(informal = case_when(employed ==1 & position %in% c(4)  ~ 1,
                              is.na(employed)|is.na(position) ~ NA_real_,
                              T ~0)) %>%
  #Formal will be everyone else that is employed
  mutate(formal = 1-informal*employed) %>% 
  #Self employed
  mutate(self_emp = ifelse(position==5,1,0))

#Instruction level
df = df %>% 
  mutate(
    lths = ifelse(instruct_level <= 2, 1, 0),
    hs_some_college = ifelse(instruct_level == 3, 1, 0),
    college_more = ifelse(instruct_level == 4, 1, 0))


#Income
df = df %>% 
  mutate_at(c('inc_main_job','value_other_income'),~ifelse(is.na(.),0,.)) %>% 
  mutate(wage_total = inc_main_job+value_other_income) 
gc()


#aggregate at municipality level
agg = df %>% 
  group_by(munic) %>% 
  summarise(pop_m = sum(weight),
            employed_m = sum(employed * weight, na.rm = T),
            informal_m = sum(informal * weight, na.rm = T),
            pea_m = sum(in_pea*weight, na.rm = T),
            tot_income_m = sum(wage_total*weight, na.rm = T),
            lths_m = sum(lths*weight, na.rm = T),
            hs_some_college_m = sum(hs_some_college * weight, na.rm = T),
            college_more_m = sum(college_more * weight, na.rm = T)) %>% 
  ungroup()

#additional variable creation
agg = agg %>% 
  mutate(inf_rate_m = informal_m/employed_m,
         unem_rate_m = 1 - employed_m/pea_m,
         lths_rate_m = lths_m/pop_m,
         hs_rate_m = hs_some_college_m/pop_m,
         college_rate_m = college_more_m/pop_m,
         mean_income_m = tot_income_m/employed_m)


write_parquet(agg, paste0(data_path_job_displacement,"munic_data_10.parquet"))
rm(list = ls())
gc()
