#############################################
#This code will:
#Define treated and potential control individuals in order to be matched
#############################################

import pandas as pd
import numpy as np
import pyarrow

pnad = pd.read_parquet("../data/pnad_clean.parquet")

#Parameters
idade_min = 24
idade_max = 55
pesquisa_baseline = 2
tempo_emprego = 2

#Filtering
#Identify individuals working full-time with a formal contract in the baseline period
ind_trab_baseline = pnad[
    (pnad['numero_pesquisa'] == pesquisa_baseline ) &
    (pnad['idade'] >= idade_min) &
    (pnad['idade'] <= idade_max) &
    (pnad['anos_emprego'] >= tempo_emprego) &
    (pnad['trab_sem'] == 1) &
    (pnad['tipo_emprego'] == 3) &
    (pnad['carteira_assinada'] == 1) &
    (pnad['hrs_trab_sem'] >= 30)&
    (pnad['n_empregos'] == 1)
    ]['ind_id'].unique()

#Treated will be the ones not-working in the period right after the baseline
#but who were looking for a job
treated = pnad[
    (pnad['numero_pesquisa'] == pesquisa_baseline + 1) &
    (pnad['idade'] >= idade_min) &
    (pnad['idade'] <= idade_max) &
    (pnad['trab_sem'] == 0) &
    (pnad['proc_emprego'] == 1) &	
    (pnad['ind_id'].isin(ind_trab_baseline))
    ]['ind_id'].unique()

#Potential controls will be the ones that remain working in the period right after the baseline
pot_control = pnad[
    (pnad['numero_pesquisa'] == pesquisa_baseline +1) &
    (pnad['idade'] >= idade_min) &
    (pnad['idade'] <= idade_max) &
    (pnad['anos_emprego'] >= tempo_emprego) &
    (pnad['trab_sem'] == 1) &
    (pnad['tipo_emprego'] == 3) &
    (pnad['carteira_assinada'] == 1) &
    (pnad['hrs_trab_sem'] >= 30) &
    (pnad['ind_id'].isin(ind_trab_baseline))
    ]['ind_id'].unique()


#dataset with displaced individuals
pnad_disp = pnad[(pnad['ind_id'].isin(treated)) &
                 (pnad['numero_pesquisa'] == pesquisa_baseline)]
pnad_disp['tratado'] = 1

#dataset with potential controls
pnad_controls = pnad[(pnad['ind_id'].isin(pot_control)) &
                     (pnad['numero_pesquisa'] == pesquisa_baseline)]
pnad_controls['tratado'] = 0

#concatenate
pnad_baseline = pd.concat([pnad_disp, pnad_controls])

#Save
pnad_baseline.to_parquet("../data/potential_sample.parquet")
globals().clear()

import gc
gc.collect()