#######################################
#This code will:
#- use matched individuals data to create main panel
#- run regs
######################################

import pandas as pd
import arrow
import numpy as np

pnad = pd.read_parquet("../data/pnad_clean.parquet")
matched_sample = pd.read_parquet("../data/matched_individuals.parquet")

#Join matched individuals to main dataset
pnad = pnad[pnad['ind_id'].notnull()]
pnad = pnad.merge(matched_sample, on = 'ind_id', how = 'left')

#drop unmatched individuals
pnad = pnad[pnad['match_id'].notnull()]

#define informal job
pnad['informal'] = np.where((pnad['trab_sem'] == 1) &
                            (pnad['tipo_emprego'] == 3) &
                            (pnad['carteira_assinada'] == 0), 1, 0)
#correct n_empregos and hrs_trab_sem
pnad['n_empregos'] = pnad['n_empregos'].fillna(0)
pnad['hrs_trab_sem'] = pnad['hrs_trab_sem'].fillna(0)

#aggregate data for plots
pnad_agg = pnad.groupby(['tratado', 'numero_pesquisa']).agg({
    'trab_sem' : 'mean',
    'informal' : 'mean',
    'hrs_trab_sem' : 'mean',
    'rendimento_principal' : 'mean',
    'rendimento_total' : 'mean',
    'n_empregos': 'mean'
}).reset_index()

treated_agg = pnad_agg[pnad_agg['tratado'] == 1]
control_agg = pnad_agg[pnad_agg['tratado'] == 0]

###############
#Plots
import matplotlib.pyplot as plt

var_to_plot = ['trab_sem', 'informal', 'rendimento_principal', 'hrs_trab_sem']
count = 0
fig, axs = plt.subplots(2, 2, figsize = (10, 6))
for i in range(2):
    for j in range(2):
        axs[i, j].plot(treated_agg['numero_pesquisa'], treated_agg[var_to_plot[count]], label = 'Treated')
        axs[i, j].plot(control_agg['numero_pesquisa'], control_agg[var_to_plot[count]], label = 'Control')
        axs[i, j].set_xlabel('Survey')
        axs[i, j].set_ylabel(var_to_plot[count])
        axs[i, j].set_xticks([1, 2, 3, 4, 5])
        axs[i, j].legend()
        count = count + 1

plt.show()


