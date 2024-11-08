import pandas as pd
import numpy as np
pnad_path = '../data/microdados_limpos_datazoom/pnadcontinua/'

#select columns to keep
col_to_keep = ['Ano', 'Trimestre', 'ind_id', 'hous_id', 'UF', 'Capital', 'V2007', 'V1016', 'V2009', 'V2010',
               'VD3004', 'V4001', 'V4009', 'V4012', 'V4019', 'V4025', 'V4028', 'V4029', 'V1028',
                'V4032', 'V4039', 'V40401', 'V40402', 'V40403', 'V4041', 'V4071', 'VD4010', 
                'VD4017', 'VD4020'] 
#define new column names
new_col_names = {
    'Ano': 'ano',
    'Trimestre': 'tri',
    'UF': 'uf',
    'Capital': 'capital',
    'V1028': 'peso',
    'V1016': 'numero_pesquisa',
    'V2007': 'sexo',
    'V2009': 'idade',
    'V2010': 'cor',
    'VD3004': 'educ',
    'V4001': 'trab_sem',
    'V4009': 'n_empregos',
    'V4012': 'tipo_emprego',
    'V4019': 'tem_cnpj',
    'V4025': 'trab_temporario',
    'V4028': 'trab_publico',
    'V4029': 'carteira_assinada',
    'V4032': 'contrib_prev',
    'V4039': 'hrs_trab_sem',
    'V40401': 'temp_emprego1',
    'V40402': 'temp_emprego2',
    'V40403': 'temp_emprego3',
    'V4041': 'ocupacao',
    'VD4010': 'grupo_ocupacao',
    'V4071': 'proc_emprego',
    'VD4017': 'rendimento_principal',
    'VD4020': 'rendimento_total'
}


#run through pnad files to create agg file
ano_inicial = 2012
ano_final = 2023

pnad_agg = pd.DataFrame()
for ano in range(ano_inicial, ano_final):
    filename = pnad_path + 'PNADC' + str(ano) + '.dta'
    pnad = pd.read_stata(filename, columns = col_to_keep)
    #rename columns
    pnad = pnad.rename(columns = new_col_names)
    #bind to pnad_agg
    pnad_agg = pd.concat([pnad_agg, pnad])

#Function to create full years of employment
def calcular_anos_emprego(row):
    if pd.notna(row['temp_emprego3']):
        return row['temp_emprego3']
    elif pd.notna(row['temp_emprego2']):
        return 1
    elif pd.notna(row['temp_emprego1']):
        return  0 
    else:
        return np.nan
    
pnad_agg['anos_emprego'] = pnad_agg.apply(calcular_anos_emprego, axis = 1)

#Take a look to guarantee its ok
pnad_agg.loc[pnad_agg['temp_emprego1'].isna(), :].head(10).to_string()

#Correct some column
def compat_cols(row):
    output = np.where(row == 1, 1, 0)
    return output

cols_compat = ['trab_sem', 'tem_cnpj', 'trab_temporario', 'trab_publico', 'carteira_assinada', 'contrib_prev', 'proc_emprego']
pnad_agg[cols_compat] = pnad_agg[cols_compat].map(compat_cols)

#drop columns
pnad_agg.drop(['temp_emprego1', 'temp_emprego2', 'temp_emprego3'], axis = 1, inplace = True)

#inpute 0 wages for those not working
pnad_agg['rendimento_principal'] = pnad_agg['rendimento_principal'].fillna(0)
pnad_agg['rendimento_total'] = pnad_agg['rendimento_total'].fillna(0)

#save
pnad_agg.to_parquet("../data/pnad_clean.parquet")
globals().clear()