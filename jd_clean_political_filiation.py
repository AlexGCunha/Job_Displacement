#############################################
#This code will:
#Work on political filiation data downloaded from basedosdados and initially cleaned by calebe
#############################################
import pandas as pd
import numpy as np
import gc
import os


input_dir = "../data/calebe_data/tse/"
output_dir = "../data/calebe_data/tse_parquet/"

#create directory if not yet creaed
os.makedirs(output_dir, exist_ok = True)

# for filename in os.listdir(input_dir):
#     if filename.endswith(".csv"):
#         old_path = input_dir + filename
#         new_path = output_dir + filename.replace('.csv', '.parquet')

#         df = pd.read_csv(old_path)
#         df.to_parquet(new_path)
#         del(df)


#open data
cleanfil = pd.read_parquet(output_dir + 'affiliation_clean.parquet')

#drop null cpf
cleanfil = cleanfil[(cleanfil['cpf'].notnull()) &
                    (cleanfil['data_filiacao'].notnull())]

#select column to keep
col_to_keep = ['cpf', 'sigla_partido', 'id_municipio', 'data_filiacao', 'data_saida']
cleanfil = cleanfil.loc[:, col_to_keep]

#create a balanced panel indicating if individuals were filliated to any party on a year
dates = pd.date_range(start = '2000', end = '2017', freq = 'YE')
cpfs = cleanfil['cpf'].unique()

balpanel = pd.DataFrame([(cpf, year) for cpf in cpfs for year in dates],
                        columns = ['cpf', 'year'])

#merge with original dataset
balpanel = balpanel.merge(cleanfil, on = 'cpf', how = 'left')

#if there is no data_saida, input current date
balpanel['data_saida'] = balpanel['data_saida'].fillna('2024-12-31')

#define if the person was filiated to any party on that year
balpanel['filiated'] = np.where((balpanel['data_filiacao'] <= balpanel['year']) &
                                (balpanel['data_saida'] >= balpanel['year']), 1, 0)

# Drop the original 'data_filiacao' and 'data_saida' columns
balpanel = balpanel.drop(columns=['data_filiacao', 'data_saida'])


#save
balpanel.to_parquet((output_dir + "filliation_panel.parquet"))