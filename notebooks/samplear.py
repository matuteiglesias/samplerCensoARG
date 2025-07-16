#!/usr/bin/env python
# coding: utf-8

# ## Parseo de Argumentos

# In[4]:


import argparse # Importing the argparse module to handle command line arguments

parser = argparse.ArgumentParser()

parser.add_argument('-dbp', '--databasepath', required=True, help='Path to the database')
parser.add_argument('-f','--frac', type=float, default = 0.01, help='Fraction of the sample')
parser.add_argument('-n','--nombre', default= 'ARG', help='Name of the sample')
parser.add_argument('-y','--years', nargs=2, type=int, default=[2021, 2022], help='Years to sample')

distritos = parser.add_mutually_exclusive_group()
distritos.add_argument('-d','--departamentos', nargs='+', type = int, help='Departments to sample')
distritos.add_argument('-p','--provincias', nargs='+', type = int, help='Provinces to sample')

args = parser.parse_args()

censo_DB_path = args.databasepath
frac = args.frac
name = args.nombre
startyr, endyr = args.years

if args.departamentos is None and args.provincias is None:
    total_pais = True

# To show the results of the given option to screen.
for _, value in parser.parse_args()._get_kwargs():
    if value is not None:
        print(value)


# In[5]:


# censo_DB_path = '/media/matias/Elements/suite/ext_CPV2010_basico_radio_pub'
# frac = 0.002
# name = 'ARG_test'
# startyr, endyr = 2015, 2016


# In[6]:


import numpy as np # Importing the numpy module for numerical computations
import pandas as pd # Importing the pandas module for data manipulation and analysis
import os # Importing the os module for interacting with the operating system
import dask.dataframe as dd # Importing the dask.dataframe module for parallel computing on large datasets
from dask.diagnostics import ProgressBar # Importing the ProgressBar class from dask.diagnostics to display progress of computations using dask.dataframe


# In[7]:


# El archivo 'proy_pop200125.csv' contiene la informacion oficial de proyecciones de poblacion por departamento publicada por INDEC
#  ('https://www.indec.gob.ar/ftp/cuadros/poblacion/proyeccion_departamentos_10_25.pdf')
proy_pop = pd.read_csv('./../data/info/proy_pop20012225.csv', encoding = 'utf-8')

# Proyeccion de poblacion por departamento
ratios = proy_pop.set_index(['DPTO', 'NOMDPTO']).div(proy_pop['2010'].values, 0).reset_index()

## Referencia de radios censales segun Censo 2010
radio_ref = pd.read_csv('./../data/info/radio_ref.csv').astype({'DPTO':int, 'PROV':int})


# In[9]:


VIVIENDA = dd.read_csv(censo_DB_path + '/VIVIENDA.csv', sep = ';',
                       usecols = ['VIVIENDA_REF_ID', 'RADIO_REF_ID', 'URP', 'TIPVV', 'V01'])

HOGAR = dd.read_csv(censo_DB_path + '/HOGAR.csv', sep = ';', usecols = ['HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'H05', 'H06', 'H07', 'H08',
        'H09', 'H10', 'H11', 'H12', 'H13', 'H14', 'H15', 'H16', 'PROP', 'TOTPERS']) 

PERSONA = dd.read_csv(censo_DB_path + '/PERSONA.csv', sep = ';', usecols = ['PERSONA_REF_ID', 'HOGAR_REF_ID', 
'P01', 'P02', 'P03', 'P05', 'P06', 'P07', 'P12', 'P08', 'P09', 'P10', 'CONDACT'])


HOGAR = HOGAR.merge(VIVIENDA[['VIVIENDA_REF_ID', 'RADIO_REF_ID']], on='VIVIENDA_REF_ID')

PERSONA = PERSONA.merge(HOGAR[['HOGAR_REF_ID', 'RADIO_REF_ID']], on = 'HOGAR_REF_ID')

geo_vars = ['RADIO_REF_ID', 'DPTO', 'PROV', 'AGLOMERADO']
info = radio_ref[geo_vars]

VIVIENDA = VIVIENDA.merge(info, on = 'RADIO_REF_ID')

HOGAR = HOGAR.merge(info, on = 'RADIO_REF_ID')

PERSONA = PERSONA.merge(info, on = 'RADIO_REF_ID')

## Filtrar por departamento o provincia
def filter_dataframe(df, args):
    if args.departamentos is not None:
        df = df[df.DPTO.isin(args.departamentos)]
    elif args.provincias is not None:
        df = df[df.PROV.isin(args.provincias)]
    else:
        print('total_pais')
    return df

VIVIENDA = filter_dataframe(VIVIENDA, args)
HOGAR = filter_dataframe(HOGAR, args)
PERSONA = filter_dataframe(PERSONA, args)


# In[10]:


# Guardar sampleo del censo
if not os.path.exists('./../data/censo_samples/'):
    os.makedirs('./../data/censo_samples/')


# In[12]:


import dask.dataframe as dd
from dask import delayed, compute



# def sample_func(frac, x, yr):
#     print('debug:', 'frac', frac, 'x.shape', x.shape, 'yr', yr)
#     return x.sample(frac=frac*x[yr].mean())

def sample_func(frac, x, yr):
    # print('debug:', 'frac', frac, 'x.shape', x.shape, 'yr', yr)

    # Convert column if needed
    if yr not in x.columns:
        try:
            yr_int = int(yr)
            if yr_int in x.columns:
                x[yr] = x[yr_int]
        except Exception as e:
            print(f"[warn] Cannot resolve column {yr} in x.columns={x.columns}")
            return x.head(0)

    # Defensive guard: check for valid mean
    m = x[yr].mean()
    if pd.isna(m) or m <= 0:
        print(f"[warn] mean for {yr} is invalid: {m} → skipping sample")
        return x.head(0)

    try:
        return x.sample(frac=frac * m)
    except Exception as e:
        print(f"[error] Sampling failed with frac={frac * m}, shape={x.shape}, error={e}")
        return x.head(0)


import time

# Function to print the current timestamp and elapsed time
def log_message(message, start_time=None):
    current_time = time.time()
    readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
    elapsed_time = f'Elapsed time: {current_time - start_time:.2f} seconds' if start_time else ''
    print(f"[{readable_time}] {message} {elapsed_time}")

log_message("Script started.")

for yr in [str(s) for s in range(startyr, endyr)]:
    log_message(f"Processing year: {yr}")
    
    start_group = time.time()
    grouped = HOGAR[['HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'DPTO']].merge(ratios[['DPTO', yr]]).groupby('DPTO')
    log_message("Grouping completed.", start_group)

    start_sample = time.time()
    meta = {'HOGAR_REF_ID': 'int64', 'VIVIENDA_REF_ID': 'int64', 'DPTO': 'int64', yr: 'int64'}
    sample = grouped.apply(lambda x: sample_func(frac, x, yr), meta=meta).compute()
    log_message("Sampling completed.", start_sample)

    start_sample_processing = time.time()
    viviendas_en_sample = sample.VIVIENDA_REF_ID.unique()
    hogares_en_sample = sample.HOGAR_REF_ID.unique()

    VIVIENDA_sample = VIVIENDA.loc[VIVIENDA.VIVIENDA_REF_ID.isin(viviendas_en_sample)].persist()
    HOGAR_sample = HOGAR.loc[HOGAR.HOGAR_REF_ID.isin(hogares_en_sample)].persist()
    PERSONA_sample = PERSONA.loc[PERSONA.HOGAR_REF_ID.isin(hogares_en_sample)].persist()
    log_message("Sample processing completed.", start_sample_processing)

    start_merge = time.time()
    merge1 = delayed(VIVIENDA_sample.merge)(HOGAR_sample, on = ['VIVIENDA_REF_ID'] + geo_vars)
    merge2 = delayed(merge1.merge)(PERSONA_sample, on = ['HOGAR_REF_ID'] + geo_vars)
    merge2 = merge2.persist()
    log_message("Merging completed.", start_merge)

    start_compute = time.time()
    with ProgressBar():
        df = merge2.compute(num_workers=4)
    log_message("Compute completed.", start_compute)

    start_save = time.time()
    filename = f'./../data/censo_samples/table_f{frac}_{yr}_{name}.csv'
    df.to_csv(filename, index = False, single_file=True)
    log_message(f"Data saved to {filename}.", start_save)

log_message("Script completed.")


# In[ ]:


# for yr in [str(s) for s in range(startyr, endyr)]:
#     print(yr)
#     grouped = HOGAR[['HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'DPTO']].merge(ratios[['DPTO', yr]]).groupby('DPTO')
#     sample = grouped.apply(lambda x: x.sample(frac=frac*x[yr].mean())).compute()

#     viviendas_en_sample = sample.VIVIENDA_REF_ID.unique()
#     hogares_en_sample = sample.HOGAR_REF_ID.unique()

#     with ProgressBar():
#         VIVIENDA_sample = VIVIENDA.loc[VIVIENDA.VIVIENDA_REF_ID.isin(viviendas_en_sample)]
#         HOGAR_sample = HOGAR.loc[HOGAR.HOGAR_REF_ID.isin(hogares_en_sample)]
#         PERSONA_sample = PERSONA.loc[PERSONA.HOGAR_REF_ID.isin(hogares_en_sample)]

#         merge1 = VIVIENDA_sample.merge(HOGAR_sample, on = ['VIVIENDA_REF_ID'] + geo_vars)
#         print(len(merge1))
#         merge2 = merge1.merge(PERSONA_sample, on = ['HOGAR_REF_ID'] + geo_vars)
#         print(len(merge2))
    
#         df = merge2.compute()

#     # Guardar sampleo del censo
#     if not os.path.exists('./../data/censo_samples/'):
#         os.makedirs('./../data/censo_samples/')
        
#     filename = './../data/censo_samples/table_f'+str(frac)+'_'+yr+'_'+name+'.csv'
#     print('saved file: ' + filename)
#     df.to_csv(filename, index = False)

