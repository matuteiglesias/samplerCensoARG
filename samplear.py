### PARSEO DE ARGUMENTOS

import argparse
import numpy as np
import pandas as pd
import os
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

os.chdir(os.path.dirname(__file__))


parser = argparse.ArgumentParser()
# parser.add_argument('-i', action='append', nargs='+')

parser.add_argument('-dbp', '--databasepath', nargs=1, required=True)
parser.add_argument('-f','--frac', nargs='+', help='<Required> Set flag', required=False, type=float, default = 0.01)
parser.add_argument('-n','--nombre', nargs=1, required=False, default= 'ARG')
parser.add_argument('-y','--years', nargs='+', help='<Required> Set flag', required=False, type=int, default=[2021, 2022])

distritos = parser.add_mutually_exclusive_group(required=False)
distritos.add_argument('-d','--departamentos', nargs='+', type = int)
distritos.add_argument('-p','--provincias', nargs='+', type = int)


args = parser.parse_args()

if (args.departamentos is None and args.provincias is None):
	total_pais = True


censo_DB_path = args.databasepath[0]
frac = args.frac[0]
name = args.nombre[0]
startyr = args.years[0]
endyr = args.years[1]

# To show the results of the given option to screen.
for _, value in parser.parse_args()._get_kwargs():
    if value is not None:
        print(value)
        
        
#### PROCESAMIENTO DE DATA

## Argumentos:



## El path donde tenemos los archivos PERSONA, HOGAR y VIVIENDA
# censo_DB_path = '/media/miglesia/Elements/suite/'

# El archivo 'proy_pop200125.csv' contiene la informacion oficial de proyecciones de poblacion por departamento publicada por INDEC
#  ('https://www.indec.gob.ar/ftp/cuadros/poblacion/proyeccion_departamentos_10_25.pdf')
proy_pop = pd.read_csv('./data/info/proy_pop200125.csv', encoding = 'utf-8')
proy_pop.head(2)


## Referencia de radios censales segun Censo 2010
radio_ref = pd.read_csv('./data/info/radio_ref.csv')
radio_ref['DPTO'] = radio_ref['DPTO'].astype(int)
radio_ref['PROV'] = radio_ref['PROV'].astype(int)


#Esto es para extraer las viviendas, hogares y personas de los partidos (DPTOs) en cuestion.
# seleccion_DPTOS y usecols nos sirven para no cargar data innecesaria.


VIVIENDA = dd.read_csv(censo_DB_path + '/VIVIENDA.csv', sep = ';', usecols = ['VIVIENDA_REF_ID', 'RADIO_REF_ID', 'TIPVV', 'V01'])
VIVIENDA = VIVIENDA.merge(radio_ref[['RADIO_REF_ID', 'DPTO', 'PROV']])

if args.departamentos is not None:
    VIVIENDA = VIVIENDA.loc[VIVIENDA.DPTO.isin(args.departamentos)]
    
elif args.provincias is not None:
    VIVIENDA = VIVIENDA.loc[VIVIENDA.PROV.isin(args.provincias)]
   
else:
    print('total_pais')
#    VIVIENDA_ = VIVIENDA.loc[VIVIENDA.DPTO.isin(seleccion_distritos)]


HOGAR = dd.read_csv(censo_DB_path + '/HOGAR.csv', sep = ';', usecols = ['HOGAR_REF_ID', 'VIVIENDA_REF_ID']) # csv is too big, so it is dask-loaded. Not sure it's efficient thou

with ProgressBar():
    HOGAR_DPTO = HOGAR.merge(VIVIENDA[['VIVIENDA_REF_ID', 'DPTO']]).compute()
    
# grouped = HOGAR_DPTO.merge(proyeccion[['DPTO', 'ratio_18']]).groupby('DPTO')
# ratios = proy_pob.div(proy_pob['2010'], 0).reset_index()
ratios = proy_pop.set_index(['DPTO', 'NOMDPTO']).div(proy_pop['2010'].values, 0).reset_index()

PERSONA = dd.read_csv(censo_DB_path + '/PERSONA.csv', sep = ';', usecols = ['PERSONA_REF_ID', 'HOGAR_REF_ID', 'P01', 'P02', 'P03', 'P05', 'P06',
       'P07', 'P12', 'P08', 'P09', 'P10', 'CONDACT'])
    
for yr in [str(s) for s in range(startyr, endyr)]:
    print(yr)
    grouped = HOGAR_DPTO.merge(ratios[['DPTO', yr]]).groupby('DPTO')
    sample = grouped.apply(lambda x: x.sample(frac=frac*x[yr].mean()))

    HOGAR = dd.read_csv(censo_DB_path + '/HOGAR.csv', sep = ';', usecols = ['HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'H05', 'H06', 'H07', 'H08',
           'H09', 'H10', 'H11', 'H12', 'H13', 'H14', 'H15', 'H16', 'PROP', 'TOTPERS']) 

    VIVIENDA_sample = VIVIENDA.loc[VIVIENDA.VIVIENDA_REF_ID.isin(sample.VIVIENDA_REF_ID)]
    HOGAR_sample = HOGAR.loc[HOGAR.HOGAR_REF_ID.isin(sample.HOGAR_REF_ID)]

    tabla_censo = VIVIENDA_sample.merge(HOGAR_sample)
#     tabla_censo = tabla_censo.merge(IX_TOT) # se mergea mas adelante

    with ProgressBar():
        table = tabla_censo.compute()

    # saber de que aglo es el hogar.
    table = table.merge(radio_ref[['RADIO_REF_ID','AGLOMERADO']]) 

    PERSONA_sample = PERSONA.loc[PERSONA.HOGAR_REF_ID.isin(sample.HOGAR_REF_ID)]


    with ProgressBar():
        table = table.merge(PERSONA_sample.compute())

    # Adaptamos las categorias de respuestas para que iguales las de la EPH
    table['P07'] = table['P07'].map({1:1, 2:2, 0:2})
    
    ## Calcula tamano del hogar
    IX_TOT = table['HOGAR_REF_ID'].value_counts().reset_index(); IX_TOT.columns = ['HOGAR_REF_ID', 'IX_TOT_']
    table = table.merge(IX_TOT)

    # Guardar sampleo del censo
    
    if not os.path.exists('./data/censo_samples/'):
        os.makedirs('./data/censo_samples/')
        
    table.to_csv('./data/censo_samples/table_f'+str(frac)+'_'+yr+'_'+name+'.csv', index = False)

