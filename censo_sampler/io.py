# coding: utf-8
"""
Data loading utilities — lifted from original script with minimal rearrangement.
"""

from pathlib import Path
import pandas as pd
import dask.dataframe as dd


def load_reference_data():
    """
    Loads:
      - proy_pop (projections)
      - ratios (department-level ratios vs 2010)
      - radio_ref (radio→geo mapping)
    Paths kept relative to this file as in the original codebase.
    """
    # Original relative paths preserved
    base_info = Path(__file__).resolve().parents[1] / 'data' / 'info'

    # Proyecciones de población por departamento (INDEC)
    proy_pop = pd.read_csv(base_info / 'proy_pop20012225.csv', encoding='utf-8')

    # Ratios vs 2010
    ratios = proy_pop.set_index(['DPTO', 'NOMDPTO']).div(proy_pop['2010'].values, 0).reset_index()

    # Referencia de radios censales (Censo 2010)
    radio_ref = pd.read_csv(base_info / 'radio_ref.csv').astype({'DPTO': int, 'PROV': int})

    return proy_pop, ratios, radio_ref


def _attach_geo(df: dd.DataFrame, radio_ref: pd.DataFrame):
    geo_vars = ['RADIO_REF_ID', 'DPTO', 'PROV', 'AGLOMERADO']
    info = radio_ref[geo_vars]
    return df.merge(info, on='RADIO_REF_ID')


def _filter_dataframe(df: dd.DataFrame, departamentos=None, provincias=None):
    if departamentos is not None:
        df = df[df.DPTO.isin(departamentos)]
    elif provincias is not None:
        df = df[df.PROV.isin(provincias)]
    else:
        # parity with original behavior: echo total_pais
        print('total_pais')
    return df


def load_main_tables(censo_DB_path, radio_ref, departamentos=None, provincias=None):
    """
    Loads VIVIENDA, HOGAR, PERSONA with selected columns and attaches geo variables.
    Applies optional filters by DPTO/PROV.
    """
    p = Path(censo_DB_path)

    VIVIENDA = dd.read_csv(
        str(p / 'VIVIENDA.csv'),
        sep=';',
        usecols=['VIVIENDA_REF_ID', 'RADIO_REF_ID', 'URP', 'TIPVV', 'V01']
    )

    HOGAR = dd.read_csv(
        str(p / 'HOGAR.csv'),
        sep=';',
        usecols=[
            'HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'H05', 'H06', 'H07', 'H08',
            'H09', 'H10', 'H11', 'H12', 'H13', 'H14', 'H15', 'H16', 'PROP', 'TOTPERS'
        ]
    )

    PERSONA = dd.read_csv(
        str(p / 'PERSONA.csv'),
        sep=';',
        usecols=[
            'PERSONA_REF_ID', 'HOGAR_REF_ID',
            'P01', 'P02', 'P03', 'P05', 'P06', 'P07', 'P12', 'P08', 'P09', 'P10', 'CONDACT'
        ]
    )

    # Attach RADIO_REF_ID to HOGAR and PERSONA (lifted from original)
    HOGAR = HOGAR.merge(VIVIENDA[['VIVIENDA_REF_ID', 'RADIO_REF_ID']], on='VIVIENDA_REF_ID')
    PERSONA = PERSONA.merge(HOGAR[['HOGAR_REF_ID', 'RADIO_REF_ID']], on='HOGAR_REF_ID')

    # Attach geo columns
    VIVIENDA = _attach_geo(VIVIENDA, radio_ref)
    HOGAR = _attach_geo(HOGAR, radio_ref)
    PERSONA = _attach_geo(PERSONA, radio_ref)

    # Optional filters
    VIVIENDA = _filter_dataframe(VIVIENDA, departamentos, provincias)
    HOGAR = _filter_dataframe(HOGAR, departamentos, provincias)
    PERSONA = _filter_dataframe(PERSONA, departamentos, provincias)

    return VIVIENDA, HOGAR, PERSONA
