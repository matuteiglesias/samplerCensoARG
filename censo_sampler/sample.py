# coding: utf-8
"""
Sampling routines — this preserves your original groupby.apply + sample() logic.
We’ll replace with a deterministic Bernoulli approach in the next step.
"""

import time
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from dask.diagnostics import ProgressBar


def _log_message(message, start_time=None):
    current_time = time.time()
    readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
    elapsed_time = f'Elapsed time: {current_time - start_time:.2f} seconds' if start_time else ''
    print(f"[{readable_time}] {message} {elapsed_time}")


def _sample_func(frac, x, yr):
    # Defensive: resolve year column if provided as str vs int
    if yr not in x.columns:
        try:
            yr_int = int(yr)
            if yr_int in x.columns:
                x[yr] = x[yr_int]
        except Exception:
            print(f"[warn] Cannot resolve column {yr} in x.columns={list(x.columns)}")
            return x.head(0)

    m = x[yr].mean()
    if pd.isna(m) or m <= 0:
        print(f"[warn] mean for {yr} is invalid: {m} → skipping sample")
        return x.head(0)

    try:
        return x.sample(frac=frac * m)
    except Exception as e:
        print(f"[error] Sampling failed with frac={frac * m}, shape={x.shape}, error={e}")
        return x.head(0)


def run_groupby_sampler_over_years(
    VIVIENDA: dd.DataFrame,
    HOGAR: dd.DataFrame,
    PERSONA: dd.DataFrame,
    ratios: pd.DataFrame,
    frac: float,
    startyr: int,
    endyr: int,  # NOTE: kept half-open [start, end) as in original script
    name: str,
    out_dir
):
    """
    Mirrors the original loop:
      for yr in [str(s) for s in range(startyr, endyr)]:
        - groupby DPTO (HOGAR merged with ratios)
        - sample
        - filter VIVIENDA/HOGAR/PERSONA
        - merge and compute
        - write CSV
    """

    for yr in [str(s) for s in range(startyr, endyr)]:
        _log_message(f"Processing year: {yr}")

        start_group = time.time()
        grouped = (
            HOGAR[['HOGAR_REF_ID', 'VIVIENDA_REF_ID', 'DPTO']]
            .merge(ratios[['DPTO', yr]])
            .groupby('DPTO')
        )
        _log_message("Grouping completed.", start_group)

        start_sample = time.time()
        meta = {'HOGAR_REF_ID': 'int64', 'VIVIENDA_REF_ID': 'int64', 'DPTO': 'int64', yr: 'int64'}
        sample = grouped.apply(lambda x: _sample_func(frac, x, yr), meta=meta).compute()
        _log_message("Sampling completed.", start_sample)

        viviendas_en_sample = sample.VIVIENDA_REF_ID.unique()
        hogares_en_sample = sample.HOGAR_REF_ID.unique()

        # VIVIENDA_sample = VIVIENDA.loc[VIVIENDA.VIVIENDA_REF_ID.isin(viviendas_en_sample)].persist()
        # HOGAR_sample = HOGAR.loc[HOGAR.HOGAR_REF_ID.isin(hogares_en_sample)].persist()
        # PERSONA_sample = PERSONA.loc[PERSONA.HOGAR_REF_ID.isin(hogares_en_sample)].persist()


        # Build dask DataFrames of keys (1 partition is fine for unique keys)
        start_sample_processing = time.time()

        # Build small Dask DF of selected keys (1 partition)
        hogar_sel = dd.from_pandas(pd.DataFrame({"HOGAR_REF_ID": hogares_en_sample}), npartitions=10)
        viv_sel   = dd.from_pandas(pd.DataFrame({"VIVIENDA_REF_ID": viviendas_en_sample}), npartitions=10)

        with ProgressBar():
            t0 = time.time()
            HOGAR_sample = HOGAR.merge(hogar_sel, on="HOGAR_REF_ID", how="inner").persist()
            _log_message("HOGAR semi-join completed.", t0)

            t1 = time.time()
            PERSONA_sample = PERSONA.merge(hogar_sel, on="HOGAR_REF_ID", how="inner").persist()
            _log_message("PERSONA semi-join completed.", t1)

            t2 = time.time()
            VIVIENDA_sample = VIVIENDA.merge(viv_sel, on="VIVIENDA_REF_ID", how="inner").persist()
            _log_message("VIVIENDA semi-join completed.", t2)

        _log_message("Sample processing completed.", start_sample_processing)


        start_merge = time.time()
        geo_vars = ['RADIO_REF_ID', 'DPTO', 'PROV', 'AGLOMERADO']
        merge1 = delayed(VIVIENDA_sample.merge)(HOGAR_sample, on=['VIVIENDA_REF_ID'] + geo_vars)
        merge2 = delayed(merge1.merge)(PERSONA_sample, on=['HOGAR_REF_ID'] + geo_vars)
        merge2 = merge2.persist()
        _log_message("Merging completed.", start_merge)

        start_compute = time.time()
        with ProgressBar():
            df = merge2.compute(num_workers=4)
        _log_message("Compute completed.", start_compute)

        # Write CSV (same filename format as original)
        from .export import write_year_csv
        write_year_csv(df, frac=frac, year=yr, name=name, out_dir=out_dir)
