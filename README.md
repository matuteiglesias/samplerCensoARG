# Census Sampler (Argentina 2010)

Este repositorio proporciona una herramienta de línea de comandos para generar muestras aleatorias reproducibles de hogares del **Censo de Población y Vivienda de Argentina 2010**.

El muestreador lee las tablas de microdatos (`PERSONA.csv`, `HOGAR.csv`, `VIVIENDA.csv`, etc.), aplica ratios de proyección y genera los conjuntos de datos muestreados en formato CSV (predeterminado) o Parquet.

---

This repository provides a command-line tool to generate reproducible random samples of households from the **2010 Argentina Population and Housing Census**.

The sampler reads the microdata tables (`PERSONA.csv`, `HOGAR.csv`, `VIVIENDA.csv`, etc.), applies projection ratios, and outputs sampled datasets in CSV (default) or Parquet.

---

## Data requirements

You must obtain the census microdata files beforehand. Two common sources:

* INDEC 2010 Census REDATAM export (see unofficial mirrors such as [DatAR](http://datar.info/dataset/censo-nacional-de-poblacion-hogares-y-viviendas-2010-cd-redatam))
* REDATAM-to-CSV converter for Windows: [aacademica.org/conversor.redatam](https://www.aacademica.org/conversor.redatam)

Place the extracted files (e.g. `PERSONA.csv`, `HOGAR.csv`, `VIVIENDA.csv`) in a local directory, e.g. `/path/to/ext_CPV2010_basico_radio_pub`.

---

## Installation

Clone this repository and install dependencies:

```bash
git clone https://github.com/matuteiglesias/samplerCensoARG.git
cd samplerCensoARG
pip install -r requirements.txt
```

---

## Usage

Run via the CLI module:

```bash
python -m censo_sampler.cli -dbp /path/to/ext_CPV2010_basico_radio_pub [options]
```

### Options

* `-dbp, --databasepath PATH`
  Path to the directory containing the census CSVs. **Required**.

* `-f, --frac FLOAT`
  Fraction of households to sample. Default = `0.01` (1%).

* `-y, --years START END`
  Year interval (half-open: includes START, excludes END). Example: `2015 2021`.

* `-n, --nombre STR`
  Tag to include in output filename (e.g. geographic area or experiment name).

* `-d, --departamentos ID [ID ...]`
  Restrict sampling to given department codes. Codes are listed in `./data/info/dptos.csv`.

* `-p, --provincias ID [ID ...]`
  Restrict sampling to given province codes. Codes are listed in `./data/info/provs.csv`.

* `--out-dir PATH`
  Output directory. Defaults to `./data/censo_samples`.

---

## Examples

Sample 5% of households nationwide (current year):

```bash
python -m censo_sampler.cli -dbp /path/ext_CPV2010_basico_radio_pub -f 0.05
```

Sample 5% of households between 2015 and 2020:

```bash
python -m censo_sampler.cli -dbp /path/ext_CPV2010_basico_radio_pub -f 0.05 -y 2015 2021
```

Sample 5% of households in Buenos Aires City (province code 2) for year 2020:

```bash
python -m censo_sampler.cli -dbp /path/ext_CPV2010_basico_radio_pub -f 0.05 -p 2 -y 2020 2021
```

Sample 5% of households in Pilar and Escobar (dept codes 6638, 6252) for year 2020:

```bash
python -m censo_sampler.cli -dbp /path/ext_CPV2010_basico_radio_pub -f 0.05 -d 6638 6252 -y 2020 2021
```

Sample 1% of households nationwide and tag output with “prueba”:

```bash
python -m censo_sampler.cli -dbp /path/ext_CPV2010_basico_radio_pub -n prueba
```

---

## Output

* Files are written to the output directory (`--out-dir`), default `./data/censo_samples`.
* Filenames follow:

  ```
  table_f{frac}_{year}_{nombre}.csv
  ```
* Parquet output will be added in future versions (CSV is always available).

---

## Attribution

This software uses data originally produced by **INDEC (Instituto Nacional de Estadística y Censos, Argentina)**, Census 2010.
The publisher of this repository is not affiliated with INDEC.

