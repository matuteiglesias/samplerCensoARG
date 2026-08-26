# Muestreador del Censo 2010 — Argentina

Este repositorio produce **muestras reproducibles de hogares del Censo Nacional de Población, Hogares y Viviendas 2010** a partir de una copia local autorizada de los microdatos.

La interfaz recomendada es la release inmutable `research.census-sample/v1`: selecciona hogares de forma determinística, conserva a todas las personas de cada hogar seleccionado, asigna identificadores estables, separa probabilidad de inclusión de políticas de proyección y publica QA, manifiesto y checksums verificables.

## Interfaz actual: release gobernada

La unidad de selección es el **hogar**. La selección Bernoulli usa SHA-256 sobre `seed + household_id + stratum`, por lo que es estable frente al orden de filas y particiones. Los identificadores publicados son namespaced y reproducibles:

```text
cpv2010:<namespace>:household:<source-household-id>
cpv2010:<namespace>:person:<source-person-id>
```

La release canónica contiene:

- `persons.csv` con `sample_person_id`, `sample_household_id`, sexo, edad, radio censal y peso;
- `households.csv` con `sample_household_id`, departamento 2010, región y peso;
- manifiesto de release con identidad de fuente, parámetros de selección y políticas;
- QA de cobertura, cardinalidad y muestreo;
- checksums SHA-256 para verificación offline.

El productor **no copia los microdatos fuente dentro de la release**. Los archivos CPV originales y las releases generadas permanecen fuera de Git.

### Requisitos de datos

La ejecución necesita una exportación local de CPV-2010 con, como mínimo:

```text
VIVIENDA.csv
HOGAR.csv
PERSONA.csv
```

También necesita un archivo de geografía determinístico con:

```text
RADIO_REF_ID
radio_2010_id
department_2010_id
region_id
```

`region_id` debe pertenecer a una de las seis regiones soportadas por el contrato actual de canastas: `gran_buenos_aires`, `cuyo`, `noreste`, `noroeste`, `pampeana` o `patagonia`.

El repositorio no distribuye los microdatos del Censo. El operador es responsable de obtenerlos y tratarlos de acuerdo con sus condiciones de acceso, privacidad y uso.

### Producción de una release

```bash
python -m censo_sampler.cli release \
  --databasepath "$CENSUS_DB_PATH" \
  --geography /secure/local/GEOGRAPHY.csv \
  --fraction 0.01 \
  --seed 20260804 \
  --analysis-period 2024-Q1 \
  --name ARG \
  --weight-policy cpv2010_frame_inverse_probability \
  --output-root /local/releases/census
```

Para una integración explícita puede agregarse `--handoff-dir`; el handoff es una copia física, nunca un symlink hacia un repositorio hermano.

Use `--departments` para un subconjunto departamental explícito y `--max-households` como límite operativo local.

La política histórica `legacy_department_projection_candidate` sigue disponible como opción explícita. Los factores de proyección no cambian silenciosamente la fracción muestral: el manifiesto distingue probabilidad de inclusión, peso inverso y política de proyección.

### Verificación offline

```bash
make check
make test
make sample-release-fixture
make sample-release-check RELEASE_DIR=/path/to/release
```

La verificación falla de forma cerrada ante, entre otros casos:

- tablas o columnas requeridas ausentes;
- IDs fuente duplicados o vacíos;
- personas huérfanas o hogares sin vivienda;
- radios sin geografía;
- regiones inválidas;
- pesos no positivos;
- rutas de salida inseguras;
- una release o checksum que no coincide con su manifiesto.

## Privacidad y límites de uso

Este proyecto contiene **código de muestreo**, no autoridad sobre los microdatos censales ni una estimación oficial de población actual.

- No suba microdatos CPV ni muestras con información sensible a Git.
- Mantenga source data, releases y handoffs en almacenamiento local/controlado.
- Una muestra proyectada desde CPV-2010 no es una población observada en un período posterior.
- Los identificadores publicados en la release son identificadores de investigación namespaced; no reemplazan los identificadores nativos de la fuente.
- La release v1 está diseñada para downstreams que necesitan identidad exacta y reproducibilidad, no para publicar microdatos individuales.

## Instalación

```bash
git clone https://github.com/matuteiglesias/samplerCensoARG.git
cd samplerCensoARG
pip install -r requirements.txt
```

## Interfaz histórica / exploratoria

El repositorio conserva la CLI original para muestreos ad hoc. Es útil para exploración y compatibilidad histórica, pero **no es la interfaz recomendada para handoffs científicos reproducibles**.

```bash
python -m censo_sampler.cli -dbp /path/to/ext_CPV2010_basico_radio_pub [options]
```

Opciones históricas principales:

- `-dbp, --databasepath PATH`: directorio con los CSV del Censo;
- `-f, --frac FLOAT`: fracción de hogares a muestrear;
- `-y, --years START END`: intervalo de años usado por la lógica histórica de proyección;
- `-n, --nombre STR`: etiqueta de salida;
- `-d, --departamentos ID [...]`: restringe departamentos;
- `-p, --provincias ID [...]`: restringe provincias;
- `--out-dir PATH`: directorio de salida.

Ejemplo exploratorio:

```bash
python -m censo_sampler.cli -dbp /path/ext_CPV2010_basico_radio_pub -f 0.05 -p 2 -y 2020 2021
```

La salida histórica usa archivos por tabla y parámetros de ejecución. Para nuevos consumidores, prefiera siempre `research.census-sample/v1` y su manifiesto verificable.

## Fuentes y atribución

El software está diseñado para trabajar con datos producidos por **INDEC — Instituto Nacional de Estadística y Censos de la República Argentina**. Este repositorio no está afiliado a INDEC y no redistribuye los microdatos censales.

Fuentes o herramientas históricamente utilizadas para obtener/exportar CPV-2010 pueden haber cambiado; una URL de terceros no debe interpretarse como fuente oficial vigente ni como autorización de redistribución.
