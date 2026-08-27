# Muestreador del Censo 2010 — Argentina

Este repositorio produce **muestras reproducibles de hogares del Censo Nacional de Población, Hogares y Viviendas 2010** a partir de una copia local autorizada de los microdatos.

La interfaz recomendada es la release inmutable `research.census-sample/v1`: selecciona hogares de forma determinística, conserva a todas las personas de cada hogar seleccionado, asigna identificadores estables y publica QA, manifiesto y checksums verificables.

El repositorio también posee la semántica de un modo opcional de **muestreo objetivo por año**: la distribución de hogares seleccionados entre departamentos puede modificarse usando una fuente exacta de población por departamento del año objetivo. Ese mecanismo es parte del diseño muestral; no es una capa posterior de calibración poblacional. Véase [`docs/TARGET_YEAR_HOUSEHOLD_SAMPLING.md`](docs/TARGET_YEAR_HOUSEHOLD_SAMPLING.md).

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

`region_id` pertenece todavía al contrato implementado actual y debe usar una de las seis regiones soportadas por la integración histórica de canastas. La arquitectura objetivo lo separa de la identidad censal intrínseca y lo obtiene mediante un binding territorial gobernado.

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

### Muestreo objetivo por año

El método histórico usaba una tabla de población por departamento y modificaba la probabilidad de seleccionar hogares según el tamaño relativo del departamento en el año objetivo respecto de 2010. El objetivo es obtener una muestra sintética suficientemente grande cuya **composición geográfica departamental** se aproxime a la del año indicado, manteniendo los hogares de Censo-2010 como donor frame.

Ese mecanismo no actualiza por separado edad, educación, empleo, tamaño del hogar u otras dimensiones dentro del departamento. La validez de esas dimensiones descansa en la representatividad estadística del muestreo de hogares suficientemente grande y debe quedar declarada como supuesto del producto.

La implementación gobernada actual **todavía no debe considerarse una implementación aprobada de este método objetivo por año**. La opción histórica `legacy_department_projection_candidate` y el único campo `sample_weight` conservan semánticas heredadas que necesitan reparación antes de un real scientific run. En particular, una ponderación inversa `1 / p` puede deshacer deliberadamente la composición geográfica creada por probabilidades departamentales diferentes. El contrato moderno deberá separar `selection_probability`, peso inverso de diseño y cualquier `analysis_weight` autorizado.

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
- regiones inválidas en el contrato implementado actual;
- pesos no positivos;
- rutas de salida inseguras;
- una release o checksum que no coincide con su manifiesto.

## Privacidad y límites de uso

Este proyecto contiene **código de muestreo**, no autoridad sobre los microdatos censales ni una estimación oficial de población actual.

- No suba microdatos CPV ni muestras con información sensible a Git.
- Mantenga source data, releases y handoffs en almacenamiento local/controlado.
- Una muestra objetivo derivada desde CPV-2010 sigue formada por hogares/personas observados en 2010; el año objetivo informa su composición departamental, no convierte los registros en observaciones contemporáneas.
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
- `-f, --frac FLOAT`: fracción base de hogares a muestrear;
- `-y, --years START END`: intervalo de años usado por la lógica histórica de muestreo con tamaños relativos departamentales;
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
