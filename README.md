# Muestreador de Censos de Argentina

Este repositorio produce **muestras reproducibles de hogares censales** a partir de un donor frame validado. La arquitectura moderna es neutral respecto del año censal: el mismo núcleo de muestreo está diseñado para consumir Censo 2010 o Censo 2022 mediante el contrato `research.census-frame/v1`.

Estado actual:

- **CPV-2010:** frame CSV→Parquet implementado y probado contra el fixture histórico;
- **migración 2010:** la nueva ruta reproduce exactamente las decisiones científicas del sampler streaming anterior para 2024 y 2025;
- **CPV-2022:** compatibilidad completa probada con un frame sintético de forma 2022; falta únicamente el gate con el extracto RXDB real que se materializa fuera de este repo;
- las interfaces históricas/v1 siguen disponibles como compatibilidad y oracle de regresión.

La unidad de selección sigue siendo el **hogar** y se conservan **todas las personas** de cada hogar seleccionado.

## Arquitectura actual

```text
CPV-2010 CSV                         CPV-2022 RXDB
     │                                    │
2010 frame builder                  rxdb-extractor
     │                                    │
     │                         argentina-censo2022-rxdb
     │                                    │
     └──────────────┬─────────────────────┘
                    ▼
          research.census-frame/v1
                    │
                    ▼
              samplerCensoARG
            un solo sampling kernel
                    │
                    ▼
      research.census-target-year-sample/v2
```

El sampler **no abre RXDB, no depende de RedEngine y no decide qué variables necesita EPH**. El donor frame conserva el payload censal completo; la selección de features y su alineación semántica pertenecen a consumidores downstream como `eph-censo-aligner`.

Véanse:

- [`docs/CENSUS_FRAME_CONTRACT_V1.md`](docs/CENSUS_FRAME_CONTRACT_V1.md)
- [`docs/CENSUS_SAMPLE_V2_CONTRACT.md`](docs/CENSUS_SAMPLE_V2_CONTRACT.md)
- [`docs/CPV2022_FRAME_HANDOFF.md`](docs/CPV2022_FRAME_HANDOFF.md)
- [`docs/CENSUS_VINTAGE_NEUTRAL_RETROFIT_PLAN.md`](docs/CENSUS_VINTAGE_NEUTRAL_RETROFIT_PLAN.md)
- [`docs/HUMAN_OPERATOR_QUEUE.md`](docs/HUMAN_OPERATOR_QUEUE.md)

## 1. Preparar un frame CPV-2010

Los CSV históricos siguen siendo un source válido, pero se convierten una vez a Parquet antes de muestrear repetidamente.

Input local:

```text
VIVIENDA.csv
HOGAR.csv
PERSONA.csv
GEOGRAPHY.csv
```

El frame builder necesita únicamente identidades y relaciones censales para el diseño muestral. **No exige `P02`, `P03`, edad, educación, empleo ni otras columnas de un experimento EPH.** Todas las columnas fuente que existan se conservan en el payload Parquet.

```bash
censo-sampler frame build-2010 \
  --databasepath /secure/cpv2010 \
  --geography /secure/cpv2010/GEOGRAPHY.csv \
  --output-root /secure/census-frames
```

Verificación profunda:

```bash
censo-sampler frame check /secure/census-frames/<frame-release>
```

Un frame contiene:

```text
manifest.json
frame_households.parquet
donor_person_mass.parquet
payload/
  vivienda.parquet
  hogar.parquet
  persona.parquet
```

## 2. CPV-2022

El source 2022 se prepara upstream:

```text
RXDB/RBFX
  → rxdb-extractor
  → argentina-censo2022-rxdb
  → research.census-frame/v1
```

El sampler recibe exactamente el mismo contrato que para 2010. La implementación real debe mapear aproximadamente:

```text
vivienda_key  → frame_dwelling_id
hogar_key     → frame_household_id
persona_key   → frame_person_id
dpto_cmpcode  → department_id
radio_cmpcode → radio_id
```

La primera prueba real prevista es RADIO `061471101` (73 viviendas, 56 hogares, 137 personas). El sampler ya pasa la misma ruta completa con un fixture sintético 2022; no se declara todavía el gate real cerrado hasta ejecutar ese handoff local.

## 3. Muestreo target-year compartido

Para donor person mass `D[d]`, target person mass `T[d,y]` e intensidad global `c`:

```text
p[d,y] = c * T[d,y] / D[d]
```

Los hogares son seleccionados con un score SHA-256 determinístico. El año objetivo no entra en el score, de modo que 2024 y 2025 usan el mismo orden pseudoaleatorio dentro de un mismo donor frame; cambian únicamente las probabilidades departamentales.

```bash
censo-sampler sample \
  --frame /secure/census-frames/<frame-release> \
  --target-population /secure/targets/<target-release> \
  --target-year 2024 \
  --fraction 0.01 \
  --seed 20260831 \
  --materialize full-payload \
  --output-root /secure/census-samples
```

La geografía departamental usa por ahora la política explícita:

```text
assume-code-identity/v1
```

Es decir, se asume identidad del código oficial de departamento/partido/comuna entre 2010, 2022 y el parent de población objetivo. El sampler compara los universos y **falla de forma visible** si encuentra excepciones; esas pocas excepciones podrán resolverse luego con un crosswalk gobernado específico.

## 4. Key-first: el sampler no elige features

La decisión científica se materializa primero en:

```text
selection.parquet
person_membership.parquet
```

`selection.parquet` fija qué hogares fueron elegidos y con qué probabilidad. `person_membership.parquet` fija exactamente qué personas pertenecen a esos hogares.

Sólo después se materializa, si se solicita, el payload completo:

```text
vivienda.parquet
hogar.parquet
persona.parquet
```

Esto permite que el mismo sample sea independiente de qué columnas necesite un modelo posterior.

Modos:

```text
--materialize selection-only
--materialize full-payload
```

## 5. Release v2 y pesos

El contrato nuevo es:

```text
research.census-target-year-sample/v2
```

Usa identidades y geografía neutrales al vintage:

```text
frame_release_id
frame_vintage
department_id
radio_id
sample_household_id
sample_person_id
selection_probability
design_inverse_probability_weight
```

Las semánticas siguen separadas:

```text
selection_probability
 design_inverse_probability_weight = 1 / selection_probability
 analysis_weight = unset
```

No se inventa un `sample_weight`/`analysis_weight` genérico que pueda deshacer accidentalmente la composición geográfica creada por el diseño muestral.

Validación offline:

```bash
censo-sampler check-release-v2 /secure/census-samples/<release>
```

## 6. Reproducibilidad y QA

El sistema valida/falla ante, entre otros casos:

- IDs vacíos o duplicados;
- PERSONA huérfana o HOGAR sin VIVIENDA;
- household person counts inconsistentes;
- donor mass inconsistente con el frame;
- corrupción/hash mismatch de artifacts;
- universos departamentales incompatibles;
- probabilidades fuera de `(0,1]`;
- selección/person membership incompletos;
- creación implícita de un analysis weight no autorizado.

La migración tiene además un gate fuerte: para el fixture CPV-2010, el sampler nuevo debe seleccionar exactamente los mismos hogares/personas y las mismas probabilidades que `streaming_target_year.py` en 2024 y 2025.

## 7. Verificación del repositorio

```bash
make test
make frame-fixture
make frame-check
make sample-v2-fixture
make sample-v2-check
make check
```

CI ejecuta la frontera legacy + frame/v2 sobre Python 3.10 y 3.12.

## Privacidad y custodia

Este repositorio contiene **software de muestreo**, no autoridad para redistribuir microdatos censales.

- No suba Census source data ni releases reales a Git.
- Mantenga frames y samples reales en almacenamiento local/controlado.
- Un sample target-year sigue formado por hogares/personas observados en el Census donor frame correspondiente; el target year modifica la composición departamental, no convierte esos registros en observaciones contemporáneas.
- La publicación de payloads persona-level es una decisión de privacidad/legal separada del funcionamiento del sampler.

## Instalación

```bash
git clone https://github.com/matuteiglesias/samplerCensoARG.git
cd samplerCensoARG
pip install -e .
```

## Compatibilidad v1 / interfaz histórica

Las rutas previas se mantienen para reproducibilidad y regresión.

Release gobernada v1:

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

Sampler exploratorio histórico:

```bash
python -m censo_sampler.cli \
  -dbp /path/to/ext_CPV2010_basico_radio_pub \
  -f 0.05 -p 2 -y 2020 2021
```

Estas interfaces ya no definen la arquitectura objetivo. Para nuevos trabajos use `research.census-frame/v1` + sample v2.

## Fuentes y atribución

El software está diseñado para trabajar con datos producidos por **INDEC — Instituto Nacional de Estadística y Censos de la República Argentina**. Este repositorio no está afiliado a INDEC y no redistribuye los microdatos censales.
