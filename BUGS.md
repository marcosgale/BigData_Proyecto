# BUGS: defectos que destapa el universo

Con cinco países y un año, el código de la Práctica 1 funcionaba. Al pasar a todos los países y toda la serie histórica salieron estos defectos. Para cada uno: cómo se reproduce, qué efecto tenía y cómo lo arreglamos.

## 1. Los agregados del Banco Mundial se cuelan como países

**Reproducción.** Quitar el filtro de `focus_countries` y cargar todos los registros de una página del Banco Mundial (`data/raw/worldbank_gdp_NY.GDP.PCAP.CD_page_1.json`). Entre las entidades aparecen `WLD` (World), `EUU` (European Union), `AFE` (Africa Eastern and Southern), etc. Para `NY.GDP.PCAP.CD` hay 262 códigos distintos, pero solo 194 son países según nuestro criterio.

**Efecto.** En 2020 hay 258 entidades con PIB per cápita en bruto y solo 190 países. La mediana del bruto es 6131,8 USD y la de los países 5414,6 USD. Los agregados la suben unos 700 USD: `EUU` vale 34 694,7 y `WLD` 10 999,5. Cualquier mediana o recuento de "países" hecho sobre el bruto sale mal. En `spark/job_01.py`, `countDistinct(iso3)` contaría regiones como países.

**Arreglo.** Criterio de país explícito en `sources.yaml` (`country_criteria`): se acepta lo que el perfil mledoze marca como `independent: true`, más Kosovo como excepción. `src/transform.py` (`map_indicators_to_iso3`) solo deja pasar a `data/clean/` los iso3 aceptados. Todo lo demás va a `data/clean/excluded_entities.csv` con su motivo (`agregado o código sin equivalencia en el perfil` o `territorio no independiente en el perfil`).

## 2. El año del Banco Mundial llega como texto

**Reproducción.** Leer cualquier página del Banco Mundial y mirar `type(registro["date"])`: es `str` (`"2020"`), no `int`. El transform de la P1 lo copiaba tal cual a `indicators.csv`.

**Efecto.** Con textos, el orden y las comparaciones son alfabéticos: `sorted(["1999", "2000", "990"])` da `['1999', '2000', '990']`, así que `"990"` queda como el año más reciente. Las décadas (`year // 10`) no se pueden calcular sin convertir antes. En Parquet la columna `year` quedaba como `string`, y Spark tendría que castear en cada consulta.

**Arreglo.** En `type_and_validate` (`src/transform.py`): `pd.to_numeric(year, errors="raise").astype("Int64")`. Si algún año no es un número, el transform falla en vez de esconderlo. `value` se pasa a `float64` y lo que falta queda nulo, nunca 0. Además, si la clave `(iso3, indicator_id, year, source_id)` se repite, se lanza `ValueError`.

## 3. Cada fuente usa su propio código para algunos países

**Reproducción.** Buscar Grecia, Reino Unido y Kosovo en los datos brutos:
- Eurostat usa `EL` para Grecia y `UK` para Reino Unido, pero en el perfil son `GR`/`GRC` y `GB`/`GBR`.
- Kosovo es `UNK` en mledoze, `XKX` en el Banco Mundial, `XK` en Eurostat y `OWID_KOS` en Our World in Data.

**Efecto.** Sin traducir los códigos, esas filas no casan con ningún país aceptado y se pierden en silencio. Grecia y Reino Unido se quedan sin las 66 filas de población de Eurostat cada uno. Kosovo pierde además sus 330 filas de indicadores del Banco Mundial y sus 74 filas de OWID. En `join_orphans.csv` saldrían como huérfanos aunque el dato sí esté en el bruto.

**Arreglo.** Bloque `code_equivalences` en `sources.yaml` (`XKX`, `XK`, `OWID_KOS` → `UNK`; `EL` → `GRC`; `UK` → `GBR`), leído por `src/config.py`. El transform aplica primero la equivalencia, después mira si ya es un iso3 del perfil y, si no, traduce desde iso2. Con eso, `GRC`, `GBR` y `UNK` tienen sus 66 filas de Eurostat.

## 4. El transform de Eurostat de la P1 coge un valor que no es del país

**Reproducción.** Ejecutar la versión de la P1 de `transform_eurostat_demo_pjan` sobre el `eurostat_pop.json` que baja ahora el extract (todos los países y años). Esa versión hacía `next(iter(payload["value"].values()))`, es decir, coger el primer valor del JSON.

**Efecto.** La celda 0 del JSON-stat es `EU27_2020` en 1960: 354 531 754 personas. Ese número se asignaba como población de cada país foco, sin ningún error ni aviso.

**Arreglo.** El transform recorre la matriz JSON-stat completa: posición = `pos_geo * n_años + pos_año`, con los índices de `dimension.geo` y `dimension.time`. Así cada valor va con su país y su año, y las celdas sin dato quedan nulas. Un `assert` comprueba que el resto de dimensiones (`freq`, `unit`, `age`, `sex`) tienen tamaño 1, porque si no esa fórmula dejaría de valer. La versión de la P1 se conserva solo para el formato antiguo de un fichero por país (`url_template`).
