# ENTREGA 02: del ejemplo al universo

Equipo: Alejandro Nieto Aguilar y Marcos García León (ver `CONTRIBUTORS.md`).

## 1. Predicción

Cifras razonadas antes de mirar el resultado del transform. Hay dos números distintos, como pide el enunciado.

**A. Bruto (lo que comprueba el extract).** El metadato `total` de la API del Banco Mundial dice **17 556** registros por indicador. Son 266 entidades (países + agregados) × 66 años (1960–2025). Con 6 indicadores: 6 × 17 556 = **105 336** filas brutas del Banco Mundial. A eso se suman Eurostat (59 entidades × 66 años = 3 894 celdas) y el CSV de OWID (≈ 21 500 filas).

**B. Limpio (lo que decide el transform).** No es "6 × 17 556". Tras el criterio de país quedan unos 194 países del Banco Mundial que están en nuestro universo. Esperábamos **194 × 66 = 12 804** filas por indicador (nulos incluidos como nulos explícitos), es decir **76 824** para los 6. Para Eurostat, unos 49 países europeos × 66 años ≈ 3 234. Para OWID, ≈ 16 000 filas (195 países con series de longitud distinta).

## 2. Resultado

Salida de `python -m src.pipeline --skip-extract`:

```
[transform] countries: 195 aceptados, 55 excluidos del perfil
[transform] languages: 330 filas
[transform] indicators: 130795 filas brutas → 96476 limpias
[transform] excluded_entities: 279 filas
[transform] join_orphans: 169 filas
```

| Fuente / indicador | Predicción | Resultado | Nulos |
|---|---|---|---|
| Banco Mundial, cada uno de los 6 | 12 804 | 12 804 | de 0 (`SP.POP.TOTL`, `SP.URB.TOTL.IN.ZS`) a 6 938 (`IT.NET.USER.ZS`) |
| Eurostat `demo_pjan` | ≈ 3 234 | 3 234 | 657 |
| OWID `life_expectancy` | ≈ 16 000 | 16 418 | 0 |
| **Total limpio** | ≈ 96 000 | **96 476** | |

**Diferencias.**
- El Banco Mundial cuadra exacto: 194 países × 66 años.
- Del universo de 195 países falta uno en el Banco Mundial, la Ciudad del Vaticano (`VAT`). Por eso sale en `join_orphans.csv` para los 6 indicadores.
- OWID sale algo por encima de lo estimado: solo guarda años con dato, pero algunos países tienen series desde el siglo XVIII (Reino Unido tiene 242 años).
- El bruto total, 130 795, coincide con A: 105 336 + 3 894 + 21 565.

## 3. Decisiones

- **Criterio de país.** Un país es una entrada del perfil mledoze con `independent: true`, más Kosovo (`UNK`), que mledoze deja en `null` pero el Banco Mundial trata como país. Resultado: 195 aceptados. Los 55 territorios del perfil y todos los agregados de las APIs (`WLD`, `EUU`, `AFE`, `EU27_2020`, continentes de OWID...) van a `excluded_entities.csv` con su motivo. Elegimos `independent` porque es un dato de la fuente, no una lista hecha a mano, y porque dejar fuera territorios pequeños con muchos huecos evita sesgar las medianas regionales. Está en `sources.yaml` (`country_criteria`), no en un `if`.
- **Equivalencias de códigos** en `sources.yaml` (`code_equivalences`): `XKX`, `XK`, `OWID_KOS` → `UNK`; `EL` → `GRC`; `UK` → `GBR`.
- **Indicadores.** Fijos: `NY.GDP.PCAP.CD` (PIB per cápita) y `SP.POP.TOTL` (población). Los cuatro que elegimos:
  - `SP.DYN.LE00.IN` (esperanza de vida): se puede contrastar con la de OWID.
  - `SP.URB.TOTL.IN.ZS` (% población urbana): no tiene nulos, sirve de control de cobertura.
  - `IT.NET.USER.ZS` (% usuarios de Internet): la serie empieza en los 90, así que pone a prueba los huecos por década.
  - `SL.UEM.TOTL.ZS` (% paro): tiene muchos nulos y huérfanos (17 países sin ningún dato).
- **Particionado.** `data/clean/indicators/` particionado estilo Hive por `indicator_id`: 8 carpetas, una por indicador, con un fichero Parquet en cada una. Las consultas filtran casi siempre por indicador, así que Spark solo lee esa carpeta. No particionamos también por año porque saldrían cientos de ficheros diminutos.

## 4. Evidencia

- **Event log del job:** `evidencias/eventlog_v2_local-1795374792362/` (copiado entero de `logs/spark-events/`).
- **Salida de `python spark/job_01.py`** (en WSL, Ubuntu 26.04, Java 17, PySpark 4.3):

```
[job_01] particiones del DataFrame leído: 8
[job_01] RDD: 92 grupos (indicador, década)
[job_01] DataFrame: 92 grupos (indicador, década)
[job_01] Spark SQL: 92 grupos (indicador, década)
[job_01] SQLite: 92 grupos (indicador, década)
[job_01] las cuatro versiones coinciden; cobertura → data/serving/spark/cobertura_decada
```

- **Plan físico de la versión DataFrame** (resumido):

```
Sort [indicator_id, decade]
+- Exchange rangepartitioning(indicator_id, decade, 200)
   +- HashAggregate(keys=[indicator_id, decade], functions=[count(1), min(year), max(year), count(distinct iso3)])
      +- Exchange hashpartitioning(indicator_id, decade, 200)
         +- HashAggregate(keys=[indicator_id, decade], ... partial_count(distinct iso3))
            +- HashAggregate(keys=[indicator_id, decade, iso3], ...)
               +- Exchange hashpartitioning(indicator_id, decade, iso3, 200)
                  +- HashAggregate(keys=[indicator_id, decade, iso3], functions=[partial_count(1), ...])
                     +- Project [..., (cast(year / 10.0 as int) * 10) AS decade]
                        +- Filter isnotnull(value)
                           +- FileScan parquet [...] PushedFilters: [IsNotNull(value)]
```

## 5. Preguntas de teoría (con nuestros datos)

**1. ¿Qué hay en el event log y qué registra Spark antes de nuestra primera acción?**
El log tiene 256 eventos. Antes de cualquier trabajo aparecen los de arranque: `LogStart`, `ResourceProfileAdded`, `ExecutorAdded` (en `local[*]` hay un único executor dentro del mismo proceso que el driver), `BlockManagerAdded`, `EnvironmentUpdate` y `ApplicationStart`.

Lo curioso es que el primer `JobStart` (job 0, una sola stage) llega **antes de nuestra primera acción**. Lo lanza `spark.read.parquet(...)`, porque para conocer el esquema Spark tiene que leer los pies de los ficheros Parquet. En total hay **18 jobs, 19 stages y 61 tareas**. Hay más jobs que acciones (`collect` ×3, `write` y `show`) porque con AQE cada `Exchange` se materializa como un job aparte. Por eso los jobs de la agregación tienen 2, 3 y 4 stages.

**2. ¿Cuántas particiones hay y dónde está el shuffle?**
- **Al leer:** `getNumPartitions()` da **8** con `local[*]`, una por fichero (8 ficheros, uno por `indicator_id`). Los ficheros pesan 717 KB en total, muy por debajo de `spark.sql.files.maxPartitionBytes` (128 MB), así que no se parten.
- **`local[1]` frente a `local[*]`:** lo comprobamos y con `local[1]` sale **1** partición. Spark agrupa los ficheros pequeños según el paralelismo disponible (`defaultParallelism` = 1 frente a 12), así que en nuestro caso, con ficheros tan pequeños, sí cambia.
- **El shuffle:** en el plan físico son los operadores **`Exchange hashpartitioning(...)`**. El `groupBy` es una dependencia ancha: todas las filas de un mismo (indicador, década) tienen que acabar en la misma partición. Hay dos porque `countDistinct` agrega primero por (indicador, década, iso3) y luego por (indicador, década). El `Exchange rangepartitioning` final es el del `orderBy`. Tras el shuffle se usan `spark.sql.shuffle.partitions` = 200 particiones, que AQE junta porque solo hay 92 grupos.
- **En la versión RDD:** el shuffle es el `reduceByKey`. Antes de mover datos combina en cada partición, igual que el `partial_` del plan DataFrame.

**3. CSV frente a Parquet: ¿cuánto ocupa lo mismo?**
La misma tabla `indicators` (96 476 filas):
- `data/clean/indicators.csv`: **8 612 192 bytes** (8,2 MB).
- `data/clean/indicators/` en Parquet: **717 154 bytes** (0,7 MB) en 8 ficheros.

El Parquet ocupa **12 veces menos**. Al ser columnar, los valores repetidos de una columna van juntos y se comprimen muy bien: `iso3`, `unit`, `indicator_label` y `source_id` se repiten miles de veces. Además, `indicator_id` ni siquiera se guarda dentro de los ficheros, porque va en el nombre de la carpeta (Hive). Y Parquet guarda los tipos (`year` como entero, `value` como double con nulos reales), cosa que en el CSV es todo texto.

## 6. Pendientes

Cerrado antes de la etiqueta: el extract (`--force`, 404 sin reintento, solo HTTP 200 en `raw/`, fallos de red en el manifiesto) y las fichas (mediana de la región en el mismo indicador y año, y aviso cuando el país no tiene dato ese año). Pasan los tests del Hito 1 (65) y del Hito 2 salvo la etiqueta.

- **Fichas:** el año de la comparación es el último con algún dato en la región. En indicadores recientes (p. ej. Internet en 2025) ese año puede tener pocos países, así que la mediana es frágil. Sería más robusto el último año con, al menos, la mitad de la región informada.
- **La predicción no se commiteó antes de la primera ejecución completa**, como pide el enunciado. La razonamos con los metadatos de la API, pero el pipeline completo ya se había ejecutado al escribir este documento. Lo dejamos dicho en vez de reordenar commits.
- `load.py` sigue cargando el SQLite desde los CSV de `clean/`. Por eso el transform escribe CSV además de Parquet. Falta pasar `load.py` a leer Parquet y quitar esos CSV.
