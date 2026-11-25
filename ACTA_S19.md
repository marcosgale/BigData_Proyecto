# ACTA S19: un programa que no termina

Equipo: Alejandro Nieto Aguilar y Marcos García León.

**Dónde lo probamos.** No en DataLab: en un Kafka 4.4.0 local (modo KRaft, `127.0.0.1:9092`, topic `sensores.temp`) dentro de WSL (Ubuntu 26.04, Java 17, PySpark 4.3). El conector es el de nuestra versión de Spark, `spark-sql-kafka-0-10_2.13:4.3.0`, no el `_2.12:3.5.9` de DataLab. Lanzamos `python -u streaming/temp_stream.py` con `--packages` y `spark.sql.shuffle.partitions=4` en `PYSPARK_SUBMIT_ARGS`, y paramos con SIGINT (equivale a Ctrl+C). El código de `temp_stream.py` es el mismo que se usaría en DataLab. La consola muestra las horas en local (UTC+1) y los eventos llevan `ts` en UTC.

## 1. ¿Qué hace `spark/job_01.py` y por qué termina?

Lee el Parquet de `data/clean/indicators` y calcula la cobertura por indicador y década de tres formas (RDD, DataFrame y Spark SQL). Las compara entre sí y con el SQLite de `serving/`, escribe el resultado en Parquet y termina con código 0. Termina porque es **batch**: los datos de entrada son un conjunto cerrado que ya está en disco. Cuando lo ha leído todo, no hay nada más que esperar.

## 2. ¿Qué hace `temp-stream` y por qué no termina?

Lee sin parar los mensajes del topic `sensores.temp`. Cada 5 segundos (un micro-lote) cuenta cuántos mensajes ha mandado cada sensor en cada minuto (`window(ts, "1 minute")`) y saca por consola solo las cuentas que cambian (`update`). No termina porque es **streaming**: la entrada no tiene fin, siempre puede llegar otro mensaje. `awaitAnyTermination()` deja el proceso esperando hasta que lo paramos.

## 3. ¿Qué guarda el checkpoint?

En `~/checkpoints/temp-stream` vimos:
- **Progreso**, en `offsets/` y `commits/`: por dónde va en Kafka. El último fichero de `offsets/` dice `{"sensores.temp":{"0":7}}`, es decir, que ya ha leído los 7 mensajes del topic. También guarda la watermark del lote (`batchWatermarkMs`).
- **Estado**, en `state/0/`, con una carpeta por partición de shuffle (`0 1 2 3`): las cuentas a medias de cada (ventana, sensor), que todavía pueden subir mientras la watermark no cierre la ventana.

`~/checkpoints/temp-nulos` tiene `offsets/` y `commits/` pero **no tiene `state/`**: ese query solo filtra, no agrega, así que no necesita recordar cuentas.

## 4. ¿Qué pasó en la prueba de parar y arrancar?

1. Publicamos 3 mensajes (s01 ×2, s02 ×1) y arrancamos. Batch 0 dio `s01 2` y `s02 1` en la ventana 12:22–12:23.
2. Paramos con Ctrl+C. El proceso salió con código 0.
3. Con el programa parado publicamos 2 mensajes más de s01 (`publicar_control.sh`).
4. Arrancamos con el mismo comando. El query tenía **el mismo `id`** (`4043ee81-...`) y los lotes siguieron numerándose (Batch 1, 2...), no empezaron en 0. El Batch 2 dio **`s01 4`** en la misma ventana 12:22.

Se ven las dos memorias. Por el **progreso**, no volvió a leer los 3 primeros mensajes; si no, saldría s02 otra vez y s01 = 6. Por el **estado**, recordaba que s01 llevaba 2 y le sumó los 2 nuevos; sin estado habría dicho 2.

Una cosa fea: al pulsar Ctrl+C sale una traza de `py4j` (`reentrant call` / `KeyboardInterrupt`). Es el propio manejador de SIGINT de PySpark, que intenta cancelar trabajos mientras hay una llamada en curso. El proceso termina igual con código 0 y el checkpoint queda bien, porque el segundo arranque continuó donde lo dejó.

## 5. Reto: los dos eventos trampa

Nuestra apuesta antes de lanzarlo: el que llega tarde no se cuenta, por la watermark. El roto (`temp` en vez de `valor`) sí se cuenta en `temp-stream` y aparece en `temp-nulos`. Eso fue lo que pasó con `publicar_trampas.sh`:

- **Trampa 1, llega tarde** (`s09`, `ts` 20 minutos antes): `temp-stream` **no la contó**, no salió ninguna ventana de las 12:03. La watermark estaba en "evento más reciente − 10 minutos", así que un evento de hace 20 minutos llega tarde y se descarta. `temp-nulos` tampoco la mostró, porque su `valor` (30.0) estaba bien.
- **Trampa 2, mensaje roto** (`s09` con `temp`): `temp-stream` **sí la contó** (`s09 1` en la ventana 12:23), porque `count()` cuenta mensajes y no mira si traen temperatura. `temp-nulos` **la mostró**: `s09 | NULL | 12:23:13`. `from_json` no falla con un campo que no conoce: deja `valor` a nulo.

Conclusión: un mensaje roto no hace ruido. Sin `temp-nulos`, la cuenta parece correcta y nadie se entera.

**¿Por qué `temp-nulos` necesita su propio checkpoint?** Cada query guarda en su checkpoint *su* progreso y *su* estado. Si los dos compartieran carpeta, uno pisaría los offsets del otro y, al rearrancar, uno se saltaría mensajes o los repetiría. Además, `temp-stream` tiene estado de agregación y `temp-nulos` no, así que sus contenidos ni siquiera tienen la misma forma.

## Salida del test

```
$ pytest -q tests/s19 --repo=../BigData_Proyecto
...........                                                              [100%]
11 passed in 0.04s
```
