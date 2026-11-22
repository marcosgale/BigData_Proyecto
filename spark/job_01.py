"""Job Spark 01: cobertura por (indicator_id, década).

Lee data/clean/indicators (Parquet particionado por indicator_id) y calcula,
para cada indicador y década, cuántas filas con valor hay, cuántos países
distintos y el primer y último año con dato. Lo hace de tres maneras:

  1. RDD al estilo MapReduce (map → reduceByKey)
  2. DataFrame (groupBy + agg)
  3. Spark SQL (vista temporal + GROUP BY)

y las compara entre sí y con la misma consulta sobre el SQLite de serving/.
Si algo no coincide, sale con código 1.

Escribe la versión DataFrame en data/serving/spark/cobertura_decada/ (Parquet)
y deja el event log en logs/spark-events/.

Uso (desde la raíz del repo, después de `python -m src.pipeline`):

    python spark/job_01.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
INDICATORS = ROOT / "data" / "clean" / "indicators"
SQLITE = ROOT / "data" / "serving" / "observatorio.sqlite"
OUT = ROOT / "data" / "serving" / "spark" / "cobertura_decada"
EVENT_LOG = ROOT / "logs" / "spark-events"

COLUMNS = ["indicator_id", "decade", "rows", "countries", "first_year", "last_year"]


def as_set(rows) -> set[tuple]:
    """Normaliza los resultados (Row de Spark o tuplas de sqlite) para compararlos."""
    return {(str(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5])) for r in rows}


def coverage_rdd(df) -> set[tuple]:
    """MapReduce: emitir ((indicador, década), parcial) y reducir por clave."""
    pairs = df.rdd.map(
        lambda r: (
            (r["indicator_id"], r["year"] // 10 * 10),
            (1, {r["iso3"]}, r["year"], r["year"]),
        )
    )
    reduced = pairs.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] | b[1], min(a[2], b[2]), max(a[3], b[3]))
    )
    rows = reduced.map(lambda kv: (kv[0][0], kv[0][1], kv[1][0], len(kv[1][1]), kv[1][2], kv[1][3]))
    return as_set(rows.collect())


def coverage_dataframe(df):
    return (
        df.withColumn("decade", (F.col("year") / 10).cast("int") * 10)
        .groupBy("indicator_id", "decade")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("iso3").alias("countries"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .orderBy("indicator_id", "decade")
    )


def coverage_sql(spark) -> set[tuple]:
    # La vista "indicators" ya está filtrada a value IS NOT NULL
    result = spark.sql(
        """
        SELECT indicator_id,
               CAST(year / 10 AS INT) * 10 AS decade,
               COUNT(*)                    AS rows,
               COUNT(DISTINCT iso3)        AS countries,
               MIN(year)                   AS first_year,
               MAX(year)                   AS last_year
        FROM indicators
        GROUP BY indicator_id, CAST(year / 10 AS INT) * 10
        """
    )
    return as_set(result.collect())


def coverage_sqlite() -> set[tuple]:
    """La misma agregación sobre el SQLite de serving/ (lo que leen las fichas)."""
    with sqlite3.connect(SQLITE) as conn:
        rows = conn.execute(
            """
            SELECT indicator_id, (year / 10) * 10, COUNT(*), COUNT(DISTINCT iso3),
                   MIN(year), MAX(year)
            FROM indicators
            WHERE value IS NOT NULL
            GROUP BY indicator_id, (year / 10) * 10
            """
        ).fetchall()
    return as_set(rows)


def main() -> int:
    if not INDICATORS.is_dir() or not SQLITE.is_file():
        print("Faltan data/clean/indicators o el SQLite. Ejecutad antes: python -m src.pipeline")
        return 1

    EVENT_LOG.mkdir(parents=True, exist_ok=True)
    # Que los workers usen el mismo Python que el driver
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("job_01_cobertura_decada")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG.as_uri())
        .getOrCreate()
    )
    try:
        raw = spark.read.parquet(str(INDICATORS))
        print(f"[job_01] particiones del DataFrame leído: {raw.rdd.getNumPartitions()}")

        # Un nulo no es una observación: solo cuentan filas con valor
        df = raw.where(F.col("value").isNotNull()).select("iso3", "indicator_id", "year")
        df.createOrReplaceTempView("indicators")

        by_df = coverage_dataframe(df)
        print("[job_01] plan físico de la versión DataFrame:")
        by_df.explain()

        results = {
            "RDD": coverage_rdd(df),
            "DataFrame": as_set(by_df.collect()),
            "Spark SQL": coverage_sql(spark),
            "SQLite": coverage_sqlite(),
        }
        for name, res in results.items():
            print(f"[job_01] {name}: {len(res)} grupos (indicador, década)")

        reference = results["DataFrame"]
        mismatches = [name for name, res in results.items() if res != reference]
        if mismatches:
            for name in mismatches:
                print(f"[job_01] {name} no coincide con DataFrame:")
                print(f"  solo en {name}: {sorted(results[name] - reference)[:5]}")
                print(f"  solo en DataFrame: {sorted(reference - results[name])[:5]}")
            return 1

        by_df.write.mode("overwrite").parquet(str(OUT))
        print(f"[job_01] las cuatro versiones coinciden; cobertura → {OUT}")
        by_df.show(10, truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
