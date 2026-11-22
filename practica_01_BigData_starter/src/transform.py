"""Transform: data/raw/ → data/clean/ (tablas normalizadas, universo completo).

Práctica 2 Big Data. Pasos (en este orden):
  1. Leer cada fuente desde raw/ (sin volver a la red)
  2. Criterio de país: aceptar / descartar entidades (config en sources.yaml)
  3. Dar forma: perfil aplanado, idiomas en tabla larga, indicadores en esquema común
  4. Tipar y validar: year entero, value real o nulo, clave sin duplicados
  5. Huérfanos por fuente e indicador → join_orphans.csv

Salida en data/clean/: countries.parquet, languages.parquet,
indicators/indicator_id=<id>/*.parquet, excluded_entities.csv (source_id, source_code, name, reason), join_orphans.csv.
También se dejan countries/languages/indicators .csv porque load.py aún los lee.

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Callable

import pandas as pd

from .config import Config, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]

INDICATOR_COLUMNS = [
    "iso3", "indicator_id", "indicator_label", "year", "value", "unit", "source_id",
]
KEY = ["iso3", "indicator_id", "year", "source_id"]


def _read_json(path: Path):
    if not path.exists():
        # Si falta un fichero es culpa del extract: no inventamos filas.
        raise FileNotFoundError(f"Falta {path}. Ejecutad antes el extract.")
    with path.open(encoding="utf-8-sig") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# 1 + 3. Leer y dar forma (cada fuente a su tabla)
# Los indicadores salen con `code` (el código tal cual lo usa la fuente) y
# `entity_name`; el criterio de país se aplica después, en run_transform.
# ---------------------------------------------------------------------------


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    payload = _read_json(RAW / f"{source.id}.json")

    countries_rows = []
    language_rows = []
    for item in payload:
        iso3 = item.get("cca3")
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": item.get("cca2"),
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
                "independent": item.get("independent"),
            }
        )
        for code, lang_name in (item.get("languages") or {}).items():
            language_rows.append(
                {"iso3": iso3, "language_code": code, "language_name": lang_name}
            )

    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(language_rows),
    }


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    meta = source.meta or {}
    # Indicador principal + adicionales, cada uno con su etiqueta y unidad
    indicators = [meta] + list(meta.get("additional_indicators", []))

    rows = []
    for ind in indicators:
        ind_id = ind["indicator_id"]
        # Todas las páginas que bajó el extract para este indicador
        pages = sorted(RAW.glob(f"{source.id}_{ind_id}_page_*.json"))
        if not pages:
            raise FileNotFoundError(
                f"No hay páginas de {ind_id} en raw/. Ejecutad antes el extract."
            )
        for path in pages:
            payload = _read_json(path)
            # World Bank: [metadatos, [registros...]]
            for rec in payload[1] or []:
                rows.append(
                    {
                        "code": rec.get("countryiso3code"),
                        "entity_name": (rec.get("country") or {}).get("value"),
                        "indicator_id": ind_id,
                        "indicator_label": ind.get("indicator_label"),
                        "year": rec.get("date"),
                        "value": rec.get("value"),
                        "unit": ind.get("unit"),
                        "source_id": source.id,
                    }
                )
    return {"indicators": pd.DataFrame(rows)}


def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(RAW / f"{source.id}.csv")
    meta = source.meta or {}
    # Columnas OWID: Entity, Code, Year, Life expectancy. Ahora toda la serie.
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    out = pd.DataFrame(
        {
            # Los agregados de OWID (continentes, "World"...) vienen sin Code
            "code": df["Code"],
            "entity_name": df["Entity"],
            "indicator_id": meta.get("indicator_id", "life_expectancy"),
            "indicator_label": meta.get("indicator_label", value_col),
            "year": df["Year"],
            "value": df[value_col],
            "unit": meta.get("unit", "years"),
            "source_id": source.id,
        }
    )
    return {"indicators": out}


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    payload = _read_json(RAW / f"{source.id}.json")
    meta = source.meta or {}

    # JSON-stat: "value" es {posición: valor} sobre una matriz aplanada con
    # dimensiones payload["id"] y tamaños payload["size"]. Aquí solo varían
    # geo y time (el resto tienen tamaño 1), así que posición = geo*n_time + time.
    dims = dict(zip(payload["id"], payload["size"]))
    assert all(n == 1 for d, n in dims.items() if d not in ("geo", "time")), dims

    geo_index = payload["dimension"]["geo"]["category"]["index"]
    geo_label = payload["dimension"]["geo"]["category"]["label"]
    time_index = payload["dimension"]["time"]["category"]["index"]
    geo_by_pos = {pos: code for code, pos in geo_index.items()}
    time_by_pos = {pos: year for year, pos in time_index.items()}
    n_time = dims["time"]

    rows = []
    # Recorremos todas las celdas geo×time: las que no traen valor quedan nulas
    for g_pos, geo in geo_by_pos.items():
        for t_pos, year in time_by_pos.items():
            value = payload["value"].get(str(g_pos * n_time + t_pos))
            rows.append(
                {
                    "code": geo,
                    "entity_name": geo_label.get(geo),
                    "indicator_id": meta.get("indicator_id", "demo_pjan"),
                    "indicator_label": meta.get("indicator_label", payload.get("label")),
                    "year": year,
                    "value": value,
                    "unit": meta.get("unit", "persons"),
                    "source_id": source.id,
                }
            )
    return {"indicators": pd.DataFrame(rows)}


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    """Plantilla: lee raw y deja un DataFrame mínimo para que lo adapteis."""
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = []
    for path in paths:
        rows.append(
            {
                "source_id": source.id,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "note": "Reemplazad este transformer por uno específico",
            }
        )
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}


# ---------------------------------------------------------------------------
# 2. Criterio de país
# ---------------------------------------------------------------------------


def apply_country_criteria(
    countries: pd.DataFrame, cfg: Config
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separa el perfil en aceptados y excluidos según sources.yaml."""
    rules = cfg.country_criteria
    extra = set(rules.get("extra_countries", []))

    accepted_mask = countries["iso3"].isin(extra)
    if rules.get("require_independent", True):
        accepted_mask |= countries["independent"] == True  # noqa: E712 (puede ser None)
    else:
        accepted_mask[:] = True

    accepted = countries[accepted_mask].drop(columns="independent")
    excluded = pd.DataFrame(
        {
            "source_id": "countries_profile",
            "source_code": countries.loc[~accepted_mask, "iso3"],
            "name": countries.loc[~accepted_mask, "name_common"],
            "reason": "no independiente en el perfil (territorio o dependencia)",
        }
    )
    return accepted.reset_index(drop=True), excluded


def map_indicators_to_iso3(
    ind: pd.DataFrame, countries: pd.DataFrame, accepted_iso3: set[str], cfg: Config
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Traduce el código de cada fuente a iso3 y se queda solo con países aceptados."""
    equiv = cfg.code_equivalences
    profile_iso3 = set(countries["iso3"])
    iso3_by_iso2 = dict(zip(countries["iso2"], countries["iso3"]))

    def to_iso3(code):
        if not isinstance(code, str) or not code:
            return None
        if code in equiv:
            return equiv[code]
        if code in profile_iso3:
            return code
        return iso3_by_iso2.get(code)  # Eurostat usa iso2

    ind = ind.copy()
    ind["iso3"] = ind["code"].map(to_iso3)

    keep = ind["iso3"].isin(accepted_iso3)
    dropped = ind[~keep].drop_duplicates(["source_id", "code", "entity_name"])
    motivo = dropped["iso3"].map(
        lambda iso3: "territorio no independiente en el perfil"
        if iso3 in profile_iso3
        else "agregado o código sin equivalencia en el perfil"
    )
    excluded = pd.DataFrame(
        {
            "source_id": dropped["source_id"],
            "source_code": dropped["code"],
            "name": dropped["entity_name"],
            "reason": motivo,
        }
    )
    return ind[keep], excluded


# ---------------------------------------------------------------------------
# 4. Tipar y validar
# ---------------------------------------------------------------------------


def type_and_validate(ind: pd.DataFrame) -> pd.DataFrame:
    ind = ind[INDICATOR_COLUMNS].copy()
    ind["iso3"] = ind["iso3"].astype("string")
    # El año del Banco Mundial llega como texto ("2023")
    ind["year"] = pd.to_numeric(ind["year"], errors="raise").astype("Int64")
    # Ausente = nulo (NaN), nunca 0
    ind["value"] = pd.to_numeric(ind["value"], errors="coerce").astype("float64")

    dup = ind.duplicated(KEY, keep=False)
    if dup.any():
        raise ValueError(
            f"Clave duplicada {KEY} en {dup.sum()} filas, p. ej.:\n"
            f"{ind[dup].head().to_string()}"
        )
    return ind.sort_values(KEY).reset_index(drop=True)


# ---------------------------------------------------------------------------
# 5. Huérfanos por fuente
# ---------------------------------------------------------------------------


def find_orphans(accepted_iso3: list[str], ind: pd.DataFrame, raw_ind: pd.DataFrame) -> pd.DataFrame:
    """País aceptado × (fuente, indicador) sin ninguna fila con valor."""
    with_data = ind[ind["value"].notna()]
    have = set(zip(with_data["iso3"], with_data["source_id"], with_data["indicator_id"]))
    pairs = raw_ind[["source_id", "indicator_id"]].drop_duplicates().itertuples(index=False)

    rows = [
        {"iso3": iso3, "source_id": s, "indicator_id": i}
        for s, i in pairs
        for iso3 in accepted_iso3
        if (iso3, s, i) not in have
    ]
    return pd.DataFrame(rows, columns=["iso3", "source_id", "indicator_id"])


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------


def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    # 1 + 3. Leer y dar forma
    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(
                f"Transformer '{name}' no registrado. "
                f"Disponibles: {sorted(TRANSFORMERS)}"
            )
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    tables = {t: pd.concat(parts, ignore_index=True) for t, parts in buckets.items()}
    profile = tables["countries"]
    raw_ind = tables["indicators"]

    # 2. Criterio de país
    countries, excl_profile = apply_country_criteria(profile, cfg)
    accepted_iso3 = set(countries["iso3"])
    languages = tables["languages"][tables["languages"]["iso3"].isin(accepted_iso3)]
    ind, excl_ind = map_indicators_to_iso3(raw_ind, profile, accepted_iso3, cfg)
    excluded = pd.concat([excl_profile, excl_ind], ignore_index=True)

    # 4. Tipar y validar
    indicators = type_and_validate(ind)

    # 5. Huérfanos
    orphans = find_orphans(sorted(accepted_iso3), indicators, raw_ind)

    # Escritura en clean/
    countries.to_parquet(CLEAN / "countries.parquet", index=False)
    languages.to_parquet(CLEAN / "languages.parquet", index=False)
    ind_dir = CLEAN / "indicators"
    if ind_dir.exists():
        shutil.rmtree(ind_dir)  # si no, pyarrow añade ficheros a los de la vez anterior
    indicators.to_parquet(ind_dir, partition_cols=["indicator_id"], index=False)
    excluded.to_csv(CLEAN / "excluded_entities.csv", index=False)
    orphans.to_csv(CLEAN / "join_orphans.csv", index=False)
    # CSV que sigue leyendo load.py
    countries.to_csv(CLEAN / "countries.csv", index=False)
    languages.to_csv(CLEAN / "languages.csv", index=False)
    indicators.to_csv(CLEAN / "indicators.csv", index=False)

    print(f"[transform] countries: {len(countries)} aceptados, {len(excl_profile)} excluidos del perfil")
    print(f"[transform] languages: {len(languages)} filas")
    print(f"[transform] indicators: {len(raw_ind)} filas brutas → {len(indicators)} limpias")
    print(f"[transform] excluded_entities: {len(excluded)} filas")
    print(f"[transform] join_orphans: {len(orphans)} filas")

    return {
        "countries": countries,
        "languages": languages,
        "indicators": indicators,
        "excluded_entities": excluded,
        "join_orphans": orphans,
    }


if __name__ == "__main__":
    run_transform()
