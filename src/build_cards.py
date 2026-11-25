"""Genera fichas Markdown desde data/serving/ (nunca desde raw/)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from .paths import CARDS, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"


def _read_table(conn: sqlite3.Connection, name: str) -> pd.DataFrame:
    try:
        return pd.read_sql_query(f"SELECT * FROM {name}", conn)
    except Exception:
        return pd.DataFrame()


def _card_markdown(iso3: str, countries: pd.DataFrame, languages: pd.DataFrame,
                   indicators: pd.DataFrame) -> str:
    c = countries[countries["iso3"] == iso3].iloc[0]
    langs = languages[languages["iso3"] == iso3]
    inds = indicators[indicators["iso3"] == iso3]
    with_value = inds[inds["value"].notna()]

    lang_lines = (
        "\n".join(
            f"- `{r.language_code}`: {r.language_name}" for _, r in langs.iterrows()
        )
        or "- (sin idiomas en clean/)"
    )
    # Con toda la serie histórica, solo el último año con dato de cada indicador
    latest = with_value.sort_values("year").groupby("indicator_id").tail(1)
    ind_lines = (
        "\n".join(
            f"- **{r.indicator_label}** ({r.year}): {r.value:,.2f} {r.unit or ''} "
            f"_(fuente: {r.source_id})_"
            for _, r in latest.sort_values("indicator_id").iterrows()
        )
        or "- (sin indicadores; posible huérfano del join)"
    )

    # Mediana de la región en el mismo indicador y el mismo año: el último año
    # con algún dato en la región, tenga o no dato este país
    region = c.get("region")
    peers = indicators[
        indicators["iso3"].isin(countries.loc[countries["region"] == region, "iso3"])
        & indicators["value"].notna()
    ]
    comp_lines = []
    for ind_id, peer in peers.groupby("indicator_id"):
        year = peer["year"].max()
        median = peer.loc[peer["year"] == year, "value"].median()
        own = with_value.loc[
            (with_value["indicator_id"] == ind_id) & (with_value["year"] == year), "value"
        ]
        head = f"- `{ind_id}` ({year}): mediana de **{region}** = {median:,.2f}; "
        if own.empty:
            comp_lines.append(head + f"este país sin dato en {year}.")
        else:
            v = own.iloc[0]
            pos = "por encima" if v > median else "por debajo o igual"
            comp_lines.append(head + f"este país **{v:,.2f}** ({pos}).")
    comparative = "\n".join(comp_lines) or "_Sin indicadores para comparar._"

    return f"""# Ficha país — {c['name_common']} ({iso3})

- **Nombre oficial:** {c.get('name_official')}
- **ISO2 / ISO3:** {c.get('iso2')} / {iso3}
- **Región / subregión:** {c.get('region')} / {c.get('subregion')}

## Idiomas

{lang_lines}

## Indicadores

{ind_lines}

## Comparativo

{comparative}

---
Generado automáticamente el {date.today().isoformat()} desde `data/serving/observatorio.sqlite`.
"""


def run_build_cards() -> list[Path]:
    ensure_dirs()
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"No existe {DB_PATH}. Ejecutad antes extract → transform → load."
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        countries = _read_table(conn, "countries")
        languages = _read_table(conn, "languages")
        indicators = _read_table(conn, "indicators")
    finally:
        conn.close()

    if countries.empty:
        raise RuntimeError("Tabla countries vacía: revisad transform.")

    written: list[Path] = []
    for iso3 in countries["iso3"].tolist():
        md = _card_markdown(iso3, countries, languages, indicators)
        path = CARDS / f"{iso3}.md"
        path.write_text(md, encoding="utf-8")
        written.append(path)
        print(f"[cards] {path}")
    return written


if __name__ == "__main__":
    run_build_cards()
