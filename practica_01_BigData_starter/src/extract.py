
"""Extract: descarga intacta -> data/raw/ + manifiesto.

No transforma. Añadir una fuente nueva = entrada en sources.yaml.
"""

from __future__ import annotations

import csv
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import requests

from .config import Config, Source, load_config
from .paths import MANIFEST, RAW, ensure_dirs

USER_AGENT = "UCJC-DSBD-practica2/1.0 (educational ETL)"
TIMEOUT = 60

MAX_ATTEMPTS = 5
RETRY_DELAYS = [1, 2, 4, 8]
PER_PAGE = 1000
MIN_REQUEST_INTERVAL = 0.2

_last_request_time = 0.0

FIELDNAMES = [
    "run_id",
    "timestamp_utc",
    "source_id",
    "indicator_id",
    "url",
    "http_status",
    "bytes",
    "filename",
    "page",
    "total_pages",
    "expected_records",
    "extracted_records",
    "cache_hit",
    "iso2",
    "iso3",
    "note",
]


def _raw_name(
    source: Source,
    iso2: str | None = None,
    indicator_id: str | None = None,
    page: int | None = None,
) -> str:
    """Genera el nombre del archivo bruto."""

    if indicator_id:
        return (
            f"{source.id}_{indicator_id}_page_{page}."
            f"{source.format}"
        )

    if source.url_template and iso2:
        return f"{source.id}_{iso2}.{source.format}"

    return f"{source.id}.{source.format}"


def _download(url: str, dest: Path) -> tuple[int, int]:
    """Descarga una URL con reintentos y guarda solo HTTP 200."""

    global _last_request_time

    for attempt in range(MAX_ATTEMPTS):
        elapsed = time.monotonic() - _last_request_time

        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)

        _last_request_time = time.monotonic()

        try:
            resp = requests.get(
                url,
                timeout=TIMEOUT,
                headers={"User-Agent": USER_AGENT},
            )

            if resp.status_code == 200:
                if dest.suffix == ".json":
                    json.loads(resp.content.decode("utf-8-sig"))

                elif dest.suffix == ".csv":
                    text = resp.content.decode("utf-8-sig")
                    if not next(csv.reader(text.splitlines()), None):
                        raise ValueError("CSV vacío o sin cabecera.")

                dest.parent.mkdir(parents=True, exist_ok=True)

                temp = dest.with_suffix(dest.suffix + ".tmp")
                temp.write_bytes(resp.content)
                temp.replace(dest)

                return resp.status_code, len(resp.content)

            if resp.status_code != 429 and not 500 <= resp.status_code < 600:
                resp.raise_for_status()

            if attempt == MAX_ATTEMPTS - 1:
                resp.raise_for_status()

        except (requests.Timeout, requests.ConnectionError):
            if attempt == MAX_ATTEMPTS - 1:
                raise

        if attempt < MAX_ATTEMPTS - 1:
            time.sleep(RETRY_DELAYS[attempt])

    raise RuntimeError(f"No se pudo descargar {url}")


def _load_manifest() -> list[dict]:
    """Lee el manifiesto existente para comprobar descargas anteriores."""

    if not MANIFEST.exists():
        return []

    with MANIFEST.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _is_cached(
    url: str,
    dest: Path,
    source: Source,
    manifest: list[dict],
    indicator_id: str = "",
    page: int = 1,
) -> bool:
    """Comprueba si ya existe una descarga válida de la misma URL."""

    if not dest.is_file():
        return False

    for row in reversed(manifest):
        if (
            row.get("url") == url
            and row.get("filename") == dest.name
            and row.get("http_status") == "200"
            and row.get("indicator_id", "") == indicator_id
            and row.get("page", "1") == str(page)
        ):
            try:
                content = dest.read_bytes()

                if source.format == "json":
                    json.loads(content.decode("utf-8-sig"))

                    if source.transformer == "worldbank_indicator":
                        _parse_worldbank(content, page)

                elif source.format == "csv":
                    text = content.decode("utf-8-sig")
                    if not next(csv.reader(text.splitlines()), None):
                        return False

                return True

            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                return False

    return False


def _parse_worldbank(content: bytes, page: int) -> tuple[dict, list]:
    """Obtiene los metadatos y registros de una página del Banco Mundial."""

    data = json.loads(content.decode("utf-8-sig"))

    if not isinstance(data, list) or len(data) < 2:
        raise ValueError("Respuesta del Banco Mundial no válida.")

    metadata, records = data[0], data[1]

    if not isinstance(metadata, dict):
        raise ValueError("Metadatos no válidos.")

    if int(metadata["page"]) != page:
        raise ValueError("Número de página incorrecto.")

    if records is None:
        records = []

    if not isinstance(records, list):
        raise ValueError("Los registros no tienen formato de lista.")

    return metadata, records


def _worldbank_url(source: Source, indicator_id: str) -> str:
    """Construye la URL completa de un indicador WDI."""

    parts = urlsplit(source.resolve_url())

    if parts.path.rstrip("/").endswith("/indicator"):
        path = parts.path.rstrip("/") + "/" + indicator_id

    elif "/indicator/" in parts.path:
        path = parts.path.rsplit("/indicator/", 1)[0]
        path += "/indicator/" + indicator_id

    else:
        raise ValueError(f"URL del Banco Mundial no válida: {source.id}")

    return urlunsplit((
        parts.scheme,
        parts.netloc,
        path,
        "format=json",
        "",
    ))


def _extract_worldbank(
    source: Source,
    indicator_id: str,
    run_id: str,
    manifest: list[dict],
) -> list[dict]:
    """Descarga todas las páginas de un indicador del Banco Mundial."""

    rows = []
    base_url = _worldbank_url(source, indicator_id)

    total_records = 0
    expected_records = None
    total_pages = None

    for page in range(1, 100000):
        url = (
            f"{base_url}&page={page}&per_page={PER_PAGE}"
        )

        filename = _raw_name(
            source,
            indicator_id=indicator_id,
            page=page,
        )

        dest = RAW / filename

        try:
            cache_hit = _is_cached(
                url, dest, source, manifest,
                indicator_id, page,
            )

            if cache_hit:
                content = dest.read_bytes()
                status = 200
                nbytes = len(content)

            else:
                status, nbytes = _download(url, dest)
                content = dest.read_bytes()

            metadata, records = _parse_worldbank(content, page)

            if expected_records is None:
                expected_records = int(metadata["total"])
                total_pages = int(metadata["pages"])

                if total_pages < 1:
                    raise ValueError("Número de páginas no válido.")

            elif (
                int(metadata["total"]) != expected_records
                or int(metadata["pages"]) != total_pages
            ):
                raise ValueError("Los metadatos de paginación han cambiado.")

            total_records += len(records)

            rows.append({
                "run_id": run_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "source_id": source.id,
                "indicator_id": indicator_id,
                "url": url,
                "http_status": status,
                "bytes": nbytes,
                "filename": filename,
                "page": page,
                "total_pages": total_pages,
                "expected_records": expected_records,
                "extracted_records": len(records),
                "cache_hit": cache_hit,
                "iso2": "",
                "iso3": "",
                "note": "",
            })

            if page == total_pages:
                break

        except Exception as exc:
            response = getattr(exc, "response", None)

            rows.append({
                "run_id": run_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "source_id": source.id,
                "indicator_id": indicator_id,
                "url": url,
                "http_status": response.status_code if response is not None else 0,
                "bytes": 0,
                "filename": filename,
                "page": page,
                "total_pages": total_pages or "",
                "expected_records": expected_records or "",
                "extracted_records": 0,
                "cache_hit": False,
                "iso2": "",
                "iso3": "",
                "note": str(exc),
            })

            break

    if total_pages is not None and page == total_pages:
        if total_records != expected_records:
            rows.append({
                "run_id": run_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "source_id": source.id,
                "indicator_id": indicator_id,
                "url": base_url,
                "http_status": 0,
                "bytes": 0,
                "filename": "",
                "page": "",
                "total_pages": total_pages,
                "expected_records": expected_records,
                "extracted_records": total_records,
                "cache_hit": False,
                "iso2": "",
                "iso3": "",
                "note": "El total de registros descargados no coincide con la API.",
            })

    return rows


def extract_source(
    source: Source,
    cfg: Config,
    run_id: str,
    manifest: list[dict],
) -> list[dict]:
    """Descarga una fuente; conserva la lógica original de países."""

    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    # Fuentes del Banco Mundial.
    if source.transformer == "worldbank_indicator":
        indicators = []

        main_indicator = source.meta.get("indicator_id")

        if main_indicator:
            indicators.append(main_indicator)

        for item in source.meta.get("additional_indicators", []):
            if item["indicator_id"] not in indicators:
                indicators.append(item["indicator_id"])

        for indicator_id in indicators:
            rows.extend(
                _extract_worldbank(
                    source, indicator_id, run_id, manifest
                )
            )

        return rows

    # Resto de fuentes: se conserva el comportamiento original.
    if source.url_template and cfg.focus_countries:
        targets = cfg.focus_countries
    else:
        targets = [None]

    for country in targets:
        iso2 = country.iso2 if country else ""
        iso3 = country.iso3 if country else ""

        url = source.resolve_url(iso2=iso2, iso3=iso3)
        filename = _raw_name(source, iso2 or None)
        dest = RAW / filename

        try:
            cache_hit = _is_cached(
                url, dest, source, manifest,
                source.meta.get("indicator_id", ""), 1
            )

            if cache_hit:
                status = 200
                nbytes = dest.stat().st_size
            else:
                status, nbytes = _download(url, dest)

            note = ""

        except Exception as exc:
            response = getattr(exc, "response", None)
            status = response.status_code if response is not None else 0
            nbytes = 0
            note = str(exc)
            cache_hit = False

        rows.append({
            "run_id": run_id,
            "timestamp_utc": now,
            "source_id": source.id,
            "indicator_id": source.meta.get("indicator_id", ""),
            "url": url,
            "http_status": status,
            "bytes": nbytes,
            "filename": filename,
            "page": 1,
            "total_pages": 1,
            "expected_records": "",
            "extracted_records": "",
            "cache_hit": cache_hit,
            "iso2": iso2,
            "iso3": iso3,
            "note": note,
        })

    return rows


def run_extract(cfg: Config | None = None) -> Path:
    """Ejecuta la extracción de todas las fuentes."""

    cfg = cfg or load_config()
    ensure_dirs()

    run_id = str(uuid.uuid4())
    previous_rows = _load_manifest()

    all_rows: list[dict] = []

    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")

        all_rows.extend(
            extract_source(source, cfg, run_id, previous_rows)
        )

    # Conserva el manifiesto anterior y añade la nueva ejecución.
    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(
            {field: row.get(field, "") for field in FIELDNAMES}
            for row in previous_rows
        )
        writer.writerows(all_rows)

    print(f"[extract] manifiesto -> {MANIFEST}")

    failures = [
        row for row in all_rows
        if row["http_status"] != 200 or row["note"]
    ]

    if failures:
        raise RuntimeError(
            f"La extracción terminó con {len(failures)} errores. "
            "Consulta manifest.csv."
        )

    print(f"[extract] ejecución completada: {run_id}")

    return MANIFEST


if __name__ == "__main__":
    run_extract()
