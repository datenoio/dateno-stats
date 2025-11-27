#!/usr/bin/env python3
"""
Utility script that builds high-level statistics for the Dateno database by
leveraging Elasticsearch aggregations. The connection parameters are supplied
through environment variables to avoid committing credentials in the codebase.

Required environment variables:
  * CDIAPI_ELASTIC_KEY   – API key for the Elasticsearch instance
  * CDIAPI_ELASTIC_INDEX – Name of the index to query
"""

import csv
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import typer
from elasticsearch import Elasticsearch


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

app = typer.Typer(help="Generate aggregated statistics using Elasticsearch.")

BASE_DIR = Path(__file__).resolve().parent.parent
CURRENT_DIR = BASE_DIR / "data" / "current"
ARCHIVE_DIR = BASE_DIR / "data" / "archive"


def get_env_var(name: str) -> str:
    """Fetch a required environment variable or raise a clear error."""
    value = os.environ.get(name)
    if not value:
        typer.echo(f"Environment variable {name} is required.", err=True)
        raise typer.Exit(code=2)
    return value


def get_client(api_key: str) -> Elasticsearch:
    """
    Create a resilient Elasticsearch client pointing to the
    production instance at es.dateno.io.
    """
    host = os.environ.get("CDI_ELASTIC_HOST", "https://es.dateno.io")
    return Elasticsearch(
        host,
        api_key=api_key,
        verify_certs=False,
        ssl_show_warn=False,
        max_retries=10,
        retry_on_timeout=True,
    )


AGG_SIZE = 10_000

AGGREGATIONS: Dict[str, Dict] = {
    "catalog_type": {"terms": {"field": "source.catalog_type", "size": AGG_SIZE}},
    "formats": {"terms": {"field": "dataset.formats", "size": AGG_SIZE}},
    "topics": {"terms": {"field": "dataset.topics", "size": AGG_SIZE}},
    "datatypes": {"terms": {"field": "dataset.datatypes", "size": AGG_SIZE}},
    "geotopics": {"terms": {"field": "dataset.geotopics", "size": AGG_SIZE}},
    "macroregions": {"terms": {"field": "source.macroregions.name", "size": AGG_SIZE}},
    "subregions": {"terms": {"field": "source.subregions.name", "size": AGG_SIZE}},
    "owner_type": {"terms": {"field": "source.owner_type", "size": AGG_SIZE}},
    "countries": {"terms": {"field": "source.countries.name", "size": AGG_SIZE}},
    "langs": {"terms": {"field": "source.langs.name", "size": AGG_SIZE}},
#    "res_d_ext": {"terms": {"field": "resources.d_ext", "size": AGG_SIZE}},
    "res_d_mime": {"terms": {"field": "resources.d_mime", "size": AGG_SIZE}},
    "schemas": {"terms": {"field": "source.schema", "size": AGG_SIZE}},
 #   "license": {"terms": {"field": "dataset.license_id", "size": AGG_SIZE}},
    "sources": {"terms": {"field": "source.uid", "size": AGG_SIZE}},
}

STAT_FILE_NAMES: Dict[str, str] = {
    "catalog_type": "stats_type",
    "formats": "stats_formats",
    "topics": "stats_topics",
    "datatypes": "stats_datatypes",
    "geotopics": "stats_geotopics",
    "macroregions": "stats_macroregions",
    "subregions": "stats_subregions",
    "owner_type": "stats_owner",
    "countries": "stats_countries",
    "langs": "stats_langs",
#    "res_d_ext": "stats_res_d_ext",
    "res_d_mime": "stats_res_d_mime",
    "schemas": "stats_schemas",
#    "license": "stats_license",
    "sources": "stats_sources",
}


def ensure_directories() -> None:
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


def archive_current_data() -> Optional[Path]:
    """
    Move existing files from data/current into a timestamped archive folder.
    """
    contents = list(CURRENT_DIR.glob("*"))
    if not contents:
        logging.info("data/current is empty, nothing to archive.")
        return None

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    archive_target = ARCHIVE_DIR / timestamp
    archive_target.mkdir(parents=True, exist_ok=False)

    for path in contents:
        shutil.move(str(path), archive_target / path.name)

    logging.info("Archived %d files to %s", len(contents), archive_target)
    return archive_target


def write_dict_stat(file_stem: str, data_map: Dict[str, int]) -> None:
    """Persist a dictionary of counts into JSON and CSV outputs."""
    json_path = CURRENT_DIR / f"{file_stem}.json"
    csv_path = CURRENT_DIR / f"{file_stem}.csv"

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(data_map, handle, indent=2)

    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["name", "count"])
        for value, count in sorted(
            data_map.items(), key=lambda item: item[1], reverse=True
        ):
            writer.writerow([value, count])

    logging.info("Wrote %s and %s", json_path, csv_path)


def write_stat_files(name: str, buckets: Sequence[Dict[str, int]]) -> None:
    """Convert aggregation buckets to dictionaries and save them."""
    file_stem = STAT_FILE_NAMES.get(name, f"stats_{name}")
    data_map = {bucket["value"]: bucket["count"] for bucket in buckets}
    write_dict_stat(file_stem, data_map)


def write_all_stats(stats: Dict[str, List[Dict]]) -> List[str]:
    """
    Write all aggregations and return a list of dimensions with zero buckets.
    """
    missing: List[str] = []
    for name, buckets in stats.items():
        if not buckets:
            missing.append(name)
            continue
        write_stat_files(name, buckets)
    return missing


CONTINENTS_MAP = {
    "Western Europe": "Europe",
    "Northern America": "North America",
    "Australia and New Zealand": "Australia",
    "Northern Europe": "Europe",
    "Southern Europe": "Europe",
    "Eastern Europe": "Europe",
    "South America": "South America",
    "South-eastern Asia": "Asia",
    "Southern Asia": "Asia",
    "Eastern Asia": "Asia",
    "Central America": "North America",
    "Western Asia": "Asia",
    "Antarctica": "Antarctica",
    "Central Asia": "Asia",
    "Northern Africa": "Africa",
    "Western Africa": "Africa",
    "Eastern Africa": "Africa",
    "Caribbean": "North America",
    "Melanesia": "Australia",
    "Polynesia": "Australia",
    "Micronesia": "Australia",
    "Southern Africa": "Africa",
    "Middle Africa": "Africa",
}


def build_continent_stats() -> bool:
    """
    Derive continent-level stats from macroregions, mirroring extract.py behavior.
    """
    macro_path = CURRENT_DIR / "stats_macroregions.json"
    if not macro_path.exists():
        logging.warning("Cannot generate continent stats: %s missing", macro_path)
        return False
    with open(macro_path, "r", encoding="utf-8") as handle:
        macroregions = json.load(handle)

    results: Dict[str, int] = {}
    for region, count in macroregions.items():
        continent = CONTINENTS_MAP.get(region)
        if not continent:
            continue
        results[continent] = results.get(continent, 0) + count

    if not results:
        logging.warning("No continent mappings produced results.")
        return False

    write_dict_stat("stats_continents", results)
    return True


def build_totals_stats() -> bool:
    """Generate total sources/datasets counts as extract.py does."""
    sources_path = CURRENT_DIR / "stats_sources.json"
    if not sources_path.exists():
        logging.warning("Cannot generate totals: %s missing", sources_path)
        return False
    with open(sources_path, "r", encoding="utf-8") as handle:
        sources = json.load(handle)

    total_sources = len(sources)
    total_datasets = sum(sources.values())
    write_dict_stat("stats_totals", {"sources": total_sources, "datasets": total_datasets})
    return True


def parse_filters(raw_filters: Optional[str]) -> Dict:
    """
    Convert a semicolon-delimited filter expression (field=value;field=value)
    into an Elasticsearch bool filter clause compatible with the aggs example
    in scripts/datenoel.py.
    """
    if not raw_filters:
        return {}

    must_terms: List[Dict] = []
    for raw in raw_filters.split(";"):
        parts = raw.split("=", 1)
        if len(parts) != 2:
            logging.warning("Skipping malformed filter expression: %s", raw)
            continue
        field, value = parts
        must_terms.append({"term": {field.strip('"'): {"value": value.strip('"')}}})

    if not must_terms:
        return {}

    return {"bool": {"must": must_terms}}


@app.command()
def build(
    filters: Optional[str] = typer.Option(
        None,
        "--filters",
        "-f",
        help='Optional filters in form field=value;another.field=value',
    ),
    output: Path = typer.Option(
        CURRENT_DIR / "stats_summary.json",
        "--output",
        "-o",
        help="Path to store the aggregated statistics JSON summary.",
    ),
) -> None:
    """
    Execute the aggregation query and save results to JSON.
    """
    ensure_directories()
    archive_current_data()

    api_key = get_env_var("CDIAPI_ELASTIC_KEY")
    index = get_env_var("CDIAPI_ELASTIC_INDEX")
    client = get_client(api_key)

    post_filter = parse_filters(filters)
    query = {"bool": {"filter": post_filter.get("bool", {}).get("must", [])}}

    logging.info("Running aggregations against index '%s'", index)
    try:
        response = client.search(
            index=index,
            aggs=AGGREGATIONS,
            query=query if query["bool"]["filter"] else {"match_all": {}},
            post_filter=post_filter or None,
            size=0,
        )
    except Exception as exc:  # noqa: BLE001 - catch ES client errors
        logging.error("Failed to gather statistics: %s", exc)
        raise typer.Exit(code=1) from exc

    stats = {
        name: [{"value": bucket["key"], "count": bucket["doc_count"]} for bucket in agg["buckets"]]
        for name, agg in response.get("aggregations", {}).items()
    }

    missing_dimensions = write_all_stats(stats)
    continents_generated = build_continent_stats()
    totals_generated = build_totals_stats()

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "index": index,
                "filters": filters,
                "stats": stats,
                "missing_dimensions": missing_dimensions,
                "custom_stats": {
                    "continents": continents_generated,
                    "totals": totals_generated,
                },
            },
            handle,
            indent=2,
        )

    logging.info("Saved summary to %s", output)

    typer.echo(
        json.dumps(
            {
                "stats": stats,
                "missing_dimensions": missing_dimensions,
                "custom_stats": {
                    "continents": continents_generated,
                    "totals": totals_generated,
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    app()

