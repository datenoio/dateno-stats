# Dateno statistics

This repository keeps statistics from Dateno search service dateno.io

## Directory layout

* `data/current` – output of the latest statistics run. Each metric is written as a pair of JSON/CSV files, plus a `stats_summary.json` describing the run.
* `data/archive` – timestamped folders containing previous `data/current` contents. Every refresh moves the existing snapshots into a folder named `YYYY-MM-DD_HH-MM-SS`.

Statistics are updated irregularly for now, but later it will be published monthly or weekly, not more often yet since it's resource consuming.

It's not yet about data quality, but quality reports are next step and will be available in this repository later.

## Updating the statistics

Statistics are recomputed with `scripts/build_es_stats.py`, which queries the Dateno Elasticsearch index using the same aggregation logic as the historic MongoDB pipeline.

1. Set the required environment variables:

   ```bash
   export CDIAPI_ELASTIC_KEY="<api key>"
   export CDIAPI_ELASTIC_INDEX="<es index name>"
   # optional: override cluster host (defaults to https://es.dateno.io)
   export CDI_ELASTIC_HOST="https://es.dateno.io"
   ```

2. Run the generator:

   ```bash
   python scripts/build_es_stats.py build
   # optional filters example:
   python scripts/build_es_stats.py build --filters 'source.catalog_type=portal'
   ```

What the script does:

* ensures `data/current` and `data/archive` exist
* moves any existing files from `data/current` into a timestamped archive folder
* executes Elasticsearch term aggregations for all supported dimensions
* writes fresh JSON/CSV files into `data/current`
* derives `stats_continents.*` (from macroregions) and `stats_totals.*` (from source counts)
* writes a `stats_summary.json` containing metadata, missing-dimension warnings, and flags that indicate whether the custom continent/total statistics were generated

## Available statistics

All statistics available as JSON and CSV files:

* crawledsources - List of all crawled data source. UID identifiers from Dateno registry
* stats_continents - Statistics by continents. Schema: name, count
* stats_countries - Statistics by country names. Schema: name, count
* stats_country_owner - Statisitcs by country and catalog owner type. Schema: country, owner_type, count
* stats_country_software - Statistics by country and software. Schema: country, software, count
* stats_country_type - Statistics by country and catalog_type. Schema: country, catalog_type, count
* stats_datatypes - Statistics by data types. Schema: name, count
* stats_formats -  - Statistics by identified formats (cleaned up). Schema: name, count
* stats_geotopics - Statistics by geodata classification, topics by ISO 19115. Schema: name, count
* stats_langs - Statistics by spoken languages. Schema: name, count
* stats_license - Statistics by license types. Schema: name, count
* stats_macroregions  - Statistics by macroregions by UN49 classification. Schema: name, count
* stats_owner - Statistics by catalog owner type. Schema: name, count
* stats_res_d_ext - Statistics by detected resource file extensions (cleaned). Schema: name, count
* stats_res_d_mime - Statistics by detected resource mimetypes (cleaned). Schema: name, count
* stats_res_formats - Statistics by resource formats (not cleaned). Schema: name, count
* stats_res_mimetypes  - Statistics by resource mimetypes (not cleaned). Schema: name, count
* stats_schemas  - Statistics by metadata schemas. Schema: name, count
* stats_software - Statistics by software of the data catalogs. Schema: name, count
* stats_sources - Statistics by each data source. Schema: name, count
* stats_subregions - Statistics by subregions. Schema: name, count
* stats_topics - Statistics by topics from EU datasets topics classification. Schema: name, count
* stats_totals - Statistics by total numbers of datasets and indexed data catalogs. Schena: name, count
* stats_type  - Statistics by data catalog types. Schema: name, count

