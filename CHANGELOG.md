## 2025-11-27

- Added `scripts/build_es_stats.py` automation to archive previous outputs, run Elasticsearch aggregations, and emit JSON/CSV statistics aligned with the legacy Mongo pipeline.
- Extended the generator with new dimensions (datatypes, geotopics, macro/sub-regions, schemas, resources, etc.), totals/continents derived metrics, filter support, and environment-driven host selection.
- Documented the data directory structure and refresh workflow in `README.md`, including the required environment variables and optional filters.

## n.n.n / 2024-04-13

- Rebuilt stats after PxWeb reindex.
- Updated 2024-04 stats.
- Added more stats views and mimetype report.
- Merged upstream `main`, created `README.md`, and published the initial Dateno statistics repository.

