# Dateno statistics

This repository keeps statistics from Dateno search service dateno.io

* data/current - most recent statistics
* data/archive - archived statistics

Statistics updated irregularly for now, but later it will be published monthly or weekly, not more often yet since it's resource consuming.

It's not yet about data quality, but quality reports are next step and will be available in this repository later.

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

