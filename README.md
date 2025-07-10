☀️ Terminology Server POC ☀️

Steps to setup
 - Clone the repo
 - Open it in Code Editor
 - Use pipenv for dependencies management
 - Add `SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z` folder in Project root directory
 - Set up ElasticSearch and Kibana Server in Ubuntu environment
 - To ingest data in ES run `python -m terminology_api.es_indexer.indexer`
 - To Test API run `python manage.py runserver`
