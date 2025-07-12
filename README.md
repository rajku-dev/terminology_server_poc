# ☀️ Terminology Server POC

A proof-of-concept terminology server for managing and querying medical terminologies.

## Prerequisites

- Python 3.x
- Pipenv
- ElasticSearch
- Kibana
- Linux environment

## Setup Instructions

### 1. Repository Setup
```bash
git clone https://github.com/rajku-dev/terminology_server_poc.git
cd terminology_server_poc
```

### 2. Environment Setup
```bash
# Install dependencies using pipenv
pipenv install
pipenv shell
```

### 3. Data Preparation
Download and place the `SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z` folder in the project root directory.

### 4. ElasticSearch & Kibana Configuration
Set up ElasticSearch and Kibana servers in your Ubuntu environment.

### 5. Data Ingestion
```bash
# Index data into ElasticSearch
python -m terminology_api.es_indexer.indexer
```

### 6. Running the Server
```bash
# Start the development server
python manage.py runserver
```

## Getting Started

Once the server is running, you can access the API endpoints to query terminologies and test the functionality.

---
