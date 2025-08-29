# Terminology Server Docker Commands

## Prerequisites Setup

1. **Install Docker and Docker Compose** (Ubuntu):
```bash
sudo apt update
sudo apt install docker.io docker-compose
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER
# Log out and log back in for group changes to take effect
```

2. **Create project structure**:
```bash
git clone https://github.com/rajku-dev/terminology_server_poc.git
cd terminology_server_poc
mkdir -p data/SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z
mkdir -p data/loinc
mkdir -p logs
```

3. **Place the required data files**:
   - Download and extract SNOMED CT data to `./data/SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z/`
   - Download and extract LOINC data to `./data/loinc/`

## Quick Start Commands

### 1. Basic Setup (Infrastructure Only)
```bash
# Start Elasticsearch and Kibana
docker-compose up -d elasticsearch kibana

# Check if Elasticsearch is ready
curl http://localhost:9200/_cluster/health
```

### 2. Approach 1: Basic Indexing
```bash
# Build the application image
docker-compose build

# Start infrastructure
docker-compose up -d elasticsearch kibana

# Wait for Elasticsearch to be ready (about 30 seconds)
sleep 30

# Index SNOMED CT data
docker-compose --profile indexing up snomed_indexer

# Index LOINC data
docker-compose --profile indexing up loinc_indexer

# Start the main application
docker-compose up -d terminology_server
```

### 3. Approach 2: Valueset-First with Caching
```bash
# Build the application image
docker-compose build

# Start infrastructure
docker-compose up -d elasticsearch kibana

# Wait for Elasticsearch to be ready
sleep 30

# Index SNOMED CT data
docker-compose --profile indexing up snomed_indexer

# Optimize with caching
docker-compose --profile valueset-first up snomed_cache_optimizer

# Index LOINC data
docker-compose --profile indexing up loinc_indexer

# Start the main application
docker-compose up -d terminology_server
```

## Individual Service Commands

### Start/Stop Individual Services
```bash
# Start only Elasticsearch
docker-compose up -d elasticsearch

# Start only Kibana
docker-compose up -d kibana

# Start only the terminology server
docker-compose up -d terminology_server

# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: This deletes all indexed data!)
docker-compose down -v
```

### Manual Indexing Commands
```bash
# Run SNOMED indexing manually
docker-compose run --rm snomed_indexer

# Run LOINC indexing manually
docker-compose run --rm loinc_indexer

# Run SNOMED cache optimization
docker-compose run --rm snomed_cache_optimizer
```

### Debugging and Monitoring
```bash
# View application logs
docker-compose logs -f terminology_server

# View Elasticsearch logs
docker-compose logs -f elasticsearch

# Check Elasticsearch indices
curl http://localhost:9200/_cat/indices?v

# Check Elasticsearch cluster health
curl http://localhost:9200/_cluster/health?pretty

# Execute commands inside the running container
docker-compose exec terminology_server bash

# Check running containers
docker-compose ps
```

### Data Management
```bash
# Backup Elasticsearch data
docker run --rm -v terminology_server_poc_elasticsearch_data:/data -v $(pwd):/backup ubuntu tar czf /backup/elasticsearch-backup.tar.gz /data

# Restore Elasticsearch data
docker run --rm -v terminology_server_poc_elasticsearch_data:/data -v $(pwd):/backup ubuntu bash -c "cd /data && tar xzf /backup/elasticsearch-backup.tar.gz --strip 1"
```

## Access Points

Once everything is running, you can access:

- **Terminology Server API**: http://localhost:8000
- **Elasticsearch**: http://localhost:9200
- **Kibana**: http://localhost:5601

## Troubleshooting

### Common Issues and Solutions

1. **Elasticsearch fails to start**:
```bash
# Check if you have enough memory
docker-compose logs elasticsearch
# Increase memory limits in docker-compose.yml if needed
```

2. **Permission errors**:
```bash
sudo chown -R $USER:$USER .
sudo chmod +x setup.sh
```

3. **Port conflicts**:
```bash
# Check which ports are in use
sudo netstat -tulpn | grep :9200
sudo netstat -tulpn | grep :8000
```

4. **Clear everything and start fresh**:
```bash
docker-compose down -v
docker system prune -a
# Then run setup again
```

## Environment Variables

Key environment variables you can modify in `.env`:

- `ELASTICSEARCH_HOST`: Elasticsearch hostname (default: elasticsearch)
- `ELASTICSEARCH_PORT`: Elasticsearch port (default: 9200)
- `SNOMED_DATA_PATH`: Path to SNOMED CT data
- `LOINC_DATA_PATH`: Path to LOINC data
- `INDEX_BATCH_SIZE`: Batch size for indexing (default: 1000)
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

## Performance Tuning

For production use, consider:

1. **Increase Elasticsearch memory**:
```yaml
environment:
  - "ES_JAVA_OPTS=-Xms2g -Xmx4g"
```

2. **Add more Elasticsearch nodes** for clustering
3. **Use SSD storage** for better I/O performance
4. **Tune indexing batch sizes** based on your data volume