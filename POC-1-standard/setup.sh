#!/bin/bash

# Terminology Server Docker Setup Script
# Run this script to set up the terminology server with Docker

set -e

echo "=== Terminology Server Docker Setup ==="
echo

# Check if Docker and Docker Compose are installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed. Please install Docker first."
        echo "Run: sudo apt update && sudo apt install docker.io docker-compose"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo "Docker Compose is not installed. Please install Docker Compose first."
        echo "Run: sudo apt install docker-compose"
        exit 1
    fi
    
    echo "✓ Docker and Docker Compose are installed"
}

# Create necessary directories
create_directories() {
    echo "Creating necessary directories..."
    mkdir -p data/SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z
    mkdir -p data/loinc
    mkdir -p logs
    echo "✓ Directories created"
}

# Download and setup data (placeholder - user needs to provide actual data)
setup_data() {
    echo
    echo "=== DATA SETUP REQUIRED ==="
    echo "You need to manually place the following data files:"
    echo
    echo "1. SNOMED CT Data:"
    echo "   - Download SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z"
    echo "   - Extract to: ./data/SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z/"
    echo
    echo "2. LOINC Data:"
    echo "   - Download LOINC data files"
    echo "   - Extract to: ./data/loinc/"
    echo
    echo "Press Enter when data is ready, or Ctrl+C to exit and setup data first..."
    read -r
}

# Build and start services
start_services() {
    echo "Building Docker images..."
    docker-compose build
    
    echo "Starting Elasticsearch and Kibana..."
    docker-compose up -d elasticsearch kibana
    
    echo "Waiting for Elasticsearch to be ready..."
    sleep 30
    
    # Check if Elasticsearch is healthy
    while ! curl -s http://localhost:9200/_cluster/health | grep -q '"status":"yellow\|green"'; do
        echo "Waiting for Elasticsearch..."
        sleep 10
    done
    
    echo "✓ Elasticsearch is ready"
}

# Function to run basic indexing approach
run_basic_indexing() {
    echo
    echo "=== RUNNING BASIC INDEXING APPROACH ==="
    echo
    echo "Step 1: Indexing SNOMED CT data..."
    docker-compose --profile indexing up snomed_indexer
    
    echo "Step 2: Indexing LOINC data..."
    docker-compose --profile indexing up loinc_indexer
    
    echo "✓ Basic indexing completed"
}

# Function to run valueset-first approach
run_valueset_first() {
    echo
    echo "=== RUNNING VALUESET-FIRST APPROACH ==="
    echo
    echo "Step 1: Indexing SNOMED CT data..."
    docker-compose --profile indexing up snomed_indexer
    
    echo "Step 2: Optimizing with caches..."
    docker-compose --profile valueset-first up snomed_cache_optimizer
    
    echo "Step 3: Indexing LOINC data..."
    docker-compose --profile indexing up loinc_indexer
    
    echo "✓ Valueset-first approach completed"
}

# Start the main application
start_application() {
    echo
    echo "Starting the terminology server application..."
    docker-compose up -d terminology_server
    
    echo
    echo "=== SETUP COMPLETE ==="
    echo
    echo "Services are now running:"
    echo "- Terminology Server: http://localhost:8000"
    echo "- Elasticsearch: http://localhost:9200"
    echo "- Kibana: http://localhost:5601"
    echo
    echo "To view logs: docker-compose logs -f terminology_server"
    echo "To stop services: docker-compose down"
}

# Main execution
main() {
    echo "Choose your setup approach:"
    echo "1) Basic Indexing Approach"
    echo "2) Valueset-First Approach"
    echo "3) Setup infrastructure only (no indexing)"
    echo
    read -p "Enter your choice (1, 2, or 3): " choice
    
    check_docker
    create_directories
    setup_data
    start_services
    
    case $choice in
        1)
            run_basic_indexing
            ;;
        2)
            run_valueset_first
            ;;
        3)
            echo "Skipping indexing - infrastructure only"
            ;;
        *)
            echo "Invalid choice. Defaulting to infrastructure only."
            ;;
    esac
    
    start_application
}

# Run main function
main "$@"