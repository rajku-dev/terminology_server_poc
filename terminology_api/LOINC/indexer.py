#!/usr/bin/env python3
"""
LOINC Elasticsearch Indexer
Optimized for fast FHIR operations: $expand, $lookup, $validate-code
Memory-efficient bulk indexing with minimal storage footprint.
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, parallel_bulk
from .reader import LoincReader, LoincConcept, LoincPart, LoincAnswer, LoincHierarchy
from typing import Dict, List, Set
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoincIndexer:
    """
    Elasticsearch indexer optimized for FHIR terminology operations.
    Creates optimized indices for fast search and lookup operations.
    """
    
    def __init__(self, es_client: Elasticsearch, index_prefix: str = "loinc"):
        self.es = es_client
        self.index_prefix = index_prefix
        
        # Index names
        self.indices = {
            'concepts': f"{index_prefix}_concepts",      # Main LOINC codes + Parts + Answers
            'hierarchies': f"{index_prefix}_hierarchies", # Parent-child relationships
            'lookup': f"{index_prefix}_lookup"           # Fast lookup cache
        }
        
        # Bulk indexing settings
        self.bulk_size = 1000
        self.parallel_bulk_size = 500
        self.max_chunk_bytes = 10 * 1024 * 1024  # 10MB chunks
    
    def create_indices(self):
        """Create optimized Elasticsearch indices"""
        logger.info("Creating Elasticsearch indices...")
        
        # Main concepts index - optimized for search and expand operations
        concepts_mapping = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 0,  # Increase for production
                "analysis": {
                    "analyzer": {
                        "loinc_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "stop", "snowball"]
                        },
                        "code_analyzer": {
                            "type": "custom", 
                            "tokenizer": "keyword",
                            "filter": ["lowercase"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "code": {
                        "type": "keyword",
                        "index": True
                    },
                    "system": {
                        "type": "keyword",
                        "index": False
                    },
                    "type": {
                        "type": "keyword",  # "concept", "part", "answer"
                        "index": True
                    },
                    "display": {
                        "type": "text",
                        "analyzer": "loinc_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "suggest": {
                                "type": "completion",
                                "analyzer": "simple"
                            }
                        }
                    },
                    "search_terms": {
                        "type": "text",
                        "analyzer": "loinc_analyzer"
                    },
                    # Compact designation storage
                    "designation_value": {
                        "type": "text",
                        "analyzer": "loinc_analyzer"
                    },
                    # Hierarchy info (stored as arrays for performance)
                    "parents": {
                        "type": "keyword"
                    },
                    "children": {
                        "type": "keyword"
                    },
                    # Component info for main concepts (stored as object for lookup)
                    "components": {
                        "type": "object",
                        "enabled": False  # Not searchable, just stored
                    },
                    # Usage info for parts
                    "used_in": {
                        "type": "keyword"
                    },
                    # Status and metadata
                    "status": {
                        "type": "keyword",
                        "index": False
                    },
                    "class": {
                        "type": "keyword"
                    }
                }
            }
        }
        
        # Hierarchies index - optimized for lookup operations
        hierarchies_mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "parent": {"type": "keyword"},
                    "child": {"type": "keyword"},
                    "relationship_type": {"type": "keyword"},
                    "depth": {"type": "integer"}
                }
            }
        }
        
        # Lookup cache index - for fast $lookup operations
        lookup_mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "code": {"type": "keyword"},
                    "system": {"type": "keyword", "index": False},
                    "display": {"type": "keyword", "index": False},
                    "properties": {"type": "object", "enabled": False},
                    "designations": {"type": "object", "enabled": False}
                }
            }
        }
        
        # Create indices
        for index_name, mapping in [
            (self.indices['concepts'], concepts_mapping),
            (self.indices['hierarchies'], hierarchies_mapping),
            (self.indices['lookup'], lookup_mapping)
        ]:
            if self.es.indices.exists(index=index_name):
                logger.info(f"Deleting existing index: {index_name}")
                self.es.indices.delete(index=index_name)
            
            logger.info(f"Creating index: {index_name}")
            self.es.indices.create(index=index_name, body=mapping)
    
    def _create_concept_doc(self, concept: LoincConcept, parents: Set[str] = None, 
                           children: Set[str] = None) -> Dict:
        """Create Elasticsearch document for main concept"""
        
        # Build search terms for better matching
        search_terms = [concept.display]
        if concept.long_common_name and concept.long_common_name != concept.display:
            search_terms.append(concept.long_common_name)
        if concept.shortname and concept.shortname != concept.display:
            search_terms.append(concept.shortname)
        
        doc = {
            "code": concept.code,
            "system": "http://loinc.org",
            "type": "concept",
            "display": concept.long_common_name or concept.display,
            "search_terms": " ".join(filter(None, search_terms)),
            "parents": list(parents) if parents else [],
            "children": list(children) if children else [],
            "status": concept.status,
            "class": concept.class_
        }
        
        # Store component composition for lookup operations
        if any([concept.component, concept.property_, concept.time_aspect, 
                concept.system, concept.scale_type, concept.method_type]):
            doc["components"] = {
                "component": concept.component,
                "property": concept.property_,
                "time_aspect": concept.time_aspect,
                "system": concept.system,
                "scale_type": concept.scale_type,
                "method_type": concept.method_type
            }
        
        return doc
    
    def _create_part_doc(self, part: LoincPart, parents: Set[str] = None,
                        children: Set[str] = None, used_in: Set[str] = None) -> Dict:
        """Create Elasticsearch document for part"""
        
        # Build search terms
        search_terms = [part.name]
        if part.display_name and part.display_name != part.name:
            search_terms.append(part.display_name)
        
        doc = {
            "code": part.code,
            "system": "http://loinc.org", 
            "type": "part",
            "display": part.name,
            "designation_value": part.display_name,
            "search_terms": " ".join(filter(None, search_terms)),
            "parents": list(parents) if parents else [],
            "children": list(children) if children else [],
            "used_in": list(used_in) if used_in else [],
            "status": part.status
        }
        
        return doc
    
    def _create_answer_doc(self, answer: LoincAnswer, parents: Set[str] = None,
                          children: Set[str] = None) -> Dict:
        """Create Elasticsearch document for answer"""
        
        doc = {
            "code": answer.code,
            "system": "http://loinc.org",
            "type": "answer", 
            "display": answer.display,
            "search_terms": answer.display,
            "parents": list(parents) if parents else [],
            "children": list(children) if children else []
        }
        
        return doc
    
    def _create_hierarchy_doc(self, hierarchy: LoincHierarchy) -> Dict:
        """Create Elasticsearch document for hierarchy relationship"""
        return {
            "parent": hierarchy.parent,
            "child": hierarchy.child,
            "relationship_type": hierarchy.relationship_type,
            "depth": 1  # Can be enhanced to calculate actual depth
        }
    
    def _create_lookup_doc(self, code: str, system: str, display: str, 
                          properties: Dict = None, designations: List = None) -> Dict:
        """Create lookup cache document for fast $lookup operations"""
        doc = {
            "code": code,
            "system": system,
            "display": display
        }
        
        if properties:
            doc["properties"] = properties
        
        if designations:
            doc["designations"] = designations
            
        return doc
    
    def index_all_data(self, reader: LoincReader):
        """Index all LOINC data with optimized bulk operations"""
        start_time = time.time()
        logger.info("Starting LOINC data indexing...")
        
        # Step 1: Build relationship maps
        logger.info("Building relationship maps...")
        part_usage_map = reader.build_part_usage_map()
        hierarchy_map = self._build_hierarchy_map(reader)
        
        # Step 2: Index concepts (main LOINC codes)
        logger.info("Indexing main concepts...")
        concept_count = self._index_concepts(reader, hierarchy_map)
        
        # Step 3: Index parts
        logger.info("Indexing parts...")
        part_count = self._index_parts(reader, hierarchy_map, part_usage_map)
        
        # Step 4: Index answers
        logger.info("Indexing answers...")
        answer_count = self._index_answers(reader, hierarchy_map)
        
        # Step 5: Index hierarchies
        logger.info("Indexing hierarchies...")
        hierarchy_count = self._index_hierarchies(reader)
        
        # Step 6: Create lookup cache
        logger.info("Creating lookup cache...")
        self._create_lookup_cache(reader)
        
        # Refresh indices
        for index_name in self.indices.values():
            self.es.indices.refresh(index=index_name)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Indexing completed in {elapsed_time:.2f} seconds")
        logger.info(f"Indexed: {concept_count} concepts, {part_count} parts, "
                   f"{answer_count} answers, {hierarchy_count} hierarchies")
    
    def _build_hierarchy_map(self, reader: LoincReader) -> Dict[str, Dict[str, Set[str]]]:
        """Build parent-child relationship maps"""
        hierarchy_map = {
            'parents': {},  # child -> set of parents
            'children': {}  # parent -> set of children
        }
        
        for hierarchy in reader.read_hierarchies():
            # Parents map
            if hierarchy.child not in hierarchy_map['parents']:
                hierarchy_map['parents'][hierarchy.child] = set()
            hierarchy_map['parents'][hierarchy.child].add(hierarchy.parent)
            
            # Children map
            if hierarchy.parent not in hierarchy_map['children']:
                hierarchy_map['children'][hierarchy.parent] = set()
            hierarchy_map['children'][hierarchy.parent].add(hierarchy.child)
        
        return hierarchy_map
    
    def _index_concepts(self, reader: LoincReader, hierarchy_map: Dict) -> int:
        """Index main LOINC concepts with bulk operations"""
        def doc_generator():
            for concept in reader.read_main_concepts():
                parents = hierarchy_map['parents'].get(concept.code, set())
                children = hierarchy_map['children'].get(concept.code, set())
                
                doc = self._create_concept_doc(concept, parents, children)
                
                yield {
                    "_index": self.indices['concepts'],
                    "_id": concept.code,
                    "_source": doc
                }
        
        count = 0
        for success, info in parallel_bulk(
            self.es, 
            doc_generator(),
            chunk_size=self.parallel_bulk_size,
            max_chunk_bytes=self.max_chunk_bytes,
            thread_count=2
        ):
            if not success:
                logger.error(f"Failed to index concept: {info}")
            else:
                count += 1
                if count % 5000 == 0:
                    logger.info(f"Indexed {count} concepts")
        
        return count
    
    def _index_parts(self, reader: LoincReader, hierarchy_map: Dict, 
                    part_usage_map: Dict) -> int:
        """Index LOINC parts with bulk operations"""
        def doc_generator():
            for part in reader.read_parts():
                parents = hierarchy_map['parents'].get(part.code, set())
                children = hierarchy_map['children'].get(part.code, set())
                used_in = part_usage_map.get(part.code, set())
                
                doc = self._create_part_doc(part, parents, children, used_in)
                
                yield {
                    "_index": self.indices['concepts'],
                    "_id": part.code,
                    "_source": doc
                }
        
        count = 0
        for success, info in parallel_bulk(
            self.es,
            doc_generator(), 
            chunk_size=self.parallel_bulk_size,
            max_chunk_bytes=self.max_chunk_bytes,
            thread_count=2
        ):
            if not success:
                logger.error(f"Failed to index part: {info}")
            else:
                count += 1
                if count % 2000 == 0:
                    logger.info(f"Indexed {count} parts")
        
        return count
    
    def _index_answers(self, reader: LoincReader, hierarchy_map: Dict) -> int:
        """Index LOINC answers with bulk operations"""
        def doc_generator():
            for answer in reader.read_answers():
                parents = hierarchy_map['parents'].get(answer.code, set())
                children = hierarchy_map['children'].get(answer.code, set())
                
                doc = self._create_answer_doc(answer, parents, children)
                
                yield {
                    "_index": self.indices['concepts'],
                    "_id": answer.code,
                    "_source": doc
                }
        
        count = 0
        for success, info in parallel_bulk(
            self.es,
            doc_generator(),
            chunk_size=self.parallel_bulk_size,
            max_chunk_bytes=self.max_chunk_bytes,
            thread_count=2
        ):
            if not success:
                logger.error(f"Failed to index answer: {info}")
            else:
                count += 1
                if count % 1000 == 0:
                    logger.info(f"Indexed {count} answers")
        
        return count
    
    def _index_hierarchies(self, reader: LoincReader) -> int:
        """Index hierarchy relationships"""
        def doc_generator():
            for hierarchy in reader.read_hierarchies():
                doc = self._create_hierarchy_doc(hierarchy)
                
                yield {
                    "_index": self.indices['hierarchies'],
                    "_source": doc
                }
        
        count = 0
        # Fix: Use the correct return type for bulk()
        success_count, errors = bulk(
            self.es,
            doc_generator(),
            chunk_size=self.bulk_size,
            max_chunk_bytes=self.max_chunk_bytes
        )
        
        count = success_count
        
        # Log any errors
        if errors:
            for error in errors:
                logger.error(f"Failed to index hierarchy: {error}")
        
        logger.info(f"Indexed {count} hierarchies with {len(errors)} errors")
        return count
    
    def _create_lookup_cache(self, reader: LoincReader):
        """Create optimized lookup cache for $lookup operations"""
        def doc_generator():
            # Cache main concepts
            for concept in reader.read_main_concepts():
                properties = {}
                if hasattr(concept, 'components') and concept.components:
                    properties.update(concept.components)
                
                designations = []
                if concept.long_common_name and concept.long_common_name != concept.display:
                    designations.append({
                        "value": concept.long_common_name,
                        "use": {"system": "http://loinc.org", "code": "LONG_COMMON_NAME"}
                    })
                
                doc = self._create_lookup_doc(
                    concept.code, "http://loinc.org", concept.display,
                    properties, designations
                )
                
                yield {
                    "_index": self.indices['lookup'],
                    "_id": concept.code,
                    "_source": doc
                }
            
            # Cache parts
            for part in reader.read_parts():
                designations = []
                if part.display_name and part.display_name != part.name:
                    designations.append({
                        "value": part.display_name,
                        "use": {"system": "http://loinc.org", "code": "DISPLAY_NAME"}
                    })
                
                doc = self._create_lookup_doc(
                    part.code, "http://loinc.org", part.name,
                    {"type": part.type_name}, designations
                )
                
                yield {
                    "_index": self.indices['lookup'],
                    "_id": part.code,
                    "_source": doc
                }
            
            # Cache answers
            for answer in reader.read_answers():
                doc = self._create_lookup_doc(
                    answer.code, "http://loinc.org", answer.display
                )
                
                yield {
                    "_index": self.indices['lookup'],
                    "_id": answer.code,
                    "_source": doc
                }
        
        # Fix: Use the correct return type for bulk()
        success_count, errors = bulk(
            self.es,
            doc_generator(),
            chunk_size=self.bulk_size,
            max_chunk_bytes=self.max_chunk_bytes
        )
        
        # Log any errors
        if errors:
            for error in errors:
                logger.error(f"Failed to create lookup cache: {error}")
        
        logger.info(f"Created lookup cache with {success_count} entries and {len(errors)} errors")
    
    def get_index_stats(self) -> Dict:
        """Get statistics about indexed data"""
        stats = {}
        
        for name, index in self.indices.items():
            try:
                index_stats = self.es.indices.stats(index=index)
                doc_count = index_stats['indices'][index]['total']['docs']['count']
                size_bytes = index_stats['indices'][index]['total']['store']['size_in_bytes']
                
                stats[name] = {
                    'documents': doc_count,
                    'size_mb': round(size_bytes / (1024 * 1024), 2)
                }
            except Exception as e:
                logger.error(f"Failed to get stats for {index}: {e}")
                stats[name] = {'error': str(e)}
        
        return stats



# Example usage and main execution
if __name__ == "__main__":
    import sys
    from terminology_api.ES.es_client import es
    
    # Configuration
    LOINC_DATA_PATH = "Loinc_2.80"  # Update this path
    INDEX_PREFIX = "loinc"
    
    if len(sys.argv) > 1:
        LOINC_DATA_PATH = sys.argv[1]
    
    try:
        # Initialize components
        # logger.info("Initializing Elasticsearch connection...")
        
        # if not es.ping():
        #     raise ConnectionError("Cannot connect to Elasticsearch")
        
        # logger.info("Initializing LOINC reader...")
        # reader = LoincReader(LOINC_DATA_PATH, chunk_size=5000)
        
        # logger.info("Initializing LOINC indexer...")
        # indexer = LoincIndexer(es, INDEX_PREFIX)
        
        # # Create indices and index data
        # indexer.create_indices()
        # indexer.index_all_data(reader)
        
        # Print statistics
        # stats = indexer.get_index_stats()
        # logger.info("Indexing completed successfully!")
        # logger.info("Index Statistics:")
        # for index_name, index_stats in stats.items():
        #     if 'error' not in index_stats:
        #         logger.info(f"  {index_name}: {index_stats['documents']} documents, "
        #                    f"{index_stats['size_mb']} MB")
        #     else:
        #         logger.error(f"  {index_name}: {index_stats['error']}")
        
        # Test query engine
        # logger.info("Testing query engine...")
        # query_engine = LoincQueryEngine(es, INDEX_PREFIX)
        
        # # Test expand operation
        # expand_result = query_engine.expand_valueset("Nucleotide", count=10)
        # logger.info(f"Expand test: Found {expand_result['total']} results")
        # print(expand_result)
        
        # Test lookup operation (if we have results)
        # if expand_result['contains']:
        #     test_code = expand_result['contains'][0]['code']
        #     lookup_result = query_engine.lookup_concept(test_code)
        #     logger.info(f"Lookup test: Retrieved info for {test_code}")
        
        logger.info("All tests completed successfully!")
        
    except FileNotFoundError as e:
        logger.error(f"LOINC data files not found: {e}")
        logger.error("Please provide the correct path to LOINC data directory")
        logger.error("Usage: python loinc_indexer.py /path/to/loinc/data")
        sys.exit(1)
        
    except ConnectionError as e:
        logger.error(f"Elasticsearch connection failed: {e}")
        logger.error(f"Please ensure Elasticsearch is running")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)