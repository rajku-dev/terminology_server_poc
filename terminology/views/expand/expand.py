from rest_framework.decorators import api_view
from rest_framework.response import Response
from terminology_api.ES.es_client import es
from terminology_api.LOINC.query_engine import LoincQueryEngine
from datetime import datetime
import uuid
import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

# Maximum terms per Elasticsearch query (safe limit)
MAX_TERMS_PER_QUERY = 60000

@api_view(['POST'])
def expand_view(request):
    """
    FHIR ValueSet $expand operation implementation for SNOMED CT
    Works with separate concepts, descriptions, and relationships indices
    Optimized version matching File 1's efficiency with batch processing for large term sets
    """
    try:
        # Parse request parameters
        params = request.data.get('parameter', [])
        param_dict = {}
        
        for param in params:
            name = param.get('name')
            if 'valueString' in param:
                param_dict[name] = param['valueString']
            elif 'valueInteger' in param:
                param_dict[name] = param['valueInteger']
            elif 'valueBoolean' in param:
                param_dict[name] = param['valueBoolean']
            elif 'resource' in param:
                param_dict[name] = param['resource']
        
        # Extract parameters with defaults
        filter_text = param_dict.get('filter', '')
        count = min(param_dict.get('count', 10), 100)
        offset = param_dict.get('offset', 0)
        display_language = param_dict.get('displayLanguage', 'en')
        include_designations = param_dict.get('includeDesignations', False)
        value_set = param_dict.get('valueSet', {})
        
        # Normalize language code (en-gb -> en, en-us -> en)
        if display_language in ['en-gb', 'en-us']:
            display_language = 'en'
        
        print(f"Expand request - Language: {display_language}, Count: {count}, Filter: '{filter_text}'")
        
        # Validate required parameters
        if not value_set:
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "required",
                    "details": {"text": "Missing required parameter: valueSet"}
                }]
            }, status=400)
        
        # Process ValueSet compose
        compose = value_set.get('compose', {})
        includes = compose.get('include', [])
        excludes = compose.get('exclude', [])
        
        if not includes:
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "required",
                    "details": {"text": "ValueSet must include at least one include specification"}
                }]
            }, status=400)
        
        # Get all concept IDs from includes
        all_concept_ids = set()
        
        for include in includes:
            system = include.get('system')
            if system != 'http://snomed.info/sct':
                if system == 'http://loinc.org':
                    query = LoincQueryEngine(es)
                    loinc_resp = query.expand_valueset(filter_text=filter_text, count=count, offset=offset, include_designations=include_designations)
                    return Response(loinc_resp)
                continue
                
            # Handle direct concept codes
            if 'concept' in include:
                codes = include['concept']
                for code_entry in codes:
                    all_concept_ids.add(code_entry['code'])
            
            # Handle filters
            filters = include.get('filter', [])
            for filter_def in filters:
                property_name = filter_def.get('property')
                op = filter_def.get('op')
                value = filter_def.get('value')
                
                if property_name == 'concept' and op == 'is-a':
                    # Validate root concept exists
                    if not concept_exists(value):
                        logger.warning(f"Root concept {value} not found")
                        continue
                    
                    # Get descendants using optimized batch query
                    descendants = find_descendants(value)
                    all_concept_ids.update(descendants)
                    all_concept_ids.add(value)  # Include the concept itself
                    logger.info(f"Found {len(descendants)} descendants for {value}")
        
        logger.info(f"Total concept IDs before exclusions: {len(all_concept_ids)}")
        
        # Process excludes
        exclude_concept_ids = set()
        for exclude in excludes:
            system = exclude.get('system')
            if system != 'http://snomed.info/sct':
                continue
                
            # Handle direct concept codes
            if 'concept' in exclude:
                codes = exclude['concept']
                for code_entry in codes:
                    exclude_concept_ids.add(code_entry['code'])
            
            # Handle filters
            exclude_filters = exclude.get('filter', [])
            for filter_def in exclude_filters:
                property_name = filter_def.get('property')
                op = filter_def.get('op')
                value = filter_def.get('value')
                
                if property_name == 'concept' and op == 'is-a':
                    if concept_exists(value):
                        descendants = find_descendants(value)
                        exclude_concept_ids.update(descendants)
                        exclude_concept_ids.add(value)
        
        # Remove excluded concepts
        all_concept_ids -= exclude_concept_ids
        
        logger.info(f"Concept IDs after exclusions: {len(all_concept_ids)}")
        
        # Get expansion with efficient filtering and pagination
        if filter_text:
            expansion_contains, total_count = get_filtered_expansion(
                all_concept_ids, filter_text, display_language, include_designations, count, offset
            )
        else:
            expansion_contains, total_count = get_expansion(
                all_concept_ids, display_language, include_designations, count, offset
            )
        
        # Build response
        response = build_expansion_response(
            expansion_contains, total_count, offset, display_language
        )
        
        return Response(response)
        
    except Exception as e:
        logger.error(f"ValueSet expand error: {str(e)}", exc_info=True)
        return Response({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "exception",
                "details": {"text": f"Internal server error: {str(e)}"}
            }]
        }, status=500)

def concept_exists(concept_id):
    """Check if a concept exists in the concepts index"""
    try:
        result = es.get(index="concepts", id=concept_id, ignore=[404])
        return result.get('found', False)
    except Exception as e:
        logger.error(f"Error checking concept existence for {concept_id}: {str(e)}")
        return False

def find_descendants(concept_id):
    """
    Optimized descendant finding using composite aggregation approach
    Similar to File 1's get_all_concepts_in_valueset but for relationships
    """
    all_descendants = set()
    current_level = {concept_id}
    depth = 0
    max_depth = 10  # Prevent infinite loops

    try:
        while current_level and depth < max_depth:
            # Use composite aggregation for efficient pagination
            next_level = get_children_composite(list(current_level))
            
            # Filter out already processed concepts
            new_descendants = next_level - all_descendants - {concept_id}
            
            if not new_descendants:
                break
                
            all_descendants.update(new_descendants)
            current_level = new_descendants
            depth += 1
            
            logger.info(f"Depth {depth}: Found {len(new_descendants)} new descendants")

        logger.info(f"Total descendants for {concept_id}: {len(all_descendants)}")
        return all_descendants

    except Exception as e:
        logger.error(f"Error finding descendants for {concept_id}: {str(e)}", exc_info=True)
        return all_descendants

def get_children_composite(parent_concept_ids):
    """
    Get direct children using composite aggregation for efficiency
    """
    children = set()
    after_key = None
    
    while True:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"destination_id": parent_concept_ids}},
                        {"term": {"type_id": "116680003"}},  # IS-A relationship
                        {"term": {"active": True}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "unique_children": {
                    "composite": {
                        "size": 10000,
                        "sources": [
                            {
                                "source_id": {
                                    "terms": {
                                        "field": "source_id.keyword",
                                        "order": "asc"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        if after_key:
            query["aggs"]["unique_children"]["composite"]["after"] = after_key
        
        resp = es.search(
            index="relationships",
            body=query,
            timeout='30s'
        )
        
        buckets = resp.get("aggregations", {}).get("unique_children", {}).get("buckets", [])
        
        if not buckets:
            break
            
        batch_children = {bucket["key"]["source_id"] for bucket in buckets}
        children.update(batch_children)
        
        after_key = resp.get("aggregations", {}).get("unique_children", {}).get("after_key")
        if not after_key:
            break
    
    return children

def get_expansion(concept_ids, display_language, include_designations, count, offset):
    """
    Get expansion without text filtering - optimized like File 1
    """
    try:
        total_count = len(concept_ids)
        print(f"Found {total_count} unique concepts")
        
        # Convert to sorted list for consistent pagination
        concept_ids_list = sorted(list(concept_ids))
        
        # Apply pagination
        paginated_concept_ids = concept_ids_list[offset:offset + count]
        
        # Get detailed descriptions for paginated concepts
        expansion_contains = get_concepts_details_for_expansion(
            paginated_concept_ids, display_language, include_designations
        )
        
        return expansion_contains, total_count
        
    except Exception as e:
        logger.error(f"Error getting expansion: {str(e)}")
        return [], 0

def chunk_list(lst, chunk_size):
    """Split a list into chunks of specified size"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_filtered_expansion(concept_ids, filter_text, display_language, include_designations, count, offset):
    """
    Get expansion with text filtering - optimized with batch processing for large concept sets
    """
    try:
        # Normalize search text
        normalized_filter = normalize_search_text(filter_text)
        concept_ids_list = list(concept_ids)
        
        print(f"Processing {len(concept_ids_list)} concepts with filter '{filter_text}'")
        
        # If concept set is small enough, use original approach
        if len(concept_ids_list) <= MAX_TERMS_PER_QUERY:
            return get_filtered_expansion_single_query(
                concept_ids_list, normalized_filter, display_language, include_designations, count, offset
            )
        
        # For large concept sets, use batch processing approach
        return get_filtered_expansion_batched(
            concept_ids_list, normalized_filter, display_language, include_designations, count, offset
        )
        
    except Exception as e:
        logger.error(f"Error getting filtered expansion: {str(e)}")
        return [], 0

def get_filtered_expansion_single_query(concept_ids_list, normalized_filter, display_language, include_designations, count, offset):
    """
    Handle filtered expansion with a single query for smaller concept sets
    """
    # Build query with text filtering
    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"concept_id": concept_ids_list}},
                    {"term": {"active": True}},
                    {"term": {"language_code": display_language}}
                ],
                "should": [
                    # Exact phrase (highest priority)
                    {"match_phrase": {"term": {"query": normalized_filter, "boost": 10}}},
                    # Prefix match
                    {"prefix": {"term": {"value": normalized_filter, "boost": 5}}},
                ],
                "minimum_should_match": 1
            }
        },
        "_source": ["concept_id", "type_id", "term", "language_code"],
        "size": 10000,  # Get all matching descriptions
        "sort": [
            {"_score": {"order": "desc"}},
            {"term.keyword": {"order": "asc"}}
        ]
    }
    
    # Execute search
    resp = es.search(
        index="descriptions",
        body=query,
        timeout='30s'
    )
    
    return process_filtered_results(resp, normalized_filter, display_language, include_designations, count, offset)

def get_filtered_expansion_batched(concept_ids_list, normalized_filter, display_language, include_designations, count, offset):
    """
    Handle filtered expansion with batch processing for large concept sets
    """
    all_concept_descriptions = {}
    
    # Process in batches
    batch_count = 0
    total_batches = (len(concept_ids_list) + MAX_TERMS_PER_QUERY - 1) // MAX_TERMS_PER_QUERY
    
    for batch_concept_ids in chunk_list(concept_ids_list, MAX_TERMS_PER_QUERY):
        batch_count += 1
        print(f"Processing batch {batch_count}/{total_batches} with {len(batch_concept_ids)} concepts")
        
        # Build query for this batch
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"concept_id": batch_concept_ids}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}}
                    ],
                    "should": [
                        # Exact phrase (highest priority)
                        {"match_phrase": {"term": {"query": normalized_filter, "boost": 10}}},
                        # Prefix match
                        {"prefix": {"term": {"value": normalized_filter, "boost": 5}}},
                    ],
                    "minimum_should_match": 1
                }
            },
            "_source": ["concept_id", "type_id", "term", "language_code"],
            "size": 10000,  # Get all matching descriptions
            "sort": [
                {"_score": {"order": "desc"}},
                {"term.keyword": {"order": "asc"}}
            ]
        }
        
        # Execute search for this batch
        try:
            resp = es.search(
                index="descriptions",
                body=query,
                timeout='30s'
            )
            
            # Process results and merge with overall results
            for hit in resp["hits"]["hits"]:
                source = hit["_source"]
                concept_id = source["concept_id"]
                score = hit["_score"]
                
                # Calculate additional scoring
                additional_score = calculate_additional_score(
                    source["term"], normalized_filter, source["type_id"]
                )
                final_score = score + additional_score
                
                # Group by concept and keep best scoring description
                if concept_id not in all_concept_descriptions:
                    all_concept_descriptions[concept_id] = []
                
                all_concept_descriptions[concept_id].append({
                    "description": source,
                    "score": final_score
                })
                
        except Exception as e:
            logger.error(f"Error processing batch {batch_count}: {str(e)}")
            continue
    
    # Sort concepts by their best description score
    concept_scores = {}
    for concept_id, descriptions in all_concept_descriptions.items():
        best_desc = max(descriptions, key=lambda x: x["score"])
        concept_scores[concept_id] = {
            "concept_id": concept_id,
            "score": best_desc["score"],
            "best_term": best_desc["description"]["term"]
        }
    
    # Sort by score then alphabetically
    sorted_concepts = sorted(
        concept_scores.values(),
        key=lambda x: (-x["score"], x["best_term"].lower())
    )
    
    total_count = len(sorted_concepts)
    
    # Apply pagination
    paginated_concepts = sorted_concepts[offset:offset + count]
    concept_ids_for_details = [c["concept_id"] for c in paginated_concepts]
    
    # Get detailed descriptions for paginated concepts
    expansion_contains = get_concepts_details_for_expansion(
        concept_ids_for_details, display_language, include_designations
    )
    
    print(f"Found {total_count} matching concepts for filter '{normalized_filter}' across {batch_count} batches")
    return expansion_contains, total_count

def process_filtered_results(resp, normalized_filter, display_language, include_designations, count, offset):
    """
    Process filtered search results and return paginated expansion
    """
    # Process results and group by concept
    concept_descriptions = {}
    for hit in resp["hits"]["hits"]:
        source = hit["_source"]
        concept_id = source["concept_id"]
        score = hit["_score"]
        
        # Calculate additional scoring
        additional_score = calculate_additional_score(
            source["term"], normalized_filter, source["type_id"]
        )
        final_score = score + additional_score
        
        # Group by concept and keep best scoring description
        if concept_id not in concept_descriptions:
            concept_descriptions[concept_id] = []
        
        concept_descriptions[concept_id].append({
            "description": source,
            "score": final_score
        })
    
    # Sort concepts by their best description score
    concept_scores = {}
    for concept_id, descriptions in concept_descriptions.items():
        best_desc = max(descriptions, key=lambda x: x["score"])
        concept_scores[concept_id] = {
            "concept_id": concept_id,
            "score": best_desc["score"],
            "best_term": best_desc["description"]["term"]
        }
    
    # Sort by score then alphabetically
    sorted_concepts = sorted(
        concept_scores.values(),
        key=lambda x: (-x["score"], x["best_term"].lower())
    )
    
    total_count = len(sorted_concepts)
    
    # Apply pagination
    paginated_concepts = sorted_concepts[offset:offset + count]
    concept_ids_for_details = [c["concept_id"] for c in paginated_concepts]
    
    # Get detailed descriptions for paginated concepts
    expansion_contains = get_concepts_details_for_expansion(
        concept_ids_for_details, display_language, include_designations
    )
    
    return expansion_contains, total_count

def get_concepts_details_for_expansion(concept_ids, display_language, include_designations):
    """
    Get detailed concept information with batch processing for large concept sets
    """
    if not concept_ids:
        return []
    
    try:
        # If concept set is small, use single query
        if len(concept_ids) <= MAX_TERMS_PER_QUERY:
            return get_concepts_details_single_query(concept_ids, display_language, include_designations)
        
        # For large sets, use batch processing
        return get_concepts_details_batched(concept_ids, display_language, include_designations)
        
    except Exception as e:
        logger.error(f"Error getting concept details: {str(e)}")
        return []

def get_concepts_details_single_query(concept_ids, display_language, include_designations):
    """
    Get concept details with single query for smaller sets
    """
    # Query descriptions for the specific concepts
    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"concept_id": concept_ids}},
                    {"term": {"active": True}},
                    {"term": {"language_code": display_language}}
                ]
            }
        },
        "_source": ["concept_id", "type_id", "term", "language_code"],
        "size": len(concept_ids) * 20,  # Allow multiple descriptions per concept
    }
    
    resp = es.search(
        index="descriptions",
        body=query,
        timeout='30s'
    )
    
    # Group descriptions by concept
    descriptions_by_concept = {}
    for hit in resp["hits"]["hits"]:
        source = hit["_source"]
        concept_id = source["concept_id"]
        
        if concept_id not in descriptions_by_concept:
            descriptions_by_concept[concept_id] = []
        
        descriptions_by_concept[concept_id].append(source)
    
    # Get preferred terms from language refsets
    preferred_terms = get_preferred_terms(concept_ids, display_language)
    
    # Build concept entries
    expansion_contains = []
    for concept_id in concept_ids:
        descriptions = descriptions_by_concept.get(concept_id, [])
        preferred_term = preferred_terms.get(concept_id)
        
        concept_entry = build_concept_entry_from_descriptions(
            concept_id, descriptions, include_designations, preferred_term
        )
        
        if concept_entry:
            expansion_contains.append(concept_entry)
    
    return expansion_contains

def get_concepts_details_batched(concept_ids, display_language, include_designations):
    """
    Get concept details with batch processing for large sets
    """
    all_descriptions_by_concept = {}
    
    # Process in batches
    batch_count = 0
    total_batches = (len(concept_ids) + MAX_TERMS_PER_QUERY - 1) // MAX_TERMS_PER_QUERY
    
    for batch_concept_ids in chunk_list(concept_ids, MAX_TERMS_PER_QUERY):
        batch_count += 1
        print(f"Getting details for batch {batch_count}/{total_batches} with {len(batch_concept_ids)} concepts")
        
        # Query descriptions for this batch
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"concept_id": batch_concept_ids}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}}
                    ]
                }
            },
            "_source": ["concept_id", "type_id", "term", "language_code"],
            "size": len(batch_concept_ids) * 20,  # Allow multiple descriptions per concept
        }
        
        try:
            resp = es.search(
                index="descriptions",
                body=query,
                timeout='30s'
            )
            
            # Group descriptions by concept
            for hit in resp["hits"]["hits"]:
                source = hit["_source"]
                concept_id = source["concept_id"]
                
                if concept_id not in all_descriptions_by_concept:
                    all_descriptions_by_concept[concept_id] = []
                
                all_descriptions_by_concept[concept_id].append(source)
                
        except Exception as e:
            logger.error(f"Error getting descriptions for batch {batch_count}: {str(e)}")
            continue
    
    # Get preferred terms from language refsets (this also needs batching)
    preferred_terms = get_preferred_terms_batched(concept_ids, display_language)
    
    # Build concept entries
    expansion_contains = []
    for concept_id in concept_ids:
        descriptions = all_descriptions_by_concept.get(concept_id, [])
        preferred_term = preferred_terms.get(concept_id)
        
        concept_entry = build_concept_entry_from_descriptions(
            concept_id, descriptions, include_designations, preferred_term
        )
        
        if concept_entry:
            expansion_contains.append(concept_entry)
    
    print(f"Built {len(expansion_contains)} concept entries from {batch_count} batches")
    return expansion_contains

def get_preferred_terms(concept_ids, display_language):
    """
    Get preferred terms with batching if needed
    """
    if len(concept_ids) <= MAX_TERMS_PER_QUERY:
        return get_preferred_terms_single_query(concept_ids, display_language)
    else:
        return get_preferred_terms_batched(concept_ids, display_language)

def get_preferred_terms_single_query(concept_ids, display_language):
    """
    Optimized preferred terms lookup for smaller sets
    """
    if not concept_ids:
        return {}
    
    try:
        # Map language codes to refset IDs
        refset_map = {
            'en': '900000000000509007',  # US English
            'en-us': '900000000000509007',  # US English
            'en-gb': '900000000000508004',  # GB English
        }
        
        refset_id = refset_map.get(display_language, '900000000000509007')
        
        # First get all description IDs for the concepts
        desc_query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"concept_id": concept_ids}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}},
                        {"terms": {"type_id": ["900000000000013009", "900000000000003001"]}}
                    ]
                }
            },
            "_source": ["concept_id", "type_id", "term"],
            "size": len(concept_ids) * 10
        }
        
        desc_resp = es.search(
            index="descriptions",
            body=desc_query,
            timeout='30s'
        )
        
        # Build mapping
        desc_to_concept = {}
        description_ids = []
        for hit in desc_resp['hits']['hits']:
            desc_id = hit['_id']
            source = hit['_source']
            desc_to_concept[desc_id] = source
            description_ids.append(desc_id)
        
        if not description_ids:
            return {}
        
        # Get preferred terms using batch query
        preferred_query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"referenced_component_id": description_ids}},
                        {"term": {"refset_id": refset_id}},
                        {"term": {"active": True}},
                        {"term": {"acceptability_id": "900000000000548007"}}  # Preferred
                    ]
                }
            },
            "_source": ["referenced_component_id"],
            "size": len(description_ids)
        }
        
        pref_resp = es.search(
            index="language_refsets",
            body=preferred_query,
            timeout='30s'
        )
        
        # Build preferred terms mapping - prioritize synonyms
        preferred_terms = {}
        preferred_synonyms = {}
        preferred_fsns = {}
        
        for hit in pref_resp['hits']['hits']:
            desc_id = hit['_source']['referenced_component_id']
            
            if desc_id in desc_to_concept:
                concept_info = desc_to_concept[desc_id]
                concept_id = concept_info['concept_id']
                term = concept_info['term']
                type_id = concept_info['type_id']
                
                if type_id == "900000000000013009":  # Synonym
                    if concept_id not in preferred_synonyms:
                        preferred_synonyms[concept_id] = term
                elif type_id == "900000000000003001":  # FSN
                    if concept_id not in preferred_fsns:
                        preferred_fsns[concept_id] = term
        
        # Build final mapping - synonyms first
        for concept_id in concept_ids:
            if concept_id in preferred_synonyms:
                preferred_terms[concept_id] = preferred_synonyms[concept_id]
            elif concept_id in preferred_fsns:
                preferred_terms[concept_id] = preferred_fsns[concept_id]
        
        return preferred_terms
        
    except Exception as e:
        logger.error(f"Error getting preferred terms: {str(e)}")
        return {}

def get_preferred_terms_batched(concept_ids, display_language):
    """
    Get preferred terms with batch processing for large sets
    """
    all_preferred_terms = {}
    
    # Process in batches
    batch_count = 0
    total_batches = (len(concept_ids) + MAX_TERMS_PER_QUERY - 1) // MAX_TERMS_PER_QUERY
    
    for batch_concept_ids in chunk_list(concept_ids, MAX_TERMS_PER_QUERY):
        batch_count += 1
        print(f"Getting preferred terms for batch {batch_count}/{total_batches}")
        
        try:
            batch_preferred = get_preferred_terms_single_query(batch_concept_ids, display_language)
            all_preferred_terms.update(batch_preferred)
        except Exception as e:
            logger.error(f"Error getting preferred terms for batch {batch_count}: {str(e)}")
            continue
    
    logger.info(f"Found {len(all_preferred_terms)} preferred terms across {batch_count} batches")
    return all_preferred_terms

def build_concept_entry_from_descriptions(concept_id, descriptions, include_designations, preferred_term=None):
    """
    Build individual concept entry from descriptions - same as File 1 logic
    """
    if not descriptions:
        return {
            "system": "http://snomed.info/sct",
            "code": concept_id,
            "display": preferred_term or concept_id
        }
    
    # Use preferred term if available, otherwise find best description
    display_term = preferred_term
    
    if not display_term:
        # Sort to prioritize synonyms over FSNs
        sorted_descriptions = sorted(descriptions, key=lambda x: (
            0 if x["type_id"] == "900000000000013009" else 1,  # Synonym first
            x["term"]
        ))
        
        if sorted_descriptions:
            display_term = sorted_descriptions[0]["term"]
    
    # Build concept entry
    concept_entry = {
        "system": "http://snomed.info/sct",
        "code": concept_id,
        "display": display_term or concept_id
    }
    
    # Add designations if requested
    if include_designations:
        designations = []
        
        # Add display designation first
        display_designation = {
            "language": "en",
            "use": {
                "system": "http://terminology.hl7.org/CodeSystem/designation-usage",
                "code": "display"
            },
            "value": display_term or concept_id
        }
        designations.append(display_designation)
        
        # Add other designations from descriptions
        for desc in descriptions:
            use_system = "http://snomed.info/sct"
            use_code = desc["type_id"]
            use_display = "Synonym"
            
            if desc["type_id"] == "900000000000003001":  # FSN
                use_display = "Fully specified name"
            elif desc["type_id"] == "900000000000013009":  # Synonym
                use_display = "Synonym"
            
            designation = {
                "language": desc.get("language_code", "en"),
                "use": {
                    "system": use_system,
                    "code": use_code,
                    "display": use_display
                },
                "value": desc["term"]
            }
            designations.append(designation)
        
        concept_entry["designation"] = designations
    
    return concept_entry

def normalize_search_text(text):
    """
    Normalize search text similar to Snowstorm's approach - same as File 1
    """
    # Convert to lowercase
    text = text.lower()
    
    # Remove diacritics
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    return text

def calculate_additional_score(term, filter_text, type_id):
    """
    Calculate additional scoring factors similar to Snowstorm - same as File 1
    """
    additional_score = 0
    term_lower = term.lower()
    filter_lower = filter_text.lower()
    
    # Exact match bonus
    if term_lower == filter_lower:
        additional_score += 50
    
    # Starts with bonus
    elif term_lower.startswith(filter_lower):
        additional_score += 30
    
    # Word boundary match bonus
    elif f" {filter_lower}" in term_lower or term_lower.startswith(filter_lower):
        additional_score += 20
    
    # Prefer synonyms over FSNs
    if type_id == "900000000000013009":  # Synonym
        additional_score += 10
    elif type_id == "900000000000003001":  # FSN
        additional_score += 5
    
    # Length penalty for very long terms
    if len(term) > 100:
        additional_score -= 5
    
    return additional_score

def build_expansion_response(expansion_contains, total_count, offset, display_language):
    """
    Build the final expansion response - same as File 1
    """
    expansion_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00')
    
    response = {
        "resourceType": "ValueSet",
        "id": str(uuid.uuid4()),
        "copyright": "This value set includes content from SNOMED CT, which is copyright © 2002+ International Health Terminology Standards Development Organisation (SNOMED International), and distributed by agreement between SNOMED International and HL7. Implementer use of SNOMED CT is not covered by this agreement.",
        "expansion": {
            "id": expansion_id,
            "timestamp": timestamp,
            "total": total_count,
            "offset": offset,
            "parameter": [
                {
                    "name": "version",
                    "valueUri": "http://snomed.info/sct|http://snomed.info/sct/900000000000207008/version/20240731"
                },
                {
                    "name": "displayLanguage",
                    "valueString": display_language
                }
            ],
            "contains": expansion_contains
        }
    }
    
    return response