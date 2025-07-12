from rest_framework.decorators import api_view
from rest_framework.response import Response
from terminology_api.es_client import es
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)

@api_view(['POST'])
def expand_view(request):
    """
    FHIR ValueSet $expand operation implementation for SNOMED CT
    Works with separate concepts, descriptions, and relationships indices
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
        count = min(param_dict.get('count', 100), 1000)
        offset = param_dict.get('offset', 0)
        display_language = param_dict.get('displayLanguage', 'en')
        include_designations = param_dict.get('includeDesignations', False)
        value_set = param_dict.get('valueSet', {})
        
        # Normalize language code (en-gb -> en, en-us -> en)
        if display_language in ['en-gb', 'en-us']:
            display_language = 'en'
        
        logger.info(f"Expand request - Language: {display_language}, Count: {count}, Filter: '{filter_text}'")
        
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
                    
                    # Get descendants using batch query
                    descendants = find_descendants_batch(value)
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
                        descendants = find_descendants_batch(value)
                        exclude_concept_ids.update(descendants)
                        exclude_concept_ids.add(value)
        
        # Remove excluded concepts
        all_concept_ids -= exclude_concept_ids
        
        logger.info(f"Concept IDs after exclusions: {len(all_concept_ids)}")
        
        # OPTIMIZATION: Early termination for text filtering
        if filter_text:
            logger.info(f"Applying text filter: '{filter_text}'")
            # Pass count and offset for early termination
            expansion_contains, total_count = filter_and_paginate_concepts(
                list(all_concept_ids), filter_text, display_language, 
                include_designations, count, offset
            )
        else:
            # No text filter - apply pagination directly
            total_count = len(all_concept_ids)
            concept_ids_list = sorted(list(all_concept_ids))
            paginated_concepts = concept_ids_list[offset:offset + count]
            
            expansion_contains = get_concepts_details_batch(
                paginated_concepts, display_language, include_designations
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

def find_descendants_batch(concept_id, max_depth=15):
    """Find all descendants using batch queries with proper depth handling"""
    all_descendants = set()
    current_level = {concept_id}
    
    for depth in range(max_depth):
        if not current_level:
            break
            
        try:
            # Use scroll API for large result sets
            resp = es.search(
                index="relationships",
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"terms": {"destination_id": list(current_level)}},
                                {"term": {"type_id": "116680003"}},  # IS-A relationship
                                {"term": {"active": True}}
                            ]
                        }
                    },
                    "_source": ["source_id"],
                    "size": 10000
                },
                timeout='30s'
            )
            
            next_level = set()
            for hit in resp["hits"]["hits"]:
                child_id = hit["_source"]["source_id"]
                if child_id not in all_descendants and child_id != concept_id:
                    all_descendants.add(child_id)
                    next_level.add(child_id)
            
            current_level = next_level
            logger.info(f"Depth {depth}: found {len(next_level)} new descendants")
            
            # Break if we're not finding new descendants
            if not next_level:
                break
                
        except Exception as e:
            logger.error(f"Error finding descendants at depth {depth} for {concept_id}: {str(e)}")
            break
    
    logger.info(f"Total descendants for {concept_id}: {len(all_descendants)}")
    return all_descendants

def filter_and_paginate_concepts(concept_ids, filter_text, display_language, 
                                include_designations, count, offset):
    """
    Filter concepts by text using Snowstorm-style matching logic
    Returns only the concepts needed for the current page
    """
    if not filter_text or not concept_ids:
        return [], 0
    
    try:
        # Normalize search text (similar to Snowstorm)
        normalized_filter = normalize_search_text(filter_text)
        
        batch_size = 1000
        matching_concepts = []
        total_matching_count = 0
        
        # Track pagination
        concepts_to_skip = offset
        concepts_to_collect = count
        
        for i in range(0, len(concept_ids), batch_size):
            batch = concept_ids[i:i + batch_size]
            
            # Build Snowstorm-style query with multiple matching strategies
            query = build_snowstorm_text_query(batch, normalized_filter, display_language)
            
            resp = es.search(
                index="descriptions",
                body=query,
                timeout='30s'
            )
            
            # Process results with Snowstorm-style scoring
            batch_concepts = process_search_results(resp, normalized_filter)
            
            # Apply pagination logic
            for concept_data in batch_concepts:
                total_matching_count += 1
                
                if concepts_to_skip > 0:
                    concepts_to_skip -= 1
                    continue
                
                if concepts_to_collect > 0:
                    # Get full concept details
                    concept_details = get_concepts_details_batch(
                        [concept_data['concept_id']], display_language, include_designations
                    )
                    if concept_details:
                        matching_concepts.extend(concept_details)
                        concepts_to_collect -= 1
                
                # Early termination
                if concepts_to_collect <= 0:
                    break
            
            # Early termination at batch level
            if concepts_to_collect <= 0:
                break
        
        # Get accurate total count if needed
        if concepts_to_collect > 0:
            total_matching_count = get_total_matching_count(
                concept_ids, normalized_filter, display_language
            )
        
        logger.info(f"Found {len(matching_concepts)} concepts for page, total matching: {total_matching_count}")
        return matching_concepts, total_matching_count
        
    except Exception as e:
        logger.error(f"Error filtering concepts by text: {str(e)}")
        return [], 0

def normalize_search_text(text):
    """
    Normalize search text similar to Snowstorm's approach
    """
    import re
    import unicodedata
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove diacritics (Snowstorm feature)
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    return text

# def build_snowstorm_text_query(concept_ids, filter_text, language):
#     """
#     Build Elasticsearch query that mimics Snowstorm's text matching strategy
#     """
#     # Snowstorm uses multiple query strategies with different boost values
#     query = {
#         "query": {
#             "bool": {
#                 "must": [
#                     {"terms": {"concept_id": concept_ids}},
#                     {"term": {"active": True}},
#                     {"term": {"language_code": language}}
#                     {"term": {"type_id": "900000000000013009"}}
#                 ],
#                 "should": [
#                     # 1. Exact phrase match (highest boost) - Snowstorm prioritizes this
#                     {
#                         "match_phrase": {
#                             "term": {
#                                 "query": filter_text,
#                                 "boost": 100
#                             }
#                         }
#                     },
#                     # 2. Exact phrase match on normalized field (if available)
#                     {
#                         "match_phrase": {
#                             "term.folded": {
#                                 "query": filter_text,
#                                 "boost": 95
#                             }
#                         }
#                     },
#                     # 3. Prefix match on exact term (for autocomplete-like behavior)
#                     {
#                         "prefix": {
#                             "term.keyword": {
#                                 "value": filter_text,
#                                 "boost": 80
#                             }
#                         }
#                     },
#                     # 4. All terms must match (AND behavior)
#                     {
#                         "match": {
#                             "term": {
#                                 "query": filter_text,
#                                 "operator": "and",
#                                 "boost": 50
#                             }
#                         }
#                     },
#                     # 5. All terms must match on folded field
#                     {
#                         "match": {
#                             "term.folded": {
#                                 "query": filter_text,
#                                 "operator": "and",
#                                 "boost": 45
#                             }
#                         }
#                     },
#                     # 6. Wildcard matching for partial matches
#                     {
#                         "wildcard": {
#                             "term.keyword": {
#                                 "value": f"*{filter_text}*",
#                                 "boost": 20
#                             }
#                         }
#                     },
#                     # 7. Default match (OR behavior) - lowest priority
#                     {
#                         "match": {
#                             "term": {
#                                 "query": filter_text,
#                                 "boost": 10
#                             }
#                         }
#                     }
#                 ],
#                 "minimum_should_match": 1
#             }
#         },
#         "_source": ["concept_id", "term", "type_id", "language_code"],
#         "size": 10000,
#         # Snowstorm-style sorting: score first, then alphabetical
#         "sort": [
#             {"_score": {"order": "desc"}},
#             {"term.keyword": {"order": "asc"}}
#         ]
#     }
    
#     return query

def build_snowstorm_text_query(concept_ids, filter_text, language):
    return {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"concept_id": concept_ids}},
                    {"term": {"active": True}},
                    {"term": {"language_code": language}},
                    {"term": {"type_id": "900000000000013009"}}  # Synonyms only
                ],
                "should": [
                    # Exact phrase (highest priority)
                    {"match_phrase": {"term": {"query": filter_text, "boost": 10}}},
                    # Prefix match
                    {"prefix": {"term": {"value": filter_text, "boost": 5}}},
                    # All terms must match
                    {"match": {"term": {"query": filter_text, "operator": "and", "boost": 3}}},
                    # Any term can match
                    {"match": {"term": {"query": filter_text, "boost": 1}}}
                ],
                "minimum_should_match": 1
            }
        }
    }
    

def process_search_results(resp, filter_text):
    """
    Process search results similar to Snowstorm's approach
    """
    # Group by concept_id and keep the best matching description
    concept_scores = {}
    
    for hit in resp["hits"]["hits"]:
        concept_id = hit["_source"]["concept_id"]
        term = hit["_source"]["term"]
        score = hit["_score"]
        type_id = hit["_source"]["type_id"]
        
        # Calculate additional scoring factors (Snowstorm-style)
        additional_score = calculate_additional_score(term, filter_text, type_id)
        final_score = score + additional_score
        
        # Keep the best scoring description for each concept
        if concept_id not in concept_scores or final_score > concept_scores[concept_id]['score']:
            concept_scores[concept_id] = {
                'concept_id': concept_id,
                'score': final_score,
                'term': term,
                'type_id': type_id
            }
    
    # Sort by final score (descending) then alphabetically
    sorted_concepts = sorted(
        concept_scores.values(),
        key=lambda x: (-x['score'], x['term'].lower())
    )
    
    return sorted_concepts

def calculate_additional_score(term, filter_text, type_id):
    """
    Calculate additional scoring factors similar to Snowstorm
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
    
    # Prefer synonyms over FSNs (Snowstorm behavior)
    if type_id == "900000000000013009":  # Synonym
        additional_score += 10
    elif type_id == "900000000000003001":  # FSN
        additional_score += 5
    
    # Length penalty for very long terms (prefer concise matches)
    if len(term) > 100:
        additional_score -= 5
    
    return additional_score

def get_total_matching_count(concept_ids, filter_text, display_language):
    """
    Get total count using the same query logic as filtering
    """
    try:
        query = build_snowstorm_text_query(concept_ids, filter_text, display_language)
        
        # Modify for count only
        query["size"] = 0
        query["aggs"] = {
            "unique_concepts": {
                "cardinality": {
                    "field": "concept_id"
                }
            }
        }
        
        resp = es.search(
            index="descriptions",
            body=query,
            timeout='30s'
        )
        
        return resp["aggregations"]["unique_concepts"]["value"]
        
    except Exception as e:
        logger.error(f"Error getting total matching count: {str(e)}")
        return 0


def get_concepts_details_batch(concept_ids, display_language, include_designations):
    """Get concept details using batch queries with fallback language handling"""
    if not concept_ids:
        return []
    
    try:
        # Get descriptions for all concepts
        descriptions_query = {
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
            "size": len(concept_ids) * 20  # Allow more descriptions per concept
        }
        
        descriptions_result = es.search(
            index="descriptions",
            body=descriptions_query,
            timeout='30s'
        )
        
        # Group descriptions by concept
        descriptions_by_concept = {}
        for hit in descriptions_result['hits']['hits']:
            concept_id = hit['_source']['concept_id']
            if concept_id not in descriptions_by_concept:
                descriptions_by_concept[concept_id] = []
            descriptions_by_concept[concept_id].append(hit['_source'])
        
        # If no descriptions found with specified language, try fallback
        if not descriptions_by_concept and display_language != 'en':
            logger.info(f"No descriptions found for language '{display_language}', trying 'en'")
            descriptions_query['query']['bool']['must'][-1] = {"term": {"language_code": "en"}}
            descriptions_result = es.search(
                index="descriptions",
                body=descriptions_query,
                timeout='30s'
            )
            
            for hit in descriptions_result['hits']['hits']:
                concept_id = hit['_source']['concept_id']
                if concept_id not in descriptions_by_concept:
                    descriptions_by_concept[concept_id] = []
                descriptions_by_concept[concept_id].append(hit['_source'])
        
        # Build concept entries
        expansion_contains = []
        for concept_id in concept_ids:
            descriptions = descriptions_by_concept.get(concept_id, [])
            
            concept_entry = build_concept_entry(
                concept_id, descriptions, include_designations
            )
            
            if concept_entry:
                expansion_contains.append(concept_entry)
        
        logger.info(f"Built {len(expansion_contains)} concept entries")
        return expansion_contains
        
    except Exception as e:
        logger.error(f"Error getting concept details: {str(e)}")
        return []

def build_concept_entry(concept_id, descriptions, include_designations):
    """Build individual concept entry for expansion"""
    if not descriptions:
        # If no descriptions, still return basic entry
        return {
            "system": "http://snomed.info/sct",
            "code": concept_id,
            "display": concept_id  # Use concept ID as fallback display
        }
    
    # Find display term (prefer synonym over FSN)
    display_term = ""
    designations = []
    
    # Sort descriptions to prioritize synonyms
    sorted_descriptions = sorted(descriptions, key=lambda x: (
        0 if x["type_id"] == "900000000000013009" else 1,  # Synonym first
        x["term"]
    ))
    
    for desc in sorted_descriptions:
        # Set display term (first synonym or FSN if no synonym)
        if not display_term:
            display_term = desc["term"]
        
        # Build designations if requested
        if include_designations:
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
    
    # Build concept entry
    concept_entry = {
        "system": "http://snomed.info/sct",
        "code": concept_id,
        "display": display_term or concept_id
    }
    
    if include_designations and designations:
        # Add display designation (like Snowstorm does)
        display_designation = {
            "language": "en",
            "use": {
                "system": "http://terminology.hl7.org/CodeSystem/designation-usage",
                "code": "display"
            },
            "value": display_term or concept_id
        }
        designations.insert(0, display_designation)  # Add at beginning
        concept_entry["designation"] = designations
    
    return concept_entry

def build_expansion_response(expansion_contains, total_count, offset, display_language):
    """Build the final expansion response"""
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