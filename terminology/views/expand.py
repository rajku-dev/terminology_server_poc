from rest_framework.decorators import api_view
from rest_framework.response import Response
from terminology_api.es_client import es
from datetime import datetime
# from elasticsearch.exceptions import ElasticsearchException
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
        # print("Received expand request:", request.data)
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

def find_descendants_batch(concept_id, max_depth=None):
    """Find all descendants using optimized scroll queries per depth level"""
    all_descendants = set()
    current_level = {concept_id}
    depth = 0

    try:
        while current_level and (max_depth is None or depth < max_depth):
            # Initialize scroll for the current level
            query = {
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
                "size": 5000  # Increased batch size
            }

            # Start scroll
            resp = es.search(
                index="relationships",
                body=query,
                scroll="5m",  # Increased scroll context lifetime
                timeout="60s"  # Increased timeout
            )

            scroll_id = resp.get("_scroll_id")
            next_level = set()
            total_hits = resp.get("hits", {}).get("total", {}).get("value", 0)
            logger.info(f"Depth {depth}: Query returned {total_hits} total hits")

            # Process initial results
            for hit in resp["hits"]["hits"]:
                child_id = hit["_source"]["source_id"]
                if child_id not in all_descendants and child_id != concept_id:
                    all_descendants.add(child_id)
                    next_level.add(child_id)

            # Continue scrolling until no more results
            scroll_count = len(resp["hits"]["hits"])
            while resp["hits"]["hits"]:
                resp = es.scroll(scroll_id=scroll_id, scroll="5m")
                scroll_count += len(resp["hits"]["hits"])
                for hit in resp["hits"]["hits"]:
                    child_id = hit["_source"]["source_id"]
                    if child_id not in all_descendants and child_id != concept_id:
                        all_descendants.add(child_id)
                        next_level.add(child_id)

            logger.info(f"Depth {depth}: Processed {scroll_count} relationships, found {len(next_level)} new descendants")

            # Clear scroll context
            if scroll_id:
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception as e:
                    logger.warning(f"Error clearing scroll for concept {concept_id}: {str(e)}")

            current_level = next_level
            depth += 1

            if not next_level:
                break

        logger.info(f"Total descendants for {concept_id}: {len(all_descendants)}")
        return all_descendants

    except Exception as e:
        logger.error(f"Error finding descendants for {concept_id}: {str(e)}", exc_info=True)
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
                    "field": "concept_id.keyword"
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
        
        # Get preferred terms from language refsets
        preferred_terms = get_preferred_terms_batch(concept_ids, display_language)
        
        # Build concept entries
        expansion_contains = []
        for concept_id in concept_ids:
            descriptions = descriptions_by_concept.get(concept_id, [])
            preferred_term = preferred_terms.get(concept_id)
            
            concept_entry = build_concept_entry(
                concept_id, descriptions, include_designations, preferred_term
            )
            
            if concept_entry:
                expansion_contains.append(concept_entry)
        
        logger.info(f"Built {len(expansion_contains)} concept entries")
        return expansion_contains
        
    except Exception as e:
        logger.error(f"Error getting concept details: {str(e)}")
        return []

def get_preferred_terms_batch(concept_ids, display_language):
    """Get preferred terms from language_refsets index - CORRECTED VERSION"""
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
        
        # STEP 1: Get all descriptions for the concepts first
        descriptions_query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"concept_id": concept_ids}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}},
                        {"terms": {"type_id": ["900000000000013009", "900000000000003001"]}}  # Only synonyms and FSNs
                    ]
                }
            },
            "_source": ["concept_id", "type_id", "term"],
            "size": len(concept_ids) * 10
        }
        
        descriptions_resp = es.search(
            index="descriptions",
            body=descriptions_query,
            timeout='30s'
        )
        
        # Extract description IDs
        description_ids = [hit['_id'] for hit in descriptions_resp['hits']['hits']]
        desc_to_concept = {hit['_id']: hit['_source'] for hit in descriptions_resp['hits']['hits']}
        
        if not description_ids:
            logger.info(f"No descriptions found for concepts in language {display_language}")
            return {}
        
        # STEP 2: Check which descriptions are preferred in language_refsets
        language_refsets_query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"referenced_component_id": description_ids}},  # Now using description IDs
                        {"term": {"refset_id": refset_id}},
                        {"term": {"active": True}},
                        {"term": {"acceptability_id": "900000000000548007"}}  # Preferred
                    ]
                }
            },
            "_source": ["referenced_component_id"],
            "size": len(description_ids)
        }
        
        refsets_resp = es.search(
            index="language_refsets",
            body=language_refsets_query,
            timeout='30s'
        )
        
        # STEP 3: Build preferred terms mapping - PRIORITIZE SYNONYMS
        preferred_terms = {}
        preferred_synonyms = {}  # Track synonyms separately
        preferred_fsns = {}      # Track FSNs separately
        
        for hit in refsets_resp['hits']['hits']:
            desc_id = hit['_source']['referenced_component_id']
            
            if desc_id in desc_to_concept:
                concept_info = desc_to_concept[desc_id]
                concept_id = concept_info['concept_id']
                term = concept_info['term']
                type_id = concept_info['type_id']
                
                # Separate synonyms and FSNs
                if type_id == "900000000000013009":  # Synonym
                    if concept_id not in preferred_synonyms:
                        preferred_synonyms[concept_id] = term
                elif type_id == "900000000000003001":  # FSN
                    if concept_id not in preferred_fsns:
                        preferred_fsns[concept_id] = term
        
        # Build final preferred terms - prioritize synonyms over FSNs
        for concept_id in concept_ids:
            if concept_id in preferred_synonyms:
                preferred_terms[concept_id] = preferred_synonyms[concept_id]
            elif concept_id in preferred_fsns:
                preferred_terms[concept_id] = preferred_fsns[concept_id]
        
        logger.info(f"Found {len(preferred_terms)} preferred terms from language_refsets")
        return preferred_terms
        
    except Exception as e:
        logger.error(f"Error getting preferred terms from language_refsets: {str(e)}")
        return {}



def build_concept_entry(concept_id, descriptions, include_designations, preferred_term=None):
    """Build individual concept entry for expansion"""
    if not descriptions:
        # If no descriptions, still return basic entry
        return {
            "system": "http://snomed.info/sct",
            "code": concept_id,
            "display": preferred_term or concept_id  # Use preferred term if available
        }
    
    # Find display term - use preferred term from language_refsets if available
    display_term = preferred_term or ""
    designations = []
    
    # If no preferred term found, fall back to original logic
    if not display_term:
        # Sort descriptions to prioritize synonyms
        sorted_descriptions = sorted(descriptions, key=lambda x: (
            0 if x["type_id"] == "900000000000013009" else 1,  # Synonym first
            x["term"]
        ))
        
        for desc in sorted_descriptions:
            # Set display term (first synonym or FSN if no synonym)
            if not display_term:
                display_term = desc["term"]
                break
    
    # Build designations if requested
    if include_designations:
        # Add display designation first (like Snowstorm does)
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
    
    # Build concept entry
    concept_entry = {
        "system": "http://snomed.info/sct",
        "code": concept_id,
        "display": display_term or concept_id
    }
    
    if include_designations and designations:
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