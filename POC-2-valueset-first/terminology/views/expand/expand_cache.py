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

@api_view(['POST'])
def expand_view(request):
    """
    FHIR ValueSet $expand operation implementation for SNOMED CT
    Now works with valueset-based descriptions index
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
        valueset_id = param_dict.get('valueset', "v_0")
        filter_text = param_dict.get('filter', '')
        count = min(param_dict.get('count', 10), 100)
        offset = param_dict.get('offset', 0)
        display_language = param_dict.get('displayLanguage', 'en')
        include_designations = param_dict.get('includeDesignations', False)
        
        # Normalize language code (en-gb -> en, en-us -> en)
        if display_language in ['en-gb', 'en-us']:
            display_language = 'en'
        
        print(f"Expand request - ValueSet: {valueset_id}, Language: {display_language}, Count: {count}, Filter: '{filter_text}'")
        
        
        # Validate required parameters
        if not valueset_id:
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "required",
                    "details": {"text": "Missing required parameter: valueset"}
                }]
            }, status=400)
        
        # Get valueset expansion
        if filter_text:
            expansion_contains, total_count = get_filtered_valueset_expansion(
                valueset_id, filter_text, display_language, include_designations, count, offset
            )
        else:
            expansion_contains, total_count = get_valueset_expansion(
                valueset_id, display_language, include_designations, count, offset
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

def get_valueset_expansion(valueset_id, display_language, include_designations, count, offset):
    """
    Get valueset expansion without text filtering
    """
    try:
        # Get all unique concepts using composite aggregation
        all_concept_ids = get_all_concepts_in_valueset(valueset_id, display_language)
        total_count = len(all_concept_ids)
        
        print(f"Found {total_count} unique concepts in valueset {valueset_id}")
        
        # Apply pagination
        paginated_concept_ids = all_concept_ids[offset:offset + count]
        
        # Get detailed descriptions for paginated concepts
        expansion_contains = get_concepts_details_for_valueset(
            paginated_concept_ids, valueset_id, display_language, include_designations
        )
        
        return expansion_contains, total_count
        
    except Exception as e:
        logger.error(f"Error getting valueset expansion: {str(e)}")
        return [], 0

def get_all_concepts_in_valueset(valueset_id, display_language):
    """
    Get all unique concept IDs in a valueset using composite aggregation
    """
    all_concept_ids = []
    after_key = None
    
    while True:
        # Build query with composite aggregation
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"valuesets": valueset_id}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}}
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "unique_concepts": {
                    "composite": {
                        "size": 10000,  # Maximum batch size
                        "sources": [
                            {
                                "concept_id": {
                                    "terms": {
                                        "field": "concept_id.keyword",
                                        "order": "asc"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        # Add after_key for pagination
        if after_key:
            query["aggs"]["unique_concepts"]["composite"]["after"] = after_key
        
        # Execute query
        resp = es.search(
            index="descriptions",
            body=query,
            timeout='30s'
        )
        
        # Extract concept IDs from this batch
        buckets = resp.get("aggregations", {}).get("unique_concepts", {}).get("buckets", [])
        
        if not buckets:
            break
            
        # Add concept IDs to our list
        batch_concept_ids = [bucket["key"]["concept_id"] for bucket in buckets]
        all_concept_ids.extend(batch_concept_ids)
        
        # Check if there are more results
        after_key = resp.get("aggregations", {}).get("unique_concepts", {}).get("after_key")
        if not after_key:
            break
    
    return all_concept_ids

def get_filtered_valueset_expansion(valueset_id, filter_text, display_language, include_designations, count, offset):
    """
    Get valueset expansion with text filtering
    """
    try:
        # Normalize search text
        # normalized_filter = normalize_search_text(filter_text)
        normalized_filter = filter_text
        
        # Build query with text filtering
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"valuesets": valueset_id}},
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
            "_source": ["concept_id", "type_id", "term", "language_code", "pt"],
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
        expansion_contains = get_concepts_details_for_valueset(
            concept_ids_for_details, valueset_id, display_language, include_designations
        )
        
        print(f"Found {total_count} matching concepts for filter '{filter_text}'")
        return expansion_contains, total_count
        
    except Exception as e:
        logger.error(f"Error getting filtered valueset expansion: {str(e)}")
        return [], 0

def get_concepts_details_for_valueset(concept_ids, valueset_id, display_language, include_designations):
    """
    Get detailed concept information for specific concepts in a valueset
    """
    if not concept_ids:
        return []
    
    try:
        # Query descriptions for the specific concepts in the valueset
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
            "_source": ["concept_id", "type_id", "term", "language_code", "pt"],
            "size": len(concept_ids) * 20,  # Allow multiple descriptions per concept
            # "sort": [
            #     {"concept_id.keyword": {"order": "asc"}},
            #     {"pt": {"order": "desc"}},  # Preferred terms first
            #     {"type_id.keyword": {"order": "asc"}}  # Then by type
            # ]
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
        
        # Build concept entries
        expansion_contains = []
        for concept_id in concept_ids:
            descriptions = descriptions_by_concept.get(concept_id, [])
            
            concept_entry = build_concept_entry_from_descriptions(
                concept_id, descriptions, include_designations
            )
            
            if concept_entry:
                expansion_contains.append(concept_entry)
        
        print(f"Built {len(expansion_contains)} concept entries")
        return expansion_contains
        
    except Exception as e:
        logger.error(f"Error getting concept details: {str(e)}")
        return []

def build_concept_entry_from_descriptions(concept_id, descriptions, include_designations):
    """
    Build individual concept entry from descriptions
    """
    if not descriptions:
        return {
            "system": "http://snomed.info/sct",
            "code": concept_id,
            "display": concept_id
        }
    
    # Find preferred term (pt = true)
    preferred_term = None
    for desc in descriptions:
        if desc.get("pt", False):
            preferred_term = desc["term"]
            break
    
    # If no preferred term found, use first synonym or FSN
    # if not preferred_term:
    #     # Sort to prioritize synonyms over FSNs
    #     sorted_descriptions = sorted(descriptions, key=lambda x: (
    #         0 if x["type_id"] == "900000000000013009" else 1,  # Synonym first
    #         x["term"]
    #     ))
        
    #     if sorted_descriptions:
    #         preferred_term = sorted_descriptions[0]["term"]
    
    # Build concept entry
    concept_entry = {
        "system": "http://snomed.info/sct",
        "code": concept_id,
        "display": preferred_term or concept_id
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
            "value": preferred_term or concept_id
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
    Normalize search text similar to Snowstorm's approach
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
    Build the final expansion response
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