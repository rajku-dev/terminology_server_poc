from rest_framework.decorators import api_view
from rest_framework.response import Response
from terminology_api.ES.es_client import es
import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

@api_view(['POST'])
def validate_code_view(request):
    """
    FHIR ValueSet $validate-code operation implementation for SNOMED CT
    Compatible with Snowstorm format and behavior
    """
    try:
        # Parse request parameters
        params = request.data.get('parameter', [])
        param_dict = {}
        
        for param in params:
            name = param.get('name')
            if 'valueString' in param:
                param_dict[name] = param['valueString']
            elif 'valueUri' in param:
                param_dict[name] = param['valueUri']
            elif 'valueCode' in param:
                param_dict[name] = param['valueCode']
            elif 'valueBoolean' in param:
                param_dict[name] = param['valueBoolean']
            elif 'resource' in param:
                param_dict[name] = param['resource']
        
        # Extract parameters
        url = param_dict.get('url', '')
        value_set = param_dict.get('valueSet', {})
        code = param_dict.get('code', '')
        system = param_dict.get('system', '')
        display = param_dict.get('display', '')
        version = param_dict.get('version', '')
        display_language = param_dict.get('displayLanguage', 'en')
        
        # Normalize language code
        if display_language in ['en-gb', 'en-us']:
            display_language = 'en'
        
        logger.info(f"Validate-code request - Code: {code}, System: {system}, Display: {display}")
        
        # Validate required parameters
        if not code:
            return Response({
                "resourceType": "Parameters",
                "parameter": [
                    {
                        "name": "result",
                        "valueBoolean": False
                    },
                    {
                        "name": "message",
                        "valueString": "Missing required parameter: code"
                    }
                ]
            })
        
        # Default system to SNOMED CT if not specified
        if not system:
            system = 'http://snomed.info/sct'
        
        # Only handle SNOMED CT system
        if system != 'http://snomed.info/sct':
            return Response({
                "resourceType": "Parameters",
                "parameter": [
                    {
                        "name": "result",
                        "valueBoolean": False
                    },
                    {
                        "name": "message",
                        "valueString": f"Unsupported code system: {system}"
                    }
                ]
            })
        
        # Validate against ValueSet if provided
        if value_set or url:
            return validate_code_against_valueset(
                code, display, display_language, value_set, url
            )
        else:
            # Simple code system validation
            return validate_code_in_codesystem(
                code, display, display_language, system, version
            )
    
    except Exception as e:
        logger.error(f"ValueSet validate-code error: {str(e)}", exc_info=True)
        return Response({
            "resourceType": "Parameters",
            "parameter": [
                {
                    "name": "result",
                    "valueBoolean": False
                },
                {
                    "name": "message",
                    "valueString": f"Internal server error: {str(e)}"
                }
            ]
        }, status=500)

def validate_code_against_valueset(code, display, display_language, value_set, url):
    """
    Validate code against a ValueSet
    """
    try:
        # First check if code exists in SNOMED CT
        if not concept_exists(code):
            return build_validation_response(
                result=False,
                message=f"Code '{code}' not found in SNOMED CT",
                display=display
            )
        
        # Get the actual display for the code
        actual_display = get_concept_display(code, display_language)
        
        if not value_set:
            # If only URL is provided, we can't validate against specific ValueSet
            # Return success if code exists in system
            return build_validation_response(
                result=True,
                display=actual_display,
                message="Code validation successful"
            )
        
        # Process ValueSet compose to check if code is included
        compose = value_set.get('compose', {})
        includes = compose.get('include', [])
        excludes = compose.get('exclude', [])
        
        # Check if code is explicitly included
        code_included = False
        for include in includes:
            include_system = include.get('system', '')
            
            # Skip non-SNOMED systems
            if include_system and include_system != 'http://snomed.info/sct':
                continue
            
            # Check direct concept inclusion
            if 'concept' in include:
                concepts = include['concept']
                for concept_entry in concepts:
                    if concept_entry.get('code') == code:
                        code_included = True
                        break
            
            # Check filter inclusion
            if 'filter' in include:
                filters = include['filter']
                for filter_def in filters:
                    if check_code_against_filter(code, filter_def):
                        code_included = True
                        break
            
            # If no specific concepts or filters, include all active concepts
            if 'concept' not in include and 'filter' not in include:
                code_included = True
        
        # Check if code is explicitly excluded
        code_excluded = False
        for exclude in excludes:
            exclude_system = exclude.get('system', '')
            
            # Skip non-SNOMED systems
            if exclude_system and exclude_system != 'http://snomed.info/sct':
                continue
            
            # Check direct concept exclusion
            if 'concept' in exclude:
                concepts = exclude['concept']
                for concept_entry in concepts:
                    if concept_entry.get('code') == code:
                        code_excluded = True
                        break
            
            # Check filter exclusion
            if 'filter' in exclude:
                filters = exclude['filter']
                for filter_def in filters:
                    if check_code_against_filter(code, filter_def):
                        code_excluded = True
                        break
        
        # Determine final validation result
        is_valid = code_included and not code_excluded
        
        # Validate display if provided
        display_message = None
        if display and actual_display:
            if not is_display_match(display, actual_display, display_language):
                display_message = f"Display '{display}' does not match expected display '{actual_display}'"
        
        # Build response message
        if is_valid:
            message = "Code validation successful"
            if display_message:
                message += f". Note: {display_message}"
        else:
            if not code_included:
                message = f"Code '{code}' is not included in the ValueSet"
            else:
                message = f"Code '{code}' is excluded from the ValueSet"
        
        return build_validation_response(
            result=is_valid,
            display=actual_display,
            message=message
        )
    
    except Exception as e:
        logger.error(f"Error validating code against ValueSet: {str(e)}")
        return build_validation_response(
            result=False,
            message=f"Error validating code: {str(e)}",
            display=display
        )

def validate_code_in_codesystem(code, display, display_language, system, version):
    """
    Simple validation of code in code system (without ValueSet)
    """
    try:
        # Check if concept exists
        if not concept_exists(code):
            return build_validation_response(
                result=False,
                message=f"Code '{code}' not found in {system}",
                display=display
            )
        
        # Get actual display
        actual_display = get_concept_display(code, display_language)
        
        # Validate display if provided
        display_valid = True
        display_message = ""
        
        if display and actual_display:
            if not is_display_match(display, actual_display, display_language):
                display_valid = False
                display_message = f"Display '{display}' does not match expected display '{actual_display}'"
        
        # Build response
        if display_valid:
            message = "Code validation successful"
        else:
            message = f"Code exists but {display_message}"
        
        return build_validation_response(
            result=True,  # Code exists in system
            display=actual_display,
            message=message
        )
    
    except Exception as e:
        logger.error(f"Error validating code in CodeSystem: {str(e)}")
        return build_validation_response(
            result=False,
            message=f"Error validating code: {str(e)}",
            display=display
        )

def check_code_against_filter(code, filter_def):
    """
    Check if a code matches a filter definition
    """
    try:
        property_name = filter_def.get('property')
        op = filter_def.get('op')
        value = filter_def.get('value')
        
        if property_name == 'concept' and op == 'is-a':
            # Check if code is a descendant of value
            if code == value:
                return True
            
            # Check if code is a descendant
            ancestors = get_concept_ancestors(code)
            return value in ancestors
        
        elif property_name == 'concept' and op == '=':
            # Exact match
            return code == value
        
        elif property_name == 'concept' and op == 'in':
            # Code is in the specified set
            values = value.split(',') if isinstance(value, str) else [value]
            return code in values
        
        # Add more filter operations as needed
        return False
    
    except Exception as e:
        logger.error(f"Error checking filter: {str(e)}")
        return False

def get_concept_ancestors(concept_id):
    """
    Get all ancestor concepts for a given concept
    """
    try:
        ancestors = set()
        current_level = {concept_id}
        depth = 0
        max_depth = 15
        
        while current_level and depth < max_depth:
            # Find parents of current level concepts
            parents = get_concept_parents(list(current_level))
            
            # Filter out already processed concepts
            new_ancestors = parents - ancestors - {concept_id}
            
            if not new_ancestors:
                break
                
            ancestors.update(new_ancestors)
            current_level = new_ancestors
            depth += 1
        
        return ancestors
    
    except Exception as e:
        logger.error(f"Error getting ancestors for {concept_id}: {str(e)}")
        return set()

def get_concept_parents(concept_ids):
    """
    Get direct parents for given concept IDs
    """
    try:
        parents = set()
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"source_id": concept_ids}},
                        {"term": {"type_id": "116680003"}},  # IS-A relationship
                        {"term": {"active": True}}
                    ]
                }
            },
            "_source": ["destination_id"],
            "size": len(concept_ids) * 20
        }
        
        resp = es.search(
            index="relationships",
            body=query,
            timeout='30s'
        )
        
        for hit in resp["hits"]["hits"]:
            parent_id = hit["_source"]["destination_id"]
            parents.add(parent_id)
        
        return parents
    
    except Exception as e:
        logger.error(f"Error getting parents: {str(e)}")
        return set()

def concept_exists(concept_id):
    """Check if a concept exists and is active"""
    try:
        result = es.get(index="concepts", id=concept_id, ignore=[404])
        if result.get('found', False):
            # Check if concept is active
            source = result.get('_source', {})
            return source.get('active', False)
        return False
    except Exception as e:
        logger.error(f"Error checking concept existence for {concept_id}: {str(e)}")
        return False

def get_concept_display(concept_id, display_language):
    """
    Get the preferred display term for a concept
    """
    try:
        # First try to get preferred synonym
        preferred_term = get_preferred_term(concept_id, display_language)
        if preferred_term:
            return preferred_term
        
        # Fallback to any active synonym
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"concept_id": concept_id}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}},
                        {"term": {"type_id": "900000000000013009"}}  # Synonym
                    ]
                }
            },
            "_source": ["term"],
            "size": 1
        }
        
        resp = es.search(
            index="descriptions",
            body=query,
            timeout='30s'
        )
        
        if resp["hits"]["hits"]:
            return resp["hits"]["hits"][0]["_source"]["term"]
        
        # Ultimate fallback to FSN
        fsn_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"concept_id": concept_id}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}},
                        {"term": {"type_id": "900000000000003001"}}  # FSN
                    ]
                }
            },
            "_source": ["term"],
            "size": 1
        }
        
        fsn_resp = es.search(
            index="descriptions",
            body=fsn_query,
            timeout='30s'
        )
        
        if fsn_resp["hits"]["hits"]:
            return fsn_resp["hits"]["hits"][0]["_source"]["term"]
        
        return None
    
    except Exception as e:
        logger.error(f"Error getting display for {concept_id}: {str(e)}")
        return None

def get_preferred_term(concept_id, display_language):
    """
    Get preferred term using language refsets
    """
    try:
        # Map language to refset ID
        refset_map = {
            'en': '900000000000509007',  # US English
            'en-us': '900000000000509007',  # US English
            'en-gb': '900000000000508004',  # GB English
        }
        
        refset_id = refset_map.get(display_language, '900000000000509007')
        
        # Get all active synonyms for the concept
        desc_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"concept_id": concept_id}},
                        {"term": {"active": True}},
                        {"term": {"language_code": display_language}},
                        {"term": {"type_id": "900000000000013009"}}  # Synonym
                    ]
                }
            },
            "_source": ["term"],
            "size": 50
        }
        
        desc_resp = es.search(
            index="descriptions",
            body=desc_query,
            timeout='30s'
        )
        
        if not desc_resp["hits"]["hits"]:
            return None
        
        # Get description IDs
        description_ids = [hit["_id"] for hit in desc_resp["hits"]["hits"]]
        desc_terms = {hit["_id"]: hit["_source"]["term"] for hit in desc_resp["hits"]["hits"]}
        
        # Find preferred description
        pref_query = {
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
            "size": 1
        }
        
        pref_resp = es.search(
            index="language_refsets",
            body=pref_query,
            timeout='30s'
        )
        
        if pref_resp["hits"]["hits"]:
            preferred_desc_id = pref_resp["hits"]["hits"][0]["_source"]["referenced_component_id"]
            return desc_terms.get(preferred_desc_id)
        
        # If no preferred found, return first synonym
        return desc_resp["hits"]["hits"][0]["_source"]["term"]
    
    except Exception as e:
        logger.error(f"Error getting preferred term for {concept_id}: {str(e)}")
        return None

def is_display_match(provided_display, actual_display, language):
    """
    Check if provided display matches actual display
    Allow for minor variations and case differences
    """
    if not provided_display or not actual_display:
        return False
    
    # Normalize both displays
    provided_normalized = normalize_display_text(provided_display)
    actual_normalized = normalize_display_text(actual_display)
    
    # Exact match
    if provided_normalized == actual_normalized:
        return True
    
    # Check if provided is a substring of actual (for FSN vs synonym cases)
    if provided_normalized in actual_normalized or actual_normalized in provided_normalized:
        return True
    
    return False

def normalize_display_text(text):
    """
    Normalize display text for comparison
    """
    # Convert to lowercase
    text = text.lower()
    
    # Remove diacritics
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove common punctuation variations
    text = re.sub(r'[(),-]', ' ', text)
    text = re.sub(r'\s+', ' ', text.strip())
    
    return text

def build_validation_response(result, message=None, display=None, code=None, system=None):
    """
    Build FHIR Parameters response for validation
    """
    parameters = [
        {
            "name": "result",
            "valueBoolean": result
        }
    ]
    
    if message:
        parameters.append({
            "name": "message",
            "valueString": message
        })
    
    if display:
        parameters.append({
            "name": "display",
            "valueString": display
        })
    
    if code:
        parameters.append({
            "name": "code",
            "valueCode": code
        })
    
    if system:
        parameters.append({
            "name": "system",
            "valueUri": system
        })
    
    # Add version parameter
    parameters.append({
        "name": "version",
        "valueString": "http://snomed.info/sct/900000000000207008/version/20240731"
    })
    
    return Response({
        "resourceType": "Parameters",
        "parameter": parameters
    })