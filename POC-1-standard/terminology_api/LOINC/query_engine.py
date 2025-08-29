from elasticsearch import Elasticsearch
from typing import Dict, List
from datetime import datetime
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LoincQueryEngine:
    """
    Query engine optimized for FHIR terminology operations.
    Provides fast $expand, $lookup, and $validate-code operations.
    """
    
    def __init__(self, es_client: Elasticsearch, index_prefix: str = "loinc"):
        self.es = es_client
        self.indices = {
            'concepts': f"{index_prefix}_concepts",
            'hierarchies': f"{index_prefix}_hierarchies",
            'lookup': f"{index_prefix}_lookup"
        }
    
    def expand_valueset(self, filter_text: str = "", count: int = 10, 
                       offset: int = 0, include_designations: bool = True,
                       expand_entire_codesystem: bool = False,
                       include_spec: Dict = None) -> Dict:
        """
        FHIR $expand operation - optimized for ValueSet expansion
        
        Args:
            filter_text: Text filter for searching
            count: Number of results to return
            offset: Offset for pagination
            include_designations: Whether to include designations
            expand_entire_codesystem: True for empty ValueSets (expand all LOINC codes)
            include_spec: Include specification with specific concepts/filters
        """
        query = {"match_all": {}}
        
        # Handle different expansion scenarios
        if expand_entire_codesystem:
            logger.info("Expanding entire LOINC code system")
            # For empty ValueSets - expand all LOINC codes
            if filter_text:
                # Apply text filter to entire code system
                query = {
                    "bool": {
                        "should": [
                            {"match_phrase_prefix": {"search_terms": filter_text.lower()}},
                            {"match_phrase_prefix": {"display": filter_text.lower()}},
                            {"prefix": {"code": filter_text.upper()}},
                        ],
                        "minimum_should_match": 1
                    }
                }
            else:
                # No filter - return all LOINC codes
                query = {"match_all": {}}
                
        elif include_spec:
            # Handle specific concepts or filters from include specification
            query = self._build_query_from_include_spec(include_spec, filter_text)
            
        elif filter_text:
            # Standard text filtering
            query = {
                "bool": {
                    "should": [
                        {"match_phrase_prefix": {"search_terms": filter_text.lower()}},
                        {"match_phrase_prefix": {"display": filter_text.lower()}},
                        {"prefix": {"code": filter_text.upper()}},
                    ],
                    "minimum_should_match": 1
                }
            }
        
        print(f"LOINC expand - filter_text: '{filter_text}', expand_entire: {expand_entire_codesystem}, count: {count}")

        search_body = {
            "query": query,
            "size": count,
            "from": offset,
            "sort": [
                {"_score": {"order": "desc"}},
                {"code": {"order": "asc"}}
            ],
            "_source": ["code", "system", "display", "type", "designation_value"]
        }
        
        response = self.es.search(index=self.indices['concepts'], body=search_body)
        
        # Format as FHIR ValueSet expansion
        expansion_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
        expansion = {
            "resourceType": "ValueSet",
            "id": str(uuid.uuid4()),
            "expansion": {
                "id": expansion_id,
                "timestamp": timestamp,
                "total": response['hits']['total']['value'],
                "offset": offset,
                "parameter": [
                    {
                        "name": "version",
                        "valueUri": "http://loinc.org|2.77"
                    }
                ],
                "contains": []
            }
        }
        
        for hit in response['hits']['hits']:
            source = hit['_source']
            
            concept = {
                "system": source.get('system', 'http://loinc.org'),
                "code": source['code'],
                "display": source['display']
            }
            
            # Add designations if requested and available
            if include_designations:
                designations = []
                
                # Add display designation
                designations.append({
                    "language": "en",
                    "use": {
                        "system": "http://terminology.hl7.org/CodeSystem/designation-usage",
                        "code": "display"
                    },
                    "value": source['display']
                })
                
                # Add additional designation if different from display
                if source.get('designation_value') and source['designation_value'] != source['display']:
                    designations.append({
                        "language": "en",
                        "use": {
                            "system": "http://loinc.org",
                            "code": "LONG_COMMON_NAME"
                        },
                        "value": source['designation_value']
                    })
                
                if designations:
                    concept["designation"] = designations
            
            expansion['expansion']['contains'].append(concept)
        
        return expansion
    
    def _build_query_from_include_spec(self, include_spec: Dict, filter_text: str = "") -> Dict:
        """
        Build Elasticsearch query from FHIR ValueSet include specification
        """
        query_parts = []
        
        # Handle specific concept codes
        if 'concept' in include_spec and include_spec['concept']:
            concept_codes = [concept['code'] for concept in include_spec['concept']]
            query_parts.append({"terms": {"code": concept_codes}})
        
        # Handle filters
        if 'filter' in include_spec and include_spec['filter']:
            for filter_def in include_spec['filter']:
                property_name = filter_def.get('property')
                op = filter_def.get('op')
                value = filter_def.get('value')
                
                if property_name == 'STATUS' and op == 'equals':
                    # Filter by LOINC status
                    query_parts.append({"term": {"status": value}})
                elif property_name == 'CLASS' and op == 'equals':
                    # Filter by LOINC class
                    query_parts.append({"term": {"class": value.upper()}})
                elif property_name == 'COMPONENT' and op == 'equals':
                    # Filter by component
                    query_parts.append({"match": {"component": value}})
                # Add more property filters as needed
        
        # Combine with text filter if provided
        if filter_text:
            text_query = {
                "bool": {
                    "should": [
                        {"match_phrase_prefix": {"search_terms": filter_text.lower()}},
                        {"match_phrase_prefix": {"display": filter_text.lower()}},
                        {"prefix": {"code": filter_text.upper()}},
                    ],
                    "minimum_should_match": 1
                }
            }
            query_parts.append(text_query)
        
        if query_parts:
            if len(query_parts) == 1:
                return query_parts[0]
            else:
                return {"bool": {"must": query_parts}}
        else:
            return {"match_all": {}}
    
    def lookup_concept(self, code: str, system: str = "http://loinc.org") -> Dict:
        """
        FHIR $lookup operation - fast concept lookup with properties
        """
        try:
            # First try lookup cache for fastest response
            response = self.es.get(index=self.indices['lookup'], id=code)
            source = response['_source']
            
            result = {
                "name": "LOINC",
                "system": system,
                "display": source['display']
            }
            
            # Add properties if available
            if source.get('properties'):
                properties = []
                for prop_code, prop_value in source['properties'].items():
                    if prop_value:
                        properties.append({
                            "code": prop_code,
                            "value": prop_value
                        })
                if properties:
                    result["property"] = properties
            
            # Add designations if available
            if source.get('designations'):
                result["designation"] = source['designations']
            
            # Get hierarchical relationships
            hierarchy_info = self._get_hierarchy_info(code)
            if hierarchy_info:
                if 'property' not in result:
                    result['property'] = []
                result['property'].extend(hierarchy_info)
            
            return result
            
        except Exception as e:
            logger.error(f"Lookup failed for {code}: {e}")
            return {"error": f"Code {code} not found"}
    
    def validate_code(self, code: str, system: str = "http://loinc.org", 
                     display: str = None) -> Dict:
        """
        FHIR $validate-code operation - fast code validation
        """
        try:
            response = self.es.get(index=self.indices['lookup'], id=code)
            source = response['_source']
            
            result = {
                "result": True,
                "system": system,
                "code": code,
                "display": source['display']
            }
            
            # Validate display if provided
            if display and display != source['display']:
                # Check if display matches any designation
                if source.get('designations'):
                    display_valid = any(
                        d.get('value', '').lower() == display.lower() 
                        for d in source['designations']
                    )
                    if not display_valid:
                        result["message"] = f"Display '{display}' does not match expected '{source['display']}'"
            
            return result
            
        except Exception as e:
            return {
                "result": False,
                "system": system,
                "code": code,
                "message": f"Code {code} not found in system {system}"
            }
    
    def _get_hierarchy_info(self, code: str) -> List[Dict]:
        """Get parent-child relationships for a code"""
        try:
            # Get from main concepts index which has pre-computed relationships
            response = self.es.get(index=self.indices['concepts'], id=code)
            source = response['_source']
            
            properties = []
            
            # Add parent relationships
            if source.get('parents'):
                for parent in source['parents']:
                    properties.append({
                        "code": "parent",
                        "value": {
                            "system": "http://loinc.org",
                            "code": parent
                        }
                    })
            
            # Add child relationships (limit to avoid large responses)
            if source.get('children'):
                for child in source['children'][:10]:  # Limit children
                    properties.append({
                        "code": "child", 
                        "value": {
                            "system": "http://loinc.org",
                            "code": child
                        }
                    })
            
            return properties
            
        except Exception as e:
            logger.error(f"Failed to get hierarchy info for {code}: {e}")
            return []