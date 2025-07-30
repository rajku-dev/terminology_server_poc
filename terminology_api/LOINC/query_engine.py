
from elasticsearch import Elasticsearch
from typing import Dict, List
from datetime import datetime
import logging
import time

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
                       offset: int = 0, include_designations: bool = True) -> Dict:
        """
        FHIR $expand operation - optimized for ValueSet expansion
        """
        query = {"match_all": {}}
        print(f"filter_text: {filter_text}")
        
        if filter_text:
           query = {
            "bool": {
                "should": [
                    {"match_phrase_prefix": {"search_terms": filter_text.lower()}},
                ],
            }
        }

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
        expansion = {
            "id": f"expansion-{int(time.time())}",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "total": response['hits']['total']['value'],
            "offset": offset,
            "contains": []
        }
        
        for hit in response['hits']['hits']:
            source = hit['_source']
            
            concept = {
                "system": source['system'],
                "code": source['code'],
                "display": source['display']
            }
            
            # Add designations if requested and available
            if include_designations and source.get('designation_value'):
                if source['designation_value'] != source['display']:
                    concept["designation"] = [{
                        "use": {"system": "null", "code": "null"},
                        "value": source['designation_value']
                    }]
            
            expansion['contains'].append(concept)
        
        return expansion
    
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
