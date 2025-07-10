from rest_framework.decorators import api_view
from rest_framework.response import Response
from terminology_api.es_client import es

@api_view(['GET'])
def lookup_view(request):
    system = request.query_params.get("system")
    code = request.query_params.get("code")
    
    # Validate required parameters
    if not system or not code:
        return Response({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "required",
                "details": {"text": "Missing required parameters: system and code"}
            }]
        }, status=400)
    
    # Validate system
    if system != "http://snomed.info/sct":
        return Response({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "not-supported",
                "details": {"text": f"System {system} is not supported"}
            }]
        }, status=400)
    
    try:
        # Get concept - handle 404 properly
        concept_resp = es.get(index="concepts", id=code, ignore=[404])
        if not concept_resp.get("found", False):
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "not-found",
                    "details": {"text": f"Code {code} not found in system {system}"}
                }]
            }, status=404)
        
        concept = concept_resp['_source']
        
        # Get descriptions with language reference sets (acceptabilities)
        descriptions_resp = es.search(
            index="descriptions", 
            body={"query": {"term": {"concept_id": code}}}, 
            size=1000
        )
        descriptions = descriptions_resp["hits"]["hits"]
        
        # Get language reference set members for acceptability
        # lang_refset_resp = es.search(
        #     index="language_refset_members",
        #     body={"query": {"terms": {"referenced_component_id": [d["_source"]["id"] for d in descriptions]}}},
        #     size=1000
        # )
        # lang_refset_members = {m["_source"]["referenced_component_id"]: m["_source"] for m in lang_refset_resp["hits"]["hits"]}
        
        # Get relationships (parents)
        relationships_resp = es.search(
            index="relationships",
            body={"query": {"bool": {"must": [
                {"term": {"source_id": code}},
                {"term": {"type_id": "116680003"}},  # IS-A relationship
                {"term": {"active": True}}
            ]}}},
            size=1000
        )
        parents = [r['_source']['destination_id'] for r in relationships_resp["hits"]["hits"]]
        
        # Get children
        children_resp = es.search(
            index="relationships",
            body={"query": {"bool": {"must": [
                {"term": {"destination_id": code}},
                {"term": {"type_id": "116680003"}},  # IS-A relationship
                {"term": {"active": True}}
            ]}}},
            size=1000
        )
        children = [r['_source']['source_id'] for r in children_resp["hits"]["hits"]]
        
        # Process designations with extensions
        designations = []
        display_term = ""
        
        for d in descriptions:
            src = d["_source"]
            
            if not src.get("active", True):
                continue
                
            # Get acceptability from language reference set
            # lang_member = lang_refset_members.get(src["id"])
            
            # Create extensions for designation use context
            extensions = []
            
            # Add context extensions for US and GB editions
            for context_code in ["900000000000509007", "900000000000508004"]:  # US/GB editions
                role_code = "900000000000548007"  # PREFERRED
                role_display = "PREFERRED"
                
                # if lang_member:
                #     if lang_member.get("acceptability_id") == "900000000000549004":
                #         role_code = "900000000000549004"
                #         role_display = "ACCEPTABLE"
                
                extension = {
                    "url": "http://snomed.info/fhir/StructureDefinition/designation-use-context",
                    "extension": [
                        {
                            "url": "context",
                            "valueCoding": {
                                "system": "http://snomed.info/sct",
                                "code": context_code
                            }
                        },
                        {
                            "url": "role", 
                            "valueCoding": {
                                "system": "http://snomed.info/sct",
                                "code": role_code,
                                "display": role_display
                            }
                        },
                        {
                            "url": "type",
                            "valueCoding": {
                                "system": "http://snomed.info/sct",
                                "code": src["type_id"],
                                "display": "Fully specified name" if src["type_id"] == "900000000000003001" else "Synonym"
                            }
                        }
                    ]
                }
                extensions.append(extension)
            
            # Set display term (prefer synonym over FSN for display)
            if src["type_id"] == "900000000000013009" and not display_term:  # Synonym
                display_term = src["term"]
            elif src["type_id"] == "900000000000003001" and not display_term:  # FSN as fallback
                display_term = src["term"]
            
            # Create designation
            designation = {
                "extension": extensions,
                "name": "designation",
                "part": [
                    {"name": "language", "valueCode": src.get("language_code", "en")},
                    {
                        "name": "use",
                        "valueCoding": {
                            "system": "http://snomed.info/sct",
                            "code": src["type_id"],
                            "display": "Fully specified name" if src["type_id"] == "900000000000003001" else "Synonym"
                        }
                    },
                    {"name": "value", "valueString": src["term"]}
                ]
            }
            designations.append(designation)
        
        # Build response parameters - order matters!
        parameters = [
            {"name": "code", "valueString": code},
            {"name": "display", "valueString": display_term},
            {"name": "name", "valueString": "International Edition"},
            {"name": "system", "valueString": system},
            {"name": "version", "valueString": "http://snomed.info/sct/900000000000207008/version/20220630"},
            {"name": "active", "valueBoolean": concept.get("active", True)},
        ]
        
        # Add properties
        properties = [
            {
                "name": "property",
                "part": [
                    {"name": "code", "valueString": "effectiveTime"},
                    {"name": "valueString", "valueString": concept.get("effective_time", "")}
                ]
            },
            {
                "name": "property", 
                "part": [
                    {"name": "code", "valueString": "moduleId"},
                    {"name": "value", "valueCode": concept.get("module_id", "")}
                ]
            }
        ]
        
        parameters.extend(properties)
        parameters.extend(designations)
        
        # Add parent properties
        for parent_id in parents:
            parameters.append({
                "name": "property",
                "part": [
                    {"name": "code", "valueString": "parent"},
                    {"name": "value", "valueCode": parent_id}
                ]
            })
        
        # Add child properties  
        for child_id in children:
            parameters.append({
                "name": "property",
                "part": [
                    {"name": "code", "valueString": "child"},
                    {"name": "value", "valueCode": child_id}
                ]
            })
        
        response = {
            "resourceType": "Parameters",
            "parameter": parameters
        }
        
        return Response(response)
        
    except Exception as e:
        return Response({
            "resourceType": "OperationOutcome", 
            "issue": [{
                "severity": "error",
                "code": "exception",
                "details": {"text": f"Internal server error: {str(e)}"}
            }]
        }, status=500)