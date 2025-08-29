from rest_framework.decorators import api_view
from rest_framework.response import Response
from terminology_api.ES.es_client import es
import json

@api_view(['POST'])
def lookup_post_view(request):
    try:
        # Parse the request body
        if hasattr(request, 'data'):
            data = request.data
        else:
            data = json.loads(request.body)
        
        # Validate resource type
        if data.get("resourceType") != "Parameters":
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "invalid",
                    "details": {"text": "Request must be a Parameters resource"}
                }]
            }, status=400)
        
        # Extract parameters
        parameters = data.get("parameter", [])
        coding = None
        requested_properties = []
        
        for param in parameters:
            if param.get("name") == "coding":
                coding = param.get("valueCoding")
            elif param.get("name") == "property":
                requested_properties.append(param.get("valueString"))
        
        # Validate coding parameter
        if not coding:
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "required",
                    "details": {"text": "Missing required parameter: coding"}
                }]
            }, status=400)
        
        system = coding.get("system")
        code = coding.get("code")
        
        if not system or not code:
            return Response({
                "resourceType": "OperationOutcome",
                "issue": [{
                    "severity": "error",
                    "code": "required",
                    "details": {"text": "Coding must include system and code"}
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
        
        # Get concept
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
        
        # Get descriptions
        descriptions_resp = es.search(
            index="descriptions", 
            body={"query": {"bool": {"must": [
                {"term": {"concept_id": code}},
                {"term": {"active": True}}
            ]}}}, 
            size=1000
        )
        descriptions = descriptions_resp["hits"]["hits"]
        
        # Get language reference set members for acceptability
        desc_ids = []
        for d in descriptions:
            # Try different possible field names for description ID
            desc_id = d["_source"].get("id") or d["_source"].get("description_id") or d.get("_id")
            if desc_id:
                desc_ids.append(desc_id)
        
        lang_refset_members = {}
        if desc_ids:
            try:
                lang_refset_resp = es.search(
                    index="language_refset_members",
                    body={"query": {"bool": {"must": [
                        {"terms": {"referenced_component_id": desc_ids}},
                        {"term": {"active": True}}
                    ]}}},
                    size=1000,
                    ignore=[404]
                )
                lang_refset_members = {m["_source"]["referenced_component_id"]: m["_source"] for m in lang_refset_resp.get("hits", {}).get("hits", [])}
            except Exception:
                # If language refset index doesn't exist, continue without it
                pass
        
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
            
            # Get acceptability from language reference set
            desc_id = src.get("id") or src.get("description_id") or d.get("_id")
            lang_member = lang_refset_members.get(desc_id) if desc_id else None
            
            # Create extensions for designation use context
            extensions = []
            
            # Add context extensions for US and GB editions
            for context_code in ["900000000000509007", "900000000000508004"]:  # US/GB editions
                role_code = "900000000000548007"  # PREFERRED
                role_display = "PREFERRED"
                
                if lang_member:
                    if lang_member.get("acceptability_id") == "900000000000549004":
                        role_code = "900000000000549004"
                        role_display = "ACCEPTABLE"
                
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
        
        # Add core properties
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
            },
            {
                "name": "property",
                "part": [
                    {"name": "code", "valueString": "inactive"},
                    {"name": "valueBoolean", "valueString": str(not concept.get("active", True)).lower()}
                ]
            }
        ]
        
        parameters.extend(properties)
        
        # Add designations (order: synonym first, then FSN)
        synonym_designations = [d for d in designations if d["part"][1]["valueCoding"]["code"] == "900000000000013009"]
        fsn_designations = [d for d in designations if d["part"][1]["valueCoding"]["code"] == "900000000000003001"]
        
        parameters.extend(synonym_designations)
        parameters.extend(fsn_designations)
        
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
        
        # Filter parameters based on requested properties if specified
        if requested_properties:
            filtered_params = []
            
            # Always include core parameters
            core_params = ["code", "display", "name", "system", "version", "active"]
            for param in parameters:
                if param.get("name") in core_params:
                    filtered_params.append(param)
                elif param.get("name") == "property":
                    # Check if this property was requested
                    prop_code = None
                    for part in param.get("part", []):
                        if part.get("name") == "code":
                            prop_code = part.get("valueString")
                            break
                    if prop_code in requested_properties:
                        filtered_params.append(param)
                elif param.get("name") == "designation":
                    # Always include designations for now
                    filtered_params.append(param)
            
            parameters = filtered_params
        
        response = {
            "resourceType": "Parameters",
            "parameter": parameters
        }
        
        return Response(response)
        
    except json.JSONDecodeError:
        return Response({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "invalid",
                "details": {"text": "Invalid JSON in request body"}
            }]
        }, status=400)
    except Exception as e:
        return Response({
            "resourceType": "OperationOutcome", 
            "issue": [{
                "severity": "error",
                "code": "exception",
                "details": {"text": f"Internal server error: {str(e)}"}
            }]
        }, status=500)