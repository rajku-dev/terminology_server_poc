from rest_framework.decorators import api_view
from rest_framework.response import Response
from .es_client import es

@api_view(['GET'])
def lookup_view(request):
    system = request.query_params.get("system")
    code = request.query_params.get("code")

    if not system or not code:
        return Response({"error": "Missing required parameters"}, status=400)

    concept = es.get(index="concepts", id=code, ignore=[404])['_source']
    if not concept:
        return Response({"error": "Concept not found"}, status=404)

    descriptions = es.search(index="descriptions", query={"term": {"concept_id": code}}, size=1000)["hits"]["hits"]
    relationships = es.search(index="relationships", query={"term": {"source_id": code}}, size=1000)["hits"]["hits"]
    parents = [r['_source']['destination_id'] for r in relationships if r['_source']['type_id'] == "116680003"]
    
    children_resp = es.search(index="relationships", query={"term": {"destination_id": code}}, size=1000)["hits"]["hits"]
    children = [r['_source']['source_id'] for r in children_resp if r['_source']['type_id'] == "116680003"]

    preferred_terms = []
    synonyms = []

    for d in descriptions:
        src = d["_source"]
        entry = {
            "name": "designation",
            "part": [
                {"name": "language", "valueCode": src["language_code"]},
                {"name": "use", "valueCoding": {
                    "system": "http://snomed.info/sct",
                    "code": src["type_id"],
                    "display": "Synonym" if src["type_id"] == "900000000000013009" else "Fully specified name"
                }},
                {"name": "value", "valueString": src["term"]}
            ]
        }
        if src["type_id"] == "900000000000003001":
            preferred_terms.append(entry)
        else:
            synonyms.append(entry)

    response = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "code", "valueString": code},
            {"name": "display", "valueString": preferred_terms[0]["part"][2]["valueString"] if preferred_terms else ""},
            {"name": "name", "valueString": "International Edition"},
            {"name": "system", "valueString": system},
            {"name": "version", "valueString": "http://snomed.info/sct/900000000000207008/version/20220630"},
            {"name": "active", "valueBoolean": concept["active"]},
            {
                "name": "property",
                "part": [{"name": "code", "valueString": "effectiveTime"}, {"name": "valueString", "valueString": concept["effective_time"]}]
            },
            {
                "name": "property",
                "part": [{"name": "code", "valueString": "moduleId"}, {"name": "value", "valueCode": concept["module_id"]}]
            },
            *[
                {"name": "property", "part": [{"name": "code", "valueString": "parent"}, {"name": "value", "valueCode": pid}]}
                for pid in parents
            ],
            *[
                {"name": "property", "part": [{"name": "code", "valueString": "child"}, {"name": "value", "valueCode": cid}]}
                for cid in children
            ],
            *preferred_terms,
            *synonyms
        ]
    }

    return Response(response)
