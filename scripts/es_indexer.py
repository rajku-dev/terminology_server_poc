from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from rf2_reader import RF2PandasReader  

# === Connect to Elasticsearch ===
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "RHf-O0X8RIrGsr77sHnq"),
    verify_certs=False
)

# === Indexing Functions ===
def index_concepts(reader):
    actions = [
        {
            "_index": "concepts",
            "_id": concept.id,
            "_source": {
                "effective_time": concept.effective_time,
                "active": concept.active,
                "module_id": concept.module_id,
                "definition_status": concept.definition_status
            }
        }
        for concept in reader.concepts.values()
    ]
    bulk(es, actions)
    print(f"✅ Indexed {len(actions)} concepts")

def index_descriptions(reader):
    actions = [
        {
            "_index": "descriptions",
            "_id": desc.id,
            "_source": {
                "concept_id": desc.concept_id,
                "term": desc.term,
                "language_code": desc.language_code,
                "active": desc.active,
                "type_id": desc.type_id,
                "case_significance": desc.case_significance
            }
        }
        for desc in reader.descriptions
    ]
    bulk(es, actions)
    print(f"✅ Indexed {len(actions)} descriptions")

def index_relationships(reader):
    actions = [
        {
            "_index": "relationships",
            "_id": rel.id,
            "_source": {
                "source_id": rel.source_id,
                "destination_id": rel.destination_id,
                "relationship_group": rel.relationship_group,
                "type_id": rel.type_id,
                "characteristic_type": rel.characteristic_type,
                "modifier": rel.modifier,
                "active": rel.active
            }
        }
        for rel in reader.relationships
    ]
    bulk(es, actions)
    print(f"✅ Indexed {len(actions)} relationships")

# === Main runner ===
if __name__ == "__main__":
    reader = RF2PandasReader()
    reader.load_rf2_release("SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z/Snapshot")

    index_concepts(reader)
    index_descriptions(reader)
    index_relationships(reader)
