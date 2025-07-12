from elasticsearch.helpers import bulk
from terminology_api.es_client import es
from terminology_api.rf2_reader.reader import RF2PandasReader  
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    actions = []
    skipped = 0
    for desc in reader.descriptions:
        if pd.isna(desc.term):  # skip rows where term is NaN
            skipped += 1
            continue
        actions.append({
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
        })

    print(f"Skipping {skipped} descriptions with invalid 'term'...")
    success, _ = bulk(es, actions, raise_on_error=True)
    print(f"✅ Indexed {success} descriptions")


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

def index_language_refset(reader):
    actions = [
        {
            "_index": "language_refsets",
            "_id": lang_ref.id,
            "_source": {
                "effective_time": lang_ref.effective_time,
                "active": lang_ref.active,
                "module_id": lang_ref.module_id,
                "refset_id": lang_ref.refset_id,
                "referenced_component_id": lang_ref.referenced_component_id,
                "acceptability_id": lang_ref.acceptability_id
            }
        }
        for lang_ref in reader.language_refsets
    ]
    bulk(es, actions)
    print(f"✅ Indexed {len(actions)} language refsets")

# === Main runner ===
if __name__ == "__main__":
    reader = RF2PandasReader()
    reader.load_rf2_release("SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z/Snapshot")

    # index_concepts(reader)
    # index_descriptions(reader)
    # index_relationships(reader)
    index_language_refset(reader)