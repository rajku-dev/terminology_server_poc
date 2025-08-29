#!/usr/bin/env python3
"""
Script to add a 'pt' (preferred term) boolean column to the descriptions index.
The script processes valuesets, expands them to find all concepts, determines preferred terms,
and updates the descriptions index with pt=1 for preferred terms only (others remain blank).
"""

import logging
from terminology_api.ES.es_client import es

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Valueset compositions from the provided data
VALUESETS = [
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "223366009", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "224930009", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "105590001", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "418038007", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "267425008", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "29736007", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "340519003", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "190753003", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "413427002", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "716186003", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "404684003", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "419492006", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "736665006", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "404684003", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "91723000", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "284009009", "property": "concept"}], "system": "http://snomed.info/sct"}]},
    {"exclude": [], "include": [{"system": "http://snomed.info/sct", "concept": [{"code": "53075003", "display": "Distal phalanx of hallux"}, {"code": "76986006", "display": "Distal phalanx of second toe"}, {"code": "65258003", "display": "Distal phalanx of third toe"}, {"code": "54333003", "display": "Distal phalanx of fourth toe"}, {"code": "10770001", "display": "Distal phalanx of fifth toe"}, {"code": "363670009", "display": "Interphalangeal joint structure of great toe"}, {"code": "371216008", "display": "Distal interphalangeal joint of second toe"}, {"code": "371219001", "display": "Distal interphalangeal joint of third toe"}, {"code": "371205001", "display": "Distal interphalangeal joint of fourth toe"}, {"code": "371203008", "display": "Distal interphalangeal joint of fifth toe"}, {"code": "371292009", "display": "Proximal interphalangeal joint of second toe"}, {"code": "371255009", "display": "Proximal interphalangeal joint of third toe"}, {"code": "371288002", "display": "Proximal interphalangeal joint of fourth toe"}, {"code": "371284000", "display": "Proximal interphalangeal joint of fifth toe"}, {"code": "67169006", "display": "Head of first metatarsal bone"}, {"code": "9677004", "display": "Head of second metatarsal bone"}, {"code": "46971007", "display": "Head of third metatarsal bone"}, {"code": "3134008", "display": "Head of fourth metatarsal bone"}, {"code": "71822005", "display": "Head of fifth metatarsal bone"}, {"code": "89221001", "display": "Base of first metatarsal bone"}, {"code": "90894004", "display": "Base of second metatarsal bone"}, {"code": "89995006", "display": "Base of third metatarsal bone"}, {"code": "15368009", "display": "Base of fourth metatarsal bone"}, {"code": "30980004", "display": "Base of fifth metatarsal bone"}, {"code": "38607000", "display": "Styloid process of fifth metatarsal bone"}, {"code": "2979003", "display": "Medial cuneiform bone"}, {"code": "19193007", "display": "Intermediate cuneiform bone"}, {"code": "67411009", "display": "Lateral cuneiform bone"}, {"code": "81012005", "display": "Bone structure of cuboid"}, {"code": "75772009", "display": "Bone structure of navicular"}, {"code": "67453005", "display": "Bone structure of talus"}, {"code": "80144004", "display": "Bone structure of calcaneum"}, {"code": "6417001", "display": "Medial malleolus"}, {"code": "113225006", "display": "Lateral malleolus of fibula"}, {"code": "22457002", "display": "Head of fibula"}, {"code": "45879002", "display": "Tibial tuberosity"}, {"code": "122474001", "display": "Medial condyle of femur"}, {"code": "122475000", "display": "Lateral condyle of femur"}, {"code": "69030007", "display": "Ischial tuberosity"}, {"code": "29850006", "display": "Iliac crest"}], "filter": [{"op": "is-a", "value": "442083009", "property": "concept"}]}]},
    {"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "272394005", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "129264002", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "386053000", "property": "concept"}], "system": "http://snomed.info/sct"}]}
]

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
                "size": 5000
            }

            # Start scroll
            resp = es.search(
                index="relationships",
                body=query,
                scroll="5m",
                timeout="60s"
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
            while resp["hits"]["hits"]:
                resp = es.scroll(scroll_id=scroll_id, scroll="5m")
                for hit in resp["hits"]["hits"]:
                    child_id = hit["_source"]["source_id"]
                    if child_id not in all_descendants and child_id != concept_id:
                        all_descendants.add(child_id)
                        next_level.add(child_id)

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
        logger.error(f"Error finding descendants for {concept_id}: {str(e)}")
        return all_descendants

def get_preferred_terms_batch(concept_ids, display_language='en'):
    """Get preferred terms from language_refsets index using scroll API for large datasets"""
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
        
        # STEP 1: Get all descriptions for the concepts using scroll API
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
            "size": 5000  # Fixed size within limits
        }
        
        # Initialize scroll for descriptions
        descriptions_resp = es.search(
            index="descriptions",
            body=descriptions_query,
            scroll="5m",
            timeout="60s"
        )
        
        scroll_id = descriptions_resp.get("_scroll_id")
        description_ids = []
        desc_to_concept = {}
        
        # Process initial batch
        for hit in descriptions_resp['hits']['hits']:
            description_ids.append(hit['_id'])
            desc_to_concept[hit['_id']] = hit['_source']
        
        # Continue scrolling for remaining results
        while descriptions_resp['hits']['hits']:
            descriptions_resp = es.scroll(scroll_id=scroll_id, scroll="5m")
            for hit in descriptions_resp['hits']['hits']:
                description_ids.append(hit['_id'])
                desc_to_concept[hit['_id']] = hit['_source']
        
        # Clear scroll context
        if scroll_id:
            try:
                es.clear_scroll(scroll_id=scroll_id)
            except Exception as e:
                logger.warning(f"Error clearing scroll for descriptions: {str(e)}")
        
        if not description_ids:
            logger.warning(f"No descriptions found for concepts in language {display_language}")
            return {}
        
        logger.info(f"Found {len(description_ids)} descriptions for processing")
        
        # STEP 2: Check which descriptions are preferred in language_refsets
        # Process description IDs in batches to avoid the 10k limit
        preferred_terms = {}
        preferred_synonyms = {}
        preferred_fsns = {}
        
        desc_batch_size = 5000  # Process description IDs in batches
        for i in range(0, len(description_ids), desc_batch_size):
            batch_desc_ids = description_ids[i:i + desc_batch_size]
            
            language_refsets_query = {
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"referenced_component_id": batch_desc_ids}},
                            {"term": {"refset_id": refset_id}},
                            {"term": {"active": True}},
                            {"term": {"acceptability_id": "900000000000548007"}}  # Preferred
                        ]
                    }
                },
                "_source": ["referenced_component_id"],
                "size": len(batch_desc_ids)  # This should be <= 5000
            }
            
            refsets_resp = es.search(
                index="language_refsets",
                body=language_refsets_query,
                timeout='30s'
            )
            
            # Process this batch of refset results
            for hit in refsets_resp['hits']['hits']:
                desc_id = hit['_source']['referenced_component_id']
                
                if desc_id in desc_to_concept:
                    concept_info = desc_to_concept[desc_id]
                    concept_id = concept_info['concept_id']
                    type_id = concept_info['type_id']
                    
                    # Separate synonyms and FSNs
                    if type_id == "900000000000013009":  # Synonym
                        if concept_id not in preferred_synonyms:
                            preferred_synonyms[concept_id] = desc_id
                    elif type_id == "900000000000003001":  # FSN
                        if concept_id not in preferred_fsns:
                            preferred_fsns[concept_id] = desc_id
        
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
    
def expand_valueset(valueset_compose):
    """Expand a valueset to get all concept IDs"""
    all_concept_ids = set()
    
    includes = valueset_compose.get('include', [])
    excludes = valueset_compose.get('exclude', [])
    
    # Process includes
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
                
                # Get descendants
                descendants = find_descendants_batch(value)
                all_concept_ids.update(descendants)
                all_concept_ids.add(value)  # Include the concept itself
                logger.info(f"Found {len(descendants)} descendants for {value}")
    
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
    
    logger.info(f"Expanded valueset to {len(all_concept_ids)} concepts")
    return all_concept_ids

def update_descriptions_with_pt_flag(preferred_description_ids, batch_size=1000):
    """Update descriptions index with pt flag - only set pt=1 for preferred descriptions"""
    logger.info(f"Updating descriptions index with pt flag for {len(preferred_description_ids)} preferred descriptions")
    
    # Only set preferred descriptions to pt=1, leave others blank (no pt field)
    if preferred_description_ids:
        for i in range(0, len(preferred_description_ids), batch_size):
            batch = list(preferred_description_ids)[i:i + batch_size]
            
            update_preferred_query = {
                "script": {
                    "source": "ctx._source.pt = 1",
                    "lang": "painless"
                },
                "query": {
                    "terms": {
                        "_id": batch
                    }
                }
            }
            
            try:
                logger.info(f"Setting batch {i//batch_size + 1} of preferred descriptions to pt=1...")
                response = es.update_by_query(
                    index="descriptions",
                    body=update_preferred_query,
                    timeout='5m',
                    wait_for_completion=True
                )
                logger.info(f"Updated {response.get('updated', 0)} descriptions to pt=1 in batch {i//batch_size + 1}")
            except Exception as e:
                logger.error(f"Error updating batch {i//batch_size + 1} to pt=1: {str(e)}")
                return False
    
    return True

def main():
    """Main function to process all valuesets and update descriptions"""
    logger.info("Starting preferred term column update process...")
    
    all_concept_ids = set()
    
    # Process all valuesets to get unique concept IDs
    for i, valueset in enumerate(VALUESETS):
        logger.info(f"Processing valueset {i + 1}/{len(VALUESETS)}")
        concept_ids = expand_valueset(valueset)
        all_concept_ids.update(concept_ids)
    
    logger.info(f"Total unique concepts across all valuesets: {len(all_concept_ids)}")
    
    # Get preferred terms for all concepts
    all_preferred_description_ids = set()
    concept_list = list(all_concept_ids)
    
    # Process in batches to avoid memory issues
    batch_size = 5000
    for i in range(0, len(concept_list), batch_size):
        batch = concept_list[i:i + batch_size]
        logger.info(f"Getting preferred terms for batch {i//batch_size + 1}/{(len(concept_list) + batch_size - 1)//batch_size}")
        
        preferred_terms = get_preferred_terms_batch(batch)
        all_preferred_description_ids.update(preferred_terms.values())
    
    logger.info(f"Total preferred description IDs found: {len(all_preferred_description_ids)}")
    
    # Update descriptions index - only set pt=1 for preferred descriptions
    success = update_descriptions_with_pt_flag(all_preferred_description_ids)
    
    if success:
        logger.info("Successfully updated descriptions index with pt column")
    else:
        logger.error("Failed to update descriptions index")
    
    # Verify the update
    try:
        count_query = {
            "query": {
                "term": {
                    "pt": 1
                }
            }
        }
        
        response = es.count(index="descriptions", body=count_query)
        pt_count = response.get('count', 0)
        
        logger.info(f"Verification: {pt_count} descriptions now have pt=1")
        
    except Exception as e:
        logger.error(f"Error verifying update: {str(e)}")

if __name__ == "__main__":
    main()