import json
import logging
from terminology_api.ES.es_client import es
from collections import defaultdict
from datetime import datetime

def concept_exists(concept_id):
    """Check if a concept exists in the concepts index"""
    try:
        result = es.get(index="concepts", id=concept_id, ignore=[404])
        return result.get('found', False)
    except Exception as e:
        print(f"Error checking concept existence for {concept_id}: {str(e)}")
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
                "size": 5000  # Increased batch size
            }

            # Start scroll
            resp = es.search(
                index="relationships",
                body=query,
                scroll="5m",  # Increased scroll context lifetime
                timeout="60s"  # Increased timeout
            )

            scroll_id = resp.get("_scroll_id")
            next_level = set()
            total_hits = resp.get("hits", {}).get("total", {}).get("value", 0)
            print(f"Depth {depth}: Query returned {total_hits} total hits")

            # Process initial results
            for hit in resp["hits"]["hits"]:
                child_id = hit["_source"]["source_id"]
                if child_id not in all_descendants and child_id != concept_id:
                    all_descendants.add(child_id)
                    next_level.add(child_id)

            # Continue scrolling until no more results
            scroll_count = len(resp["hits"]["hits"])
            while resp["hits"]["hits"]:
                resp = es.scroll(scroll_id=scroll_id, scroll="5m")
                scroll_count += len(resp["hits"]["hits"])
                for hit in resp["hits"]["hits"]:
                    child_id = hit["_source"]["source_id"]
                    if child_id not in all_descendants and child_id != concept_id:
                        all_descendants.add(child_id)
                        next_level.add(child_id)

            print(f"Depth {depth}: Processed {scroll_count} relationships, found {len(next_level)} new descendants")

            # Clear scroll context
            if scroll_id:
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception as e:
                    print(f"Error clearing scroll for concept {concept_id}: {str(e)}")

            current_level = next_level
            depth += 1

            if not next_level:
                break

        print(f"Total descendants for {concept_id}: {len(all_descendants)}")
        return all_descendants

    except Exception as e:
        print(f"Error finding descendants for {concept_id}: {str(e)}", exc_info=True)
        return all_descendants

def parse_valuesets():
    """Parse the valuesets from the provided JSON strings"""
    valueset_strings = [
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "223366009", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "224930009", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "105590001", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "418038007", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "267425008", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "29736007", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "340519003", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "190753003", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "413427002", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "716186003", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "404684003", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "419492006", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "736665006", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "404684003", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "91723000", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "284009009", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"system": "http://snomed.info/sct", "concept": [{"code": "53075003", "display": "Distal phalanx of hallux"}, {"code": "76986006", "display": "Distal phalanx of second toe"}, {"code": "65258003", "display": "Distal phalanx of third toe"}, {"code": "54333003", "display": "Distal phalanx of fourth toe"}, {"code": "10770001", "display": "Distal phalanx of fifth toe"}, {"code": "363670009", "display": "Interphalangeal joint structure of great toe"}, {"code": "371216008", "display": "Distal interphalangeal joint of second toe"}, {"code": "371219001", "display": "Distal interphalangeal joint of third toe"}, {"code": "371205001", "display": "Distal interphalangeal joint of fourth toe"}, {"code": "371203008", "display": "Distal interphalangeal joint of fifth toe"}, {"code": "371292009", "display": "Proximal interphalangeal joint of second toe"}, {"code": "371255009", "display": "Proximal interphalangeal joint of third toe"}, {"code": "371288002", "display": "Proximal interphalangeal joint of fourth toe"}, {"code": "371284000", "display": "Proximal interphalangeal joint of fifth toe"}, {"code": "67169006", "display": "Head of first metatarsal bone"}, {"code": "9677004", "display": "Head of second metatarsal bone"}, {"code": "46971007", "display": "Head of third metatarsal bone"}, {"code": "3134008", "display": "Head of fourth metatarsal bone"}, {"code": "71822005", "display": "Head of fifth metatarsal bone"}, {"code": "89221001", "display": "Base of first metatarsal bone"}, {"code": "90894004", "display": "Base of second metatarsal bone"}, {"code": "89995006", "display": "Base of third metatarsal bone"}, {"code": "15368009", "display": "Base of fourth metatarsal bone"}, {"code": "30980004", "display": "Base of fifth metatarsal bone"}, {"code": "38607000", "display": "Styloid process of fifth metatarsal bone"}, {"code": "2979003", "display": "Medial cuneiform bone"}, {"code": "19193007", "display": "Intermediate cuneiform bone"}, {"code": "67411009", "display": "Lateral cuneiform bone"}, {"code": "81012005", "display": "Bone structure of cuboid"}, {"code": "75772009", "display": "Bone structure of navicular"}, {"code": "67453005", "display": "Bone structure of talus"}, {"code": "80144004", "display": "Bone structure of calcaneum"}, {"code": "6417001", "display": "Medial malleolus"}, {"code": "113225006", "display": "Lateral malleolus of fibula"}, {"code": "22457002", "display": "Head of fibula"}, {"code": "45879002", "display": "Tibial tuberosity"}, {"code": "122474001", "display": "Medial condyle of femur"}, {"code": "122475000", "display": "Lateral condyle of femur"}, {"code": "69030007", "display": "Ischial tuberosity"}, {"code": "29850006", "display": "Iliac crest"}]}, {"filter": [{"op": "is-a", "value": "442083009", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
        '{"exclude": [], "include": [{"filter": [{"op": "is-a", "value": "272394005", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "129264002", "property": "concept"}], "system": "http://snomed.info/sct"}, {"filter": [{"op": "is-a", "value": "386053000", "property": "concept"}], "system": "http://snomed.info/sct"}]}',
    ]
    
    valuesets = []
    for i, vs_str in enumerate(valueset_strings):
        try:
            vs_data = json.loads(vs_str)
            valuesets.append({
                'id': f'v_{i}',
                'data': vs_data
            })
        except json.JSONDecodeError as e:
            print(f"Error parsing valueset {i}: {str(e)}")
    
    return valuesets

def expand_valueset(valueset_id, valueset_data):
    """Expand a single valueset and return the concept IDs"""
    print(f"Expanding valueset {valueset_id}")
    
    compose = valueset_data
    includes = compose.get('include', [])
    excludes = compose.get('exclude', [])
    
    all_concept_ids = set()
    
    # Process includes
    for include in includes:
        system = include.get('system')
        
        # Skip non-SNOMED systems
        if system != 'http://snomed.info/sct':
            print(f"Skipping non-SNOMED system: {system}")
            continue
        
        # Handle direct concept codes
        if 'concept' in include:
            codes = include['concept']
            for code_entry in codes:
                concept_id = code_entry['code']
                if concept_exists(concept_id):
                    all_concept_ids.add(concept_id)
                    print(f"Added direct concept: {concept_id}")
                else:
                    print(f"Direct concept {concept_id} not found in index")
        
        # Handle filters
        filters = include.get('filter', [])
        for filter_def in filters:
            property_name = filter_def.get('property')
            op = filter_def.get('op')
            value = filter_def.get('value')
            
            # Only process is-a filters
            if property_name == 'concept' and op == 'is-a':
                # Validate root concept exists
                if not concept_exists(value):
                    print(f"Root concept {value} not found in index")
                    continue
                
                # Get descendants using the provided function
                descendants = find_descendants_batch(value)
                all_concept_ids.update(descendants)
                all_concept_ids.add(value)  # Include the root concept itself
                print(f"Added {len(descendants)} descendants for root concept {value}")
    
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
    
    print(f"Valueset {valueset_id} expanded to {len(all_concept_ids)} concepts")
    return all_concept_ids

def get_descriptions_for_concepts(concept_ids):
    """Get all description IDs for the given concept IDs from the descriptions index"""
    print(f"Getting descriptions for {len(concept_ids)} concepts")
    
    concept_description_mapping = defaultdict(set)
    batch_size = 1000
    concept_list = list(concept_ids)
    
    for i in range(0, len(concept_list), batch_size):
        batch = concept_list[i:i + batch_size]
        
        # Query descriptions index for these concept IDs
        query = {
            "query": {
                "terms": {"concept_id": batch}
            },
            "_source": ["concept_id"],
            "size": 10000
        }
        
        try:
            # Start scroll for large result sets
            resp = es.search(
                index="descriptions",
                body=query,
                scroll="5m",
                timeout="60s"
            )
            
            scroll_id = resp.get("_scroll_id")
            
            # Process initial results
            for hit in resp["hits"]["hits"]:
                description_id = hit["_id"]
                concept_id = hit["_source"]["concept_id"]
                concept_description_mapping[concept_id].add(description_id)
            
            # Continue scrolling until no more results
            while resp["hits"]["hits"]:
                resp = es.scroll(scroll_id=scroll_id, scroll="5m")
                for hit in resp["hits"]["hits"]:
                    description_id = hit["_id"]
                    concept_id = hit["_source"]["concept_id"]
                    concept_description_mapping[concept_id].add(description_id)
            
            # Clear scroll context
            if scroll_id:
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception as e:
                    print(f"Error clearing scroll: {str(e)}")
                    
        except Exception as e:
            print(f"Error querying descriptions for batch starting at {i}: {str(e)}")
    
    # Flatten to get all description IDs
    all_description_ids = set()
    for description_ids in concept_description_mapping.values():
        all_description_ids.update(description_ids)
    
    print(f"Found {len(all_description_ids)} descriptions for {len(concept_description_mapping)} concepts")
    return all_description_ids, concept_description_mapping

def update_descriptions_index(description_valueset_mapping):
    """Update the descriptions index with valueset membership information"""
    print("Starting batch update of descriptions index")
    
    batch_size = 100
    description_ids = list(description_valueset_mapping.keys())
    total_descriptions = len(description_ids)
    updated_count = 0
    error_count = 0
    
    for i in range(0, total_descriptions, batch_size):
        batch = description_ids[i:i + batch_size]
        
        # Prepare bulk update operations
        bulk_operations = []
        
        for description_id in batch:
            valueset_ids = list(description_valueset_mapping[description_id])
            
            # Create update operation
            bulk_operations.append({
                "update": {
                    "_index": "descriptions",
                    "_id": description_id
                }
            })
            
            bulk_operations.append({
                "doc": {
                    "valuesets": valueset_ids
                },
                "doc_as_upsert": False
            })
        
        try:
            # Execute bulk update
            response = es.bulk(body=bulk_operations, timeout='60s')
            
            # Check for errors
            if response.get('errors'):
                for item in response['items']:
                    if 'update' in item and 'error' in item['update']:
                        error_count += 1
                        print(f"Error updating description {item['update']['_id']}: {item['update']['error']}")
                    else:
                        updated_count += 1
            else:
                updated_count += len(batch)
            
            print(f"Processed batch {i//batch_size + 1}/{(total_descriptions + batch_size - 1)//batch_size}: {updated_count}/{total_descriptions} descriptions updated")
            
        except Exception as e:
            print(f"Error in bulk update for batch starting at {i}: {str(e)}")
            error_count += len(batch)
    
    print(f"Bulk update completed. Updated: {updated_count}, Errors: {error_count}")
    return updated_count, error_count

def main():
    """Main function to process all valuesets and update the descriptions index"""
    start_time = datetime.now()
    print("Starting valueset expansion and description index update")
    
    # Parse valuesets
    valuesets = parse_valuesets()
    print(f"Parsed {len(valuesets)} valuesets")
    
    # Create mapping of description_id -> set of valueset_ids
    description_valueset_mapping = defaultdict(set)
    
    # Process each valueset
    for valueset in valuesets:
        valueset_id = valueset['id']
        valueset_data = valueset['data']
        
        # Expand the valueset to get concept IDs
        concept_ids = expand_valueset(valueset_id, valueset_data)
        
        # Get description IDs for these concepts
        description_ids = get_descriptions_for_concepts(concept_ids)
        
        # Add to mapping
        for description_id in description_ids:
            description_valueset_mapping[description_id].add(valueset_id)
    
    print(f"Total unique descriptions across all valuesets: {len(description_valueset_mapping)}")
    
    # Display some statistics
    for valueset in valuesets:
        valueset_id = valueset['id']
        descriptions_in_valueset = sum(1 for description_valuesets in description_valueset_mapping.values() 
                                     if valueset_id in description_valuesets)
        print(f"Valueset {valueset_id}: {descriptions_in_valueset} descriptions")
    
    # Update the descriptions index
    updated_count, error_count = update_descriptions_index(description_valueset_mapping)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Total valuesets processed: {len(valuesets)}")
    print(f"Total descriptions updated: {updated_count}")
    print(f"Total errors: {error_count}")
    print(f"Total processing time: {duration}")
    print("="*50)

if __name__ == "__main__":
    main()