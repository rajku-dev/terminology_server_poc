import csv
from pathlib import Path
from typing import List, Dict, Union, Optional
from dataclasses import dataclass, field

@dataclass
class Description:
    id: str
    effective_time: str
    active: bool
    module_id: str
    concept_id: str
    language_code: str
    type_id: str
    term: str
    case_significance: str

@dataclass
class Relationship:
    id: str
    effective_time: str
    active: bool
    module_id: str
    source_id: str
    destination_id: str
    relationship_group: str
    type_id: str
    characteristic_type: str
    modifier: str

@dataclass
class Concept:
    id: str
    effective_time: str
    active: bool
    module_id: str
    definition_status: str
    descriptions: List[Description] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)
    stated_relationships: List[Relationship] = field(default_factory=list)

@dataclass
class ReferenceSet:
    id: str
    effective_time: str
    active: bool
    module_id: str
    refset_id: str
    referenced_component_id: str
    additional_fields: Dict[str, str] = field(default_factory=dict)


class RF2Reader:
    """Reader for SNOMED CT RF2 (Release Format 2) files"""
    
    def __init__(self):
        self.concepts: Dict[str, Concept] = {}
        self.descriptions: List[Description] = []
        self.relationships: List[Relationship] = []
        self.reference_sets: List[ReferenceSet] = []
    
    def read_concepts_file(self, file_path: Union[str, Path]) -> List[Concept]:
        """
        Read RF2 Concept file (sct2_Concept_*.txt)
        Expected columns: id, effectiveTime, active, moduleId, definitionStatusId
        """
        concepts = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            for row in reader:
                concept = Concept(
                    id=row['id'],
                    effective_time=row['effectiveTime'],
                    active=row['active'] == '1',
                    module_id=row['moduleId'],
                    definition_status=row['definitionStatusId']
                )
                concepts.append(concept)
                self.concepts[concept.id] = concept
        
        return concepts
    
    def read_descriptions_file(self, file_path: Union[str, Path]) -> List[Description]:
        """
        Read RF2 Description file (sct2_Description_*.txt)
        Expected columns: id, effectiveTime, active, moduleId, conceptId, 
                         languageCode, typeId, term, caseSignificanceId
        """
        descriptions = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            for row in reader:
                description = Description(
                    id=row['id'],
                    effective_time=row['effectiveTime'],
                    active=row['active'] == '1',
                    module_id=row['moduleId'],
                    concept_id=row['conceptId'],
                    language_code=row['languageCode'],
                    type_id=row['typeId'],
                    term=row['term'],
                    case_significance=row['caseSignificanceId']
                )
                descriptions.append(description)
                
                # Link to concept if it exists
                if description.concept_id in self.concepts:
                    self.concepts[description.concept_id].descriptions.append(description)
        
        self.descriptions = descriptions
        return descriptions
    
    def read_relationships_file(self, file_path: Union[str, Path], is_stated: bool = False) -> List[Relationship]:
        """
        Read RF2 Relationship file (sct2_Relationship_*.txt or sct2_StatedRelationship_*.txt)
        Expected columns: id, effectiveTime, active, moduleId, sourceId, destinationId,
                         relationshipGroup, typeId, characteristicTypeId, modifierId
        """
        relationships = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            for row in reader:
                relationship = Relationship(
                    id=row['id'],
                    effective_time=row['effectiveTime'],
                    active=row['active'] == '1',
                    module_id=row['moduleId'],
                    source_id=row['sourceId'],
                    destination_id=row['destinationId'],
                    relationship_group=row['relationshipGroup'],
                    type_id=row['typeId'],
                    characteristic_type=row['characteristicTypeId'],
                    modifier=row['modifierId']
                )
                relationships.append(relationship)
                
                # Link to concept if it exists
                if relationship.source_id in self.concepts:
                    if is_stated:
                        self.concepts[relationship.source_id].stated_relationships.append(relationship)
                    else:
                        self.concepts[relationship.source_id].relationships.append(relationship)
        
        self.relationships.extend(relationships)
        return relationships
    
    def read_reference_set_file(self, file_path: Union[str, Path]) -> List[ReferenceSet]:
        """
        Read RF2 Reference Set file (der2_*Refset_*.txt)
        Standard columns: id, effectiveTime, active, moduleId, refsetId, referencedComponentId
        Additional columns vary by reference set type
        """
        reference_sets = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            # Standard RF2 refset columns
            standard_columns = {'id', 'effectiveTime', 'active', 'moduleId', 'refsetId', 'referencedComponentId'}
            
            for row in reader:
                # Extract additional fields (non-standard columns)
                additional_fields = {k: v for k, v in row.items() if k not in standard_columns}
                
                reference_set = ReferenceSet(
                    id=row['id'],
                    effective_time=row['effectiveTime'],
                    active=row['active'] == '1',
                    module_id=row['moduleId'],
                    refset_id=row['refsetId'],
                    referenced_component_id=row['referencedComponentId'],
                    additional_fields=additional_fields
                )
                reference_sets.append(reference_set)
        
        self.reference_sets.extend(reference_sets)
        return reference_sets
    
    def load_rf2_release(self, release_folder: Union[str, Path]):
        """
        Load a complete RF2 release from a folder containing RF2 files
        
        Args:
            release_folder: Path to folder containing RF2 files
        """
        release_path = Path(release_folder)
        
        # Find and load concept files
        concept_files = list(release_path.glob("**/sct2_Concept_*.txt"))
        for file_path in concept_files:
            print(f"Loading concepts from: {file_path.name}")
            self.read_concepts_file(file_path)
        
        # Find and load description files
        description_files = list(release_path.glob("**/sct2_Description_*.txt"))
        for file_path in description_files:
            print(f"Loading descriptions from: {file_path.name}")
            self.read_descriptions_file(file_path)
        
        # Find and load relationship files
        relationship_files = list(release_path.glob("**/sct2_Relationship_*.txt"))
        for file_path in relationship_files:
            print(f"Loading relationships from: {file_path.name}")
            self.read_relationships_file(file_path, is_stated=False)
        
        # Find and load stated relationship files
        stated_rel_files = list(release_path.glob("**/sct2_StatedRelationship_*.txt"))
        for file_path in stated_rel_files:
            print(f"Loading stated relationships from: {file_path.name}")
            self.read_relationships_file(file_path, is_stated=True)
        
        # Find and load reference set files
        refset_files = list(release_path.glob("**/der2_*Refset_*.txt"))
        for file_path in refset_files:
            print(f"Loading reference set from: {file_path.name}")
            self.read_reference_set_file(file_path)
    
    def get_concept_by_id(self, concept_id: str) -> Optional[Concept]:
        """Get a concept by its ID"""
        return self.concepts.get(concept_id)
    
    def get_active_concepts(self) -> List[Concept]:
        """Get all active concepts"""
        return [concept for concept in self.concepts.values() if concept.active]
    
    def get_descriptions_for_concept(self, concept_id: str) -> List[Description]:
        """Get all descriptions for a specific concept"""
        concept = self.get_concept_by_id(concept_id)
        return concept.descriptions if concept else []
    
    def search_concepts_by_term(self, search_term: str, case_sensitive: bool = False) -> List[Concept]:
        """Search for concepts by description term"""
        matching_concepts = []
        search_term = search_term if case_sensitive else search_term.lower()
        
        for concept in self.concepts.values():
            for description in concept.descriptions:
                if description.active:
                    term = description.term if case_sensitive else description.term.lower()
                    if search_term in term:
                        matching_concepts.append(concept)
                        break
        
        return matching_concepts


# Usage example
# def main():
    # Initialize the reader
    # reader = RF2Reader()
    
    # Option 1: Load individual files
    # reader.read_concepts_file("path/to/sct2_Concept_Snapshot_INT_20240301.txt")
    # reader.read_descriptions_file("path/to/sct2_Description_Snapshot-en_INT_20240301.txt")
    # reader.read_relationships_file("path/to/sct2_Relationship_Snapshot_INT_20240301.txt")
    
    # Option 2: Load entire release folder
    # reader.load_rf2_release("path/to/SnomedCT_InternationalRF2_PRODUCTION_20240301T120000Z")
    
    # Example usage after loading
    # concept = reader.get_concept_by_id("73211009")  # Diabetes mellitus
    # if concept:
    #     print(f"Concept: {concept.id}")
    #     for desc in concept.descriptions:
    #         print(f"  Description: {desc.term}")
    
    # print("RF2Reader initialized. Use load_rf2_release() or individual read methods to load data.")
    
    
def main():
    reader = RF2Reader()
    
    # Load entire release folder (relative path)
    reader.load_rf2_release("SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z/Snapshot")
    
    # Check loaded concept
    concept = reader.get_concept_by_id("73211009")  # Example: Diabetes mellitus
    if concept:
        print(f"Concept ID: {concept.id}, Descriptions:")
        for desc in concept.descriptions:
            print(f" - {desc.term}")




if __name__ == "__main__":
    main()