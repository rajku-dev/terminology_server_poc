import pandas as pd
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

@dataclass
class LanguageRefset:
    """
    Language Reference Set entry for SNOMED CT
    Used to specify the acceptability of descriptions in specific languages/dialects
    """
    id: str
    effective_time: str
    active: bool
    module_id: str
    refset_id: str
    referenced_component_id: str  # This is the description ID
    acceptability_id: str  # 900000000000548007 = Preferred, 900000000000549004 = Acceptable


class RF2PandasReader:
    """Pandas-based reader for SNOMED CT RF2 (Release Format 2) files"""
    
    def __init__(self):
        # DataFrames for raw data
        self.concepts_df: Optional[pd.DataFrame] = None
        self.descriptions_df: Optional[pd.DataFrame] = None
        self.relationships_df: Optional[pd.DataFrame] = None
        self.stated_relationships_df: Optional[pd.DataFrame] = None
        # self.reference_sets_df: Optional[pd.DataFrame] = None
        self.language_refsets_df: Optional[pd.DataFrame] = None
        
        # Object collections 
        self.concepts: Dict[str, Concept] = {}
        self.descriptions: List[Description] = []
        self.relationships: List[Relationship] = []
        # self.reference_sets: List[ReferenceSet] = []
        self.language_refsets: List[LanguageRefset] = []
    
    def read_concepts_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Read RF2 Concept file using pandas
        Expected columns: id, effectiveTime, active, moduleId, definitionStatusId
        """
        print(f"Loading concepts from: {Path(file_path).name}")
        
        # Read the file with pandas
        df = pd.read_csv(
            file_path,
            sep='\t',
            dtype={
                'id': str,
                'effectiveTime': str,
                'active': str,
                'moduleId': str,
                'definitionStatusId': str
            },
            encoding='utf-8'
        )
        
        # Convert active column to boolean
        df['active'] = df['active'] == '1'
        
        # Store DataFrame
        if self.concepts_df is None:
            self.concepts_df = df
        else:
            self.concepts_df = pd.concat([self.concepts_df, df], ignore_index=True)
        
        # Create Concept objects and populate concepts dict
        for _, row in df.iterrows():
            concept = Concept(
                id=row['id'],
                effective_time=row['effectiveTime'],
                active=row['active'],
                module_id=row['moduleId'],
                definition_status=row['definitionStatusId']
            )
            self.concepts[concept.id] = concept
        
        return df
    
    def read_descriptions_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Read RF2 Description file using pandas
        Expected columns: id, effectiveTime, active, moduleId, conceptId, 
                         languageCode, typeId, term, caseSignificanceId
        """
        print(f"Loading descriptions from: {Path(file_path).name}")
        
        df = pd.read_csv(
            file_path,
            sep='\t',
            dtype={
                'id': str,
                'effectiveTime': str,
                'active': str,
                'moduleId': str,
                'conceptId': str,
                'languageCode': str,
                'typeId': str,
                'term': str,
                'caseSignificanceId': str
            },
            encoding='utf-8'
        )
        
        # Convert active column to boolean
        df['active'] = df['active'] == '1'
        
        # Store DataFrame
        if self.descriptions_df is None:
            self.descriptions_df = df
        else:
            self.descriptions_df = pd.concat([self.descriptions_df, df], ignore_index=True)
        
        # Create Description objects and link to concepts
        for _, row in df.iterrows():
            description = Description(
                id=row['id'],
                effective_time=row['effectiveTime'],
                active=row['active'],
                module_id=row['moduleId'],
                concept_id=row['conceptId'],
                language_code=row['languageCode'],
                type_id=row['typeId'],
                term=row['term'],
                case_significance=row['caseSignificanceId']
            )
            self.descriptions.append(description)
            
            # Link to concept if it exists
            if description.concept_id in self.concepts:
                self.concepts[description.concept_id].descriptions.append(description)
        
        return df
    
    def read_relationships_file(self, file_path: Union[str, Path], is_stated: bool = False) -> pd.DataFrame:
        """
        Read RF2 Relationship file using pandas
        Expected columns: id, effectiveTime, active, moduleId, sourceId, destinationId,
                         relationshipGroup, typeId, characteristicTypeId, modifierId
        """
        print(f"Loading {'stated ' if is_stated else ''}relationships from: {Path(file_path).name}")
        
        df = pd.read_csv(
            file_path,
            sep='\t',
            dtype={
                'id': str,
                'effectiveTime': str,
                'active': str,
                'moduleId': str,
                'sourceId': str,
                'destinationId': str,
                'relationshipGroup': str,
                'typeId': str,
                'characteristicTypeId': str,
                'modifierId': str
            },
            encoding='utf-8'
        )
        
        # Convert active column to boolean
        df['active'] = df['active'] == '1'
        
        # Store DataFrame
        if is_stated:
            if self.stated_relationships_df is None:
                self.stated_relationships_df = df
            else:
                self.stated_relationships_df = pd.concat([self.stated_relationships_df, df], ignore_index=True)
        else:
            if self.relationships_df is None:
                self.relationships_df = df
            else:
                self.relationships_df = pd.concat([self.relationships_df, df], ignore_index=True)
        
        # Create Relationship objects and link to concepts
        for _, row in df.iterrows():
            relationship = Relationship(
                id=row['id'],
                effective_time=row['effectiveTime'],
                active=row['active'],
                module_id=row['moduleId'],
                source_id=row['sourceId'],
                destination_id=row['destinationId'],
                relationship_group=row['relationshipGroup'],
                type_id=row['typeId'],
                characteristic_type=row['characteristicTypeId'],
                modifier=row['modifierId']
            )
            self.relationships.append(relationship)
            
            # Link to concept if it exists
            if relationship.source_id in self.concepts:
                if is_stated:
                    self.concepts[relationship.source_id].stated_relationships.append(relationship)
                else:
                    self.concepts[relationship.source_id].relationships.append(relationship)
        
        return df
    
    def read_language_refset_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Read RF2 Language Reference Set file using pandas
        Expected columns: id, effectiveTime, active, moduleId, refsetId, referencedComponentId, acceptabilityId
        """
        print(f"Loading language reference set from: {Path(file_path).name}")
        
        df = pd.read_csv(
            file_path,
            sep='\t',
            dtype={
                'id': str,
                'effectiveTime': str,
                'active': str,
                'moduleId': str,
                'refsetId': str,
                'referencedComponentId': str,
                'acceptabilityId': str
            },
            encoding='utf-8'
        )
        
        # Convert active column to boolean
        df['active'] = df['active'] == '1'
        
        # Store DataFrame
        if self.language_refsets_df is None:
            self.language_refsets_df = df
        else:
            self.language_refsets_df = pd.concat([self.language_refsets_df, df], ignore_index=True)
        
        # Create LanguageRefset objects
        for _, row in df.iterrows():
            language_refset = LanguageRefset(
                id=row['id'],
                effective_time=row['effectiveTime'],
                active=row['active'],
                module_id=row['moduleId'],
                refset_id=row['refsetId'],
                referenced_component_id=row['referencedComponentId'],
                acceptability_id=row['acceptabilityId']
            )
            self.language_refsets.append(language_refset)
        
        return df
    
    # def read_reference_set_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
    #     """
    #     Read RF2 Reference Set file using pandas
    #     Standard columns: id, effectiveTime, active, moduleId, refsetId, referencedComponentId
    #     Additional columns vary by reference set type
    #     """
    #     print(f"Loading reference set from: {Path(file_path).name}")
        
    #     # Read with string dtype to preserve all data
    #     df = pd.read_csv(
    #         file_path,
    #         sep='\t',
    #         dtype=str,
    #         encoding='utf-8'
    #     )
        
    #     # Convert active column to boolean
    #     df['active'] = df['active'] == '1'
        
    #     # Store DataFrame
    #     if self.reference_sets_df is None:
    #         self.reference_sets_df = df
    #     else:
    #         self.reference_sets_df = pd.concat([self.reference_sets_df, df], ignore_index=True)
        
    #     # Standard RF2 refset columns
    #     standard_columns = {'id', 'effectiveTime', 'active', 'moduleId', 'refsetId', 'referencedComponentId'}
        
    #     # Create ReferenceSet objects
    #     for _, row in df.iterrows():
    #         # Extract additional fields (non-standard columns)
    #         additional_fields = {k: v for k, v in row.items() if k not in standard_columns}
            
    #         reference_set = ReferenceSet(
    #             id=row['id'],
    #             effective_time=row['effectiveTime'],
    #             active=row['active'],
    #             module_id=row['moduleId'],
    #             refset_id=row['refsetId'],
    #             referenced_component_id=row['referencedComponentId'],
    #             additional_fields=additional_fields
    #         )
    #         self.reference_sets.append(reference_set)
        
    #     return df
    
    def load_rf2_release(self, release_folder: Union[str, Path]):
        """
        Load a complete RF2 release from a folder containing RF2 files
        
        Args:
            release_folder: Path to folder containing RF2 files
        """
        release_path = Path(release_folder)
        
        # Find and load concept files
        # concept_files = list(release_path.glob("**/sct2_Concept_*.txt"))
        # for file_path in concept_files:
        #     self.read_concepts_file(file_path)
        
        # # Find and load description files
        # description_files = list(release_path.glob("**/sct2_Description_*.txt"))
        # for file_path in description_files:
        #     self.read_descriptions_file(file_path)
        
        # # Find and load relationship files
        # relationship_files = list(release_path.glob("**/sct2_Relationship_*.txt"))
        # for file_path in relationship_files:
        #     self.read_relationships_file(file_path, is_stated=False)
        
        # # Find and load stated relationship files
        # stated_rel_files = list(release_path.glob("**/sct2_StatedRelationship_*.txt"))
        # for file_path in stated_rel_files:
        #     self.read_relationships_file(file_path, is_stated=True)
        
        # Find and load language reference set files
        language_refset_files = list(release_path.glob("**/der2_cRefset_Language*.txt"))
        for file_path in language_refset_files:
            self.read_language_refset_file(file_path)
        
        # Find and load other reference set files
        # refset_files = list(release_path.glob("**/der2_*Refset_*.txt"))
        # Exclude language refsets as they're handled separately
        # refset_files = [f for f in refset_files if "Language" not in f.name]
        # for file_path in refset_files:
        # #     self.read_reference_set_file(file_path)

        print(f"\nLoaded:")
        # print(f"  Concepts: {len(self.concepts)}")
        # print(f"  Descriptions: {len(self.descriptions)}")
        # print(f"  Relationships: {len(self.relationships)}")
        print(f"  Language Reference Sets: {len(self.language_refsets)}")
        # print(f"  Other Reference Sets: {len(self.reference_sets)}")
    
    # Pandas-optimized query methods
    def get_concept_by_id(self, concept_id: str) -> Optional[Concept]:
        """Get a concept by its ID"""
        return self.concepts.get(concept_id)
    
    def get_active_concepts(self) -> List[Concept]:
        """Get all active concepts"""
        return [concept for concept in self.concepts.values() if concept.active]
    
    # def get_active_concepts_df(self) -> pd.DataFrame:
    #     """Get all active concepts as DataFrame"""
    #     if self.concepts_df is not None:
    #         return self.concepts_df[self.concepts_df['active'] == True]
    #     return pd.DataFrame()
    
    # def get_descriptions_for_concept(self, concept_id: str) -> List[Description]:
    #     """Get all descriptions for a specific concept"""
    #     concept = self.get_concept_by_id(concept_id)
    #     return concept.descriptions if concept else []
    
    # def get_descriptions_for_concept_df(self, concept_id: str) -> pd.DataFrame:
    #     """Get all descriptions for a specific concept as DataFrame"""
    #     if self.descriptions_df is not None:
    #         return self.descriptions_df[self.descriptions_df['conceptId'] == concept_id]
    #     return pd.DataFrame()
    
    def get_preferred_descriptions_df(self, concept_id: str = None, language_refset_id: str = "900000000000509007") -> pd.DataFrame:
        """
        Get preferred descriptions for concept(s) based on language reference set
        
        Args:
            concept_id: Specific concept ID (if None, returns all)
            language_refset_id: Language reference set ID (default is US English)
        
        Returns:
            DataFrame with preferred descriptions
        """
        if self.descriptions_df is None or self.language_refsets_df is None:
            return pd.DataFrame()
        
        # Get active language refset entries for preferred terms
        preferred_refset = self.language_refsets_df[
            (self.language_refsets_df['active'] == True) &
            (self.language_refsets_df['refsetId'] == language_refset_id) &
            (self.language_refsets_df['acceptabilityId'] == "900000000000548007")  # Preferred
        ]
        
        # Join with descriptions
        result = self.descriptions_df.merge(
            preferred_refset[['referencedComponentId']],
            left_on='id',
            right_on='referencedComponentId',
            how='inner'
        )
        
        # Filter by concept if specified
        if concept_id:
            result = result[result['conceptId'] == concept_id]
        
        return result[result['active'] == True]
    
    def get_acceptable_descriptions_df(self, concept_id: str = None, language_refset_id: str = "900000000000509007") -> pd.DataFrame:
        """
        Get acceptable descriptions for concept(s) based on language reference set
        
        Args:
            concept_id: Specific concept ID (if None, returns all)
            language_refset_id: Language reference set ID (default is US English)
        
        Returns:
            DataFrame with acceptable descriptions
        """
        if self.descriptions_df is None or self.language_refsets_df is None:
            return pd.DataFrame()
        
        # Get active language refset entries for acceptable terms
        acceptable_refset = self.language_refsets_df[
            (self.language_refsets_df['active'] == True) &
            (self.language_refsets_df['refsetId'] == language_refset_id) &
            (self.language_refsets_df['acceptabilityId'] == "900000000000549004")  # Acceptable
        ]
        
        # Join with descriptions
        result = self.descriptions_df.merge(
            acceptable_refset[['referencedComponentId']],
            left_on='id',
            right_on='referencedComponentId',
            how='inner'
        )
        
        # Filter by concept if specified
        if concept_id:
            result = result[result['conceptId'] == concept_id]
        
        return result[result['active'] == True]
    
    def search_concepts_by_term(self, search_term: str, case_sensitive: bool = False) -> List[Concept]:
        """Search for concepts by description term"""
        if self.descriptions_df is None:
            return []
        
        # Use pandas for efficient searching
        search_df = self.descriptions_df[self.descriptions_df['active'] == True]
        
        if case_sensitive:
            matching_desc = search_df[search_df['term'].str.contains(search_term, na=False)]
        else:
            matching_desc = search_df[search_df['term'].str.contains(search_term, case=False, na=False)]
        
        # Get unique concept IDs
        concept_ids = matching_desc['conceptId'].unique()
        
        # Return corresponding concept objects
        return [self.concepts[cid] for cid in concept_ids if cid in self.concepts]
    
    # def search_concepts_by_term_df(self, search_term: str, case_sensitive: bool = False) -> pd.DataFrame:
    #     """Search for concepts by description term, return DataFrame"""
    #     if self.descriptions_df is None:
    #         return pd.DataFrame()
        
    #     search_df = self.descriptions_df[self.descriptions_df['active'] == True]
        
    #     if case_sensitive:
    #         matching_desc = search_df[search_df['term'].str.contains(search_term, na=False)]
    #     else:
    #         matching_desc = search_df[search_df['term'].str.contains(search_term, case=False, na=False)]
        
    #     return matching_desc
    
    # def get_relationships_for_concept_df(self, concept_id: str, is_stated: bool = False) -> pd.DataFrame:
    #     """Get relationships for a concept as DataFrame"""
    #     df = self.stated_relationships_df if is_stated else self.relationships_df
    #     if df is not None:
    #         return df[df['sourceId'] == concept_id]
    #     return pd.DataFrame()
    
    # def get_concept_hierarchy_df(self, concept_id: str) -> pd.DataFrame:
    #     """Get parent concepts using IS-A relationships"""
    #     if self.relationships_df is None:
    #         return pd.DataFrame()
        
    #     # IS-A relationship type ID in SNOMED CT
    #     isa_type_id = "116680003"
        
    #     parents = self.relationships_df[
    #         (self.relationships_df['sourceId'] == concept_id) &
    #         (self.relationships_df['typeId'] == isa_type_id) &
    #         (self.relationships_df['active'] == True)
    #     ]
        
    #     return parents
    
    def get_language_refset_for_description_df(self, description_id: str) -> pd.DataFrame:
        """Get language reference set entries for a specific description"""
        if self.language_refsets_df is not None:
            return self.language_refsets_df[
                self.language_refsets_df['referencedComponentId'] == description_id
            ]
        return pd.DataFrame()
    
    def get_memory_usage(self) -> Dict[str, str]:
        """Get memory usage information for loaded DataFrames"""
        usage = {}
        
        if self.concepts_df is not None:
            usage['concepts'] = f"{self.concepts_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        
        if self.descriptions_df is not None:
            usage['descriptions'] = f"{self.descriptions_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        
        if self.relationships_df is not None:
            usage['relationships'] = f"{self.relationships_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        
        if self.stated_relationships_df is not None:
            usage['stated_relationships'] = f"{self.stated_relationships_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        
        if self.language_refsets_df is not None:
            usage['language_refsets'] = f"{self.language_refsets_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        
        if self.reference_sets_df is not None:
            usage['reference_sets'] = f"{self.reference_sets_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB"
        
        return usage


# # Usage example
# def main():
#     reader = RF2PandasReader()
    
#     # Load entire release folder
#     reader.load_rf2_release("SnomedCT_InternationalRF2_PRODUCTION_20250501T120000Z/Snapshot")
    
#     # Example 1: Get concept using original interface
#     concept = reader.get_concept_by_id("73211009")  # Example: Diabetes mellitus
#     if concept:
#         print(f"\nConcept ID: {concept.id}")
#         print(f"Active: {concept.active}")
#         print(f"Descriptions:")
#         for desc in concept.descriptions:
#             print(f"  - {desc.term}")
    
#     # Example 2: Use pandas for efficient queries
#     print("\n=== Pandas Query Examples ===")
    
#     # Search for diabetes-related concepts
#     diabetes_concepts = reader.search_concepts_by_term("diabetes")
#     print(f"Found {len(diabetes_concepts)} concepts containing 'diabetes'")
    
#     # Get preferred descriptions for a concept
#     preferred_desc = reader.get_preferred_descriptions_df("73211009")
#     print(f"\nPreferred descriptions for concept 73211009:")
#     if not preferred_desc.empty:
#         print(preferred_desc[['term', 'typeId']].to_string(index=False))
    
#     # Get acceptable descriptions for a concept
#     acceptable_desc = reader.get_acceptable_descriptions_df("73211009")
#     print(f"\nAcceptable descriptions for concept 73211009:")
#     if not acceptable_desc.empty:
#         print(acceptable_desc[['term', 'typeId']].to_string(index=False))
    
#     # Get language refset info for a description
#     if not preferred_desc.empty:
#         desc_id = preferred_desc.iloc[0]['id']
#         lang_refset = reader.get_language_refset_for_description_df(desc_id)
#         print(f"\nLanguage refset entries for description {desc_id}:")
#         if not lang_refset.empty:
#             print(lang_refset[['refsetId', 'acceptabilityId', 'active']].to_string(index=False))
    
#     # Memory usage
#     print(f"\nMemory usage:")
#     for component, usage in reader.get_memory_usage().items():
#         print(f"  {component}: {usage}")


# if __name__ == "__main__":
#     main()