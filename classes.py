from dataclasses import dataclass, field
from typing import List, Optional, Dict

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
