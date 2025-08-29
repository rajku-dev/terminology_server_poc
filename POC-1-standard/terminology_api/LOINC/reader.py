#!/usr/bin/env python3
"""
LOINC Data Reader
Efficiently reads LOINC CSV files and converts them to Python objects for ES indexing.
Optimized for memory usage and performance.
"""

import csv
from typing import Dict, Optional, Iterator, Set
from dataclasses import dataclass
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class LoincConcept:
    """Main LOINC concept from Loinc.csv"""
    code: str
    display: str
    component: Optional[str] = None
    property_: Optional[str] = None  # 'property' is reserved keyword
    time_aspect: Optional[str] = None
    system: Optional[str] = None
    scale_type: Optional[str] = None
    method_type: Optional[str] = None
    long_common_name: Optional[str] = None
    shortname: Optional[str] = None
    class_: Optional[str] = None  # 'class' is reserved keyword
    status: Optional[str] = None

@dataclass
class LoincPart:
    """LOINC Part from Part.csv"""
    code: str
    name: str
    display_name: str
    type_name: str
    status: Optional[str] = None

@dataclass
class LoincAnswer:
    """LOINC Answer from AnswerFile.csv"""
    code: str
    display: str
    answer_list_id: Optional[str] = None
    sequence_number: Optional[int] = None

@dataclass
class LoincHierarchy:
    """Hierarchy relationship"""
    parent: str
    child: str
    relationship_type: str = "is-a"
    immediate_parent: bool = True

class LoincReader:
    """
    Efficiently reads LOINC CSV files with memory optimization.
    Processes files in chunks to handle large datasets.
    """
    
    def __init__(self, loinc_data_path: str, chunk_size: int = 10000):
        self.loinc_data_path = Path(loinc_data_path)
        self.chunk_size = chunk_size
        
        # Validate paths
        self._validate_paths()
        
    def _validate_paths(self):
        """Validate that required LOINC files exist"""
        required_files = {
            'loinc_table': 'LoincTable/Loinc.csv',
            'part_file': 'AccessoryFiles/PartFile/Part.csv', 
            'answer_file': 'AccessoryFiles/AnswerFile/AnswerList.csv',
            'hierarchy': 'AccessoryFiles/ComponentHierarchyBySystem/ComponentHierarchyBySystem.csv'
        }
        
        missing_files = []
        for file_type, file_path in required_files.items():
            full_path = self.loinc_data_path / file_path
            if not full_path.exists():
                missing_files.append(f"{file_type}: {full_path}")
        
        if missing_files:
            raise FileNotFoundError(f"Missing LOINC files:\n" + "\n".join(missing_files))
    
    def _clean_header(self, header: str) -> str:
        """Clean CSV header by removing whitespace and normalizing"""
        return header.strip().replace(' ', '_').upper()
    
    def _safe_get(self, row: Dict, key: str, default: str = None) -> Optional[str]:
        """Safely get value from CSV row, handling empty strings"""
        value = row.get(key, default)
        return value if value and value.strip() else default
    
    def read_main_concepts(self) -> Iterator[LoincConcept]:
        """
        Read main LOINC concepts from Loinc.csv
        Yields concepts in chunks to manage memory
        """
        loinc_file = self.loinc_data_path / 'LoincTable' / 'Loinc.csv'
        logger.info(f"Reading main concepts from {loinc_file}")
        
        count = 0
        with open(loinc_file, 'r', encoding='utf-8-sig', newline='') as f:
            # Read header and normalize
            reader = csv.DictReader(f)
            reader.fieldnames = [self._clean_header(h) for h in reader.fieldnames]
            
            for row in reader:
                count += 1
                
                # Extract core fields
                concept = LoincConcept(
                    code=self._safe_get(row, 'LOINC_NUM'),
                    display=self._safe_get(row, 'COMPONENT') or self._safe_get(row, 'LONG_COMMON_NAME'),
                    component=self._safe_get(row, 'COMPONENT'),
                    property_=self._safe_get(row, 'PROPERTY'),
                    time_aspect=self._safe_get(row, 'TIME_ASPCT'),
                    system=self._safe_get(row, 'SYSTEM'),
                    scale_type=self._safe_get(row, 'SCALE_TYP'),
                    method_type=self._safe_get(row, 'METHOD_TYP'),
                    long_common_name=self._safe_get(row, 'LONG_COMMON_NAME'),
                    shortname=self._safe_get(row, 'SHORTNAME'),
                    class_=self._safe_get(row, 'CLASS'),
                    status=self._safe_get(row, 'STATUS')
                )
                
                # Skip invalid entries
                if not concept.code:
                    continue
                    
                yield concept
                
                if count % self.chunk_size == 0:
                    logger.info(f"Processed {count} main concepts")
        
        logger.info(f"Completed reading {count} main concepts")
    
    def read_parts(self) -> Iterator[LoincPart]:
        """Read LOINC parts from Part.csv"""
        part_file = self.loinc_data_path / 'AccessoryFiles' / 'PartFile' / 'Part.csv'
        logger.info(f"Reading parts from {part_file}")
        
        count = 0
        with open(part_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [self._clean_header(h) for h in reader.fieldnames]
            
            for row in reader:
                count += 1
                
                part = LoincPart(
                    code=self._safe_get(row, 'PARTNUMBER'),
                    name=self._safe_get(row, 'PARTNAME'),
                    display_name=self._safe_get(row, 'PARTDISPLAYNAME'),
                    type_name=self._safe_get(row, 'PARTTYPENAME'),
                    status=self._safe_get(row, 'STATUS')
                )
                
                # Skip invalid entries
                if not part.code or not part.name:
                    continue
                
                yield part
                
                if count % self.chunk_size == 0:
                    logger.info(f"Processed {count} parts")
        
        logger.info(f"Completed reading {count} parts")
    
    def read_answers(self) -> Iterator[LoincAnswer]:
        """Read LOINC answers from AnswerList.csv"""
        answer_file = self.loinc_data_path / 'AccessoryFiles' / 'AnswerFile' / 'AnswerList.csv'
        
        # Handle missing answer file gracefully
        if not answer_file.exists():
            logger.warning(f"Answer file not found: {answer_file}")
            return
            
        logger.info(f"Reading answers from {answer_file}")
        
        count = 0
        with open(answer_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [self._clean_header(h) for h in reader.fieldnames]
            
            for row in reader:
                count += 1
                
                answer = LoincAnswer(
                    code=self._safe_get(row, 'ANSWERSTRINGID'),
                    display=self._safe_get(row, 'DISPLAYTEXT'),
                    answer_list_id=self._safe_get(row, 'ANSWERLISTID'),
                    sequence_number=int(self._safe_get(row, 'SEQUENCENUMBER', '0')) or None
                )
                
                # Skip invalid entries
                if not answer.code or not answer.display:
                    continue
                
                yield answer
                
                if count % self.chunk_size == 0:
                    logger.info(f"Processed {count} answers")
        
        logger.info(f"Completed reading {count} answers")
    
    def read_hierarchies(self) -> Iterator[LoincHierarchy]:
        """Read hierarchical relationships from ComponentHierarchyBySystem.csv"""
        hierarchy_file = self.loinc_data_path / 'AccessoryFiles' / 'ComponentHierarchyBySystem'/ 'ComponentHierarchyBySystem.csv'
        
        if not hierarchy_file.exists():
            logger.warning(f"Hierarchy file not found: {hierarchy_file}")
            return
            
        logger.info(f"Reading hierarchies from {hierarchy_file}")
        
        count = 0
        with open(hierarchy_file, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [self._clean_header(h) for h in reader.fieldnames]
            
            for row in reader:
                count += 1
                
                parent = self._safe_get(row, 'IMMEDIATE_PARENT')
                child = self._safe_get(row, 'CODE')
                
                # Skip root entries (no parent)
                if not parent or not child or parent == child:
                    continue
                
                hierarchy = LoincHierarchy(
                    parent=parent,
                    child=child,
                    relationship_type="is-a",
                    immediate_parent=True
                )
                
                yield hierarchy
                
                if count % self.chunk_size == 0:
                    logger.info(f"Processed {count} hierarchy relationships")
        
        logger.info(f"Completed reading {count} hierarchy relationships")
    
    def build_part_usage_map(self) -> Dict[str, Set[str]]:
        """
        Build a map of which main LOINC codes use each part.
        Returns: {part_code: {set of main_codes}}
        """
        logger.info("Building part usage map...")
        
        part_usage = {}
        count = 0
        
        for concept in self.read_main_concepts():
            count += 1
            
            # Check all component fields for LP codes
            component_fields = [
                concept.component, concept.property_, concept.time_aspect,
                concept.system, concept.scale_type, concept.method_type
            ]
            
            for component in component_fields:
                if component and component.startswith('LP'):
                    if component not in part_usage:
                        part_usage[component] = set()
                    part_usage[component].add(concept.code)
        
        logger.info(f"Built part usage map for {count} concepts, {len(part_usage)} parts")
        return part_usage
