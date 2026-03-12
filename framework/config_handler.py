"""
Configuration Handler Module
Manages YAML configuration parsing and validation.
Supports Q&A questionnaire format for business context.
Now supports table_list (list of FQ table names) as primary input.
"""

import yaml
import os
from typing import Dict, Any, List


class ConfigHandler:
    """Handles configuration loading, validation, and business context assembly."""
    
    # Required Q&A fields
    QA_FIELDS = {
        'business_domain': 'What does your business do?',
        'data_description': 'What data is tracked in these tables?',
        'stakeholders_and_decisions': 'Who uses this data and what decisions do they make?',
    }
    OPTIONAL_QA_FIELDS = {
        'additional_context': 'Any other important context?',
    }
    
    def __init__(self, config_yaml: str = None, config_path: str = None):
        if config_path:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config file not found: {config_path}")
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f.read())
            self.config_path = config_path
        elif config_yaml:
            self.config = yaml.safe_load(config_yaml)
            self.config_path = None
        else:
            raise ValueError("Must provide either config_yaml or config_path")
        
        self._validate()
        self._derive_catalog_schema()
        self._assemble_business_context()
    
    def _validate(self):
        """Validate required fields."""
        # Must have either table_list OR (catalog + schema)
        has_table_list = 'table_list' in self.config and self.config['table_list']
        has_catalog_schema = 'catalog' in self.config and 'schema' in self.config
        
        if not has_table_list and not has_catalog_schema:
            raise ValueError("Missing required field: either 'table_list' or both 'catalog' and 'schema'")
        
        # Check for Q&A fields or legacy business_context
        has_qa = any(f in self.config for f in self.QA_FIELDS)
        has_legacy = 'business_context' in self.config
        
        if has_qa:
            missing = [
                f"  - {f}: {self.QA_FIELDS[f]}"
                for f in self.QA_FIELDS
                if f not in self.config or not str(self.config[f]).strip()
            ]
            if missing:
                raise ValueError("Missing required fields:\n" + "\n".join(missing))
        elif not has_legacy:
            raise ValueError(
                "Missing business context. Add these to config.yaml:\n"
                "  business_domain: What does your business do?\n"
                "  data_description: What data is tracked?\n"
                "  stakeholders_and_decisions: Who uses this data?"
            )
    
    def _derive_catalog_schema(self):
        """Derive catalog and schema from table_list if not explicitly set."""
        if 'table_list' in self.config and self.config['table_list']:
            table_list = self.config['table_list']
            first_parts = table_list[0].split('.')
            if len(first_parts) == 3:
                if 'catalog' not in self.config or not self.config.get('catalog'):
                    self.config['catalog'] = first_parts[0]
                if 'schema' not in self.config or not self.config.get('schema'):
                    self.config['schema'] = first_parts[1]
    
    def _assemble_business_context(self):
        """Assemble Q&A answers into a single business_context string for the LLM."""
        if any(f in self.config for f in self.QA_FIELDS):
            parts = []
            for field, question in {**self.QA_FIELDS, **self.OPTIONAL_QA_FIELDS}.items():
                value = str(self.config.get(field, '')).strip()
                if value:
                    parts.append(f"{question}\n{value}")
            self.config['business_context'] = "\n\n".join(parts)
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)
    
    def get_table_list(self) -> List[str]:
        """Get the list of fully qualified table names."""
        return self.config.get('table_list', [])
    
    def get_table_name_map(self) -> Dict[str, str]:
        """Build mapping from short table names to FQ names.
        Handles collisions by prefixing with schema name."""
        table_list = self.get_table_list()
        if not table_list:
            return {}
        
        # First pass: detect collisions
        short_counts = {}
        for fq in table_list:
            parts = fq.split('.')
            short = parts[-1] if parts else fq
            short_counts[short] = short_counts.get(short, 0) + 1
        
        collisions = {s for s, c in short_counts.items() if c > 1}
        
        # Second pass: build map
        short_to_fq = {}
        for fq in table_list:
            parts = fq.split('.')
            short = parts[-1] if parts else fq
            if short in collisions and len(parts) == 3:
                short = f"{parts[1]}_{parts[2]}"
            short_to_fq[short] = fq
        
        return short_to_fq
    
    def get_sample_questions(self) -> List[str]:
        questions = self.config.get('sample_questions', [])
        return [str(q).strip() for q in questions if str(q).strip()]
    
    def get_genie_space_name(self) -> str:
        name = self.config.get('genie_space_name', '')
        if name and str(name).strip():
            return str(name).strip()
        schema = self.config.get('schema', 'Data')
        return schema.replace('_', ' ').title() + ' Analytics'
    
    def get_warehouse_id(self) -> str:
        """Get warehouse ID from config, or empty string for auto-detect."""
        wid = self.config.get('warehouse_id', '')
        return str(wid).strip() if wid else ''
    
    def get_genie_description(self) -> str:
        """Generate a meaningful Genie space description from Q&A answers."""
        domain = str(self.config.get('business_domain', '')).strip()
        data = str(self.config.get('data_description', '')).strip()
        if domain and data:
            return f"{domain} Covers: {data}"
        elif domain:
            return domain
        return f"Analytics space for {self.config.get('catalog')}.{self.config.get('schema')}"
    
    def get_full_schema_name(self) -> str:
        return f"{self.config['catalog']}.{self.config['schema']}"
