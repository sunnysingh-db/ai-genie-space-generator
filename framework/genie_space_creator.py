"""
Genie Space Creator Module
Creates enriched Databricks Genie spaces with comprehensive configuration.
"""

from typing import Dict, List, Any
from databricks.sdk import WorkspaceClient
import json
import uuid


class GenieSpaceCreator:
    """Creates and configures Databricks Genie spaces."""
    
    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        self.workspace_client = WorkspaceClient()
    
    def create_genie_space(
        self, 
        config: Dict[str, Any], 
        metric_views: List[str],
        business_context: str,
        genie_space_name: str = None,

        genie_description: str = None
    ) -> Dict[str, Any]:
        """
        Create enriched Genie space with comprehensive configuration.
        
        Args:
            config: LLM-generated configuration (measures, dimensions, joins, sample_questions)
            metric_views: List of created metric view names
            business_context: Business context for instructions
            genie_space_name: Display name for the Genie space
            genie_description: Descriptive text for the Genie space
        """
        print("✨ Creating Genie Space...")
        
        # --- Warehouse selection (auto-detect best available) ---
        warehouses = list(self.workspace_client.warehouses.list())
        if not warehouses:
            raise ValueError("No SQL warehouses available in this workspace.")
        
        # Priority: running shared/serverless > running any > stopped shared/serverless > first available
        def _score_warehouse(w):
            name_lower = w.name.lower()
            is_running = 1 if str(w.state) == 'State.RUNNING' else 0
            is_shared = 1 if 'shared' in name_lower else 0
            is_serverless = 1 if 'serverless' in name_lower else 0
            is_starter = 1 if 'starter' in name_lower else 0
            return (is_running, is_shared + is_serverless + is_starter)
        
        chosen = max(warehouses, key=_score_warehouse)
        warehouse_id = chosen.id
        print(f"   Warehouse: {chosen.name} ({warehouse_id})")
        
        # --- Display name ---
        display_name = genie_space_name.strip() if genie_space_name else self.schema.replace('_', ' ').title() + ' Analytics'
        
        # --- Description ---
        description = genie_description if genie_description else f"Analytics space for {self.full_schema}"
        
        # --- Table identifiers ---
        table_identifiers = []
        for table in config.get('relevant_tables', []):
            table_identifiers.append(f"{self.full_schema}.{table}")
        if metric_views:
            for view_name in metric_views:
                table_identifiers.append(f"{self.full_schema}.{view_name}")
        table_identifiers = sorted(table_identifiers)
        
        # --- Build serialized space ---
        serialized_space = self._build_serialized_space(
            config=config,
            table_identifiers=table_identifiers,
            business_context=business_context
        )
        
        try:
            payload = {
                "title": display_name,
                "description": description,
                "serialized_space": json.dumps(serialized_space),
                "warehouse_id": warehouse_id
            }
            
            response = self.workspace_client.api_client.do(
                method="POST",
                path="/api/2.0/genie/spaces",
                body=payload
            )
            
            space_id = response.get('space_id')
            url = f"{self._get_workspace_url()}/genie/rooms/{space_id}"
            
            print(f"✅ Created Genie Space: {display_name}")
            print(f"   Space ID: {space_id}")
            print(f"   URL: {url}")
            print(f"   Tables: {len(table_identifiers)}")
            
            # Print what was populated
            sq_count = len(serialized_space.get('config', {}).get('sample_questions', []))
            sql_count = len(serialized_space.get('instructions', {}).get('example_question_sqls', []))
            meas_count = len(serialized_space.get('instructions', {}).get('sql_snippets', {}).get('measures', []))
            print(f"   Sample Questions: {sq_count}")
            print(f"   SQL Expressions: {sql_count}")
            print(f"   Measures: {meas_count}")
            
            return {
                'genie_space_id': space_id,
                'url': url,
                'display_name': display_name,
                'table_identifiers': table_identifiers
            }
        
        except Exception as e:
            print(f"❌ Failed to create Genie space: {str(e)}")
            return {
                'genie_space_id': None,
                'url': None,
                'display_name': display_name,
                'table_identifiers': table_identifiers,
                'instructions': self._build_instructions_text(config, business_context)
            }
    
    def _build_serialized_space(
        self, 
        config: Dict[str, Any], 
        table_identifiers: List[str],
        business_context: str
    ) -> Dict[str, Any]:
        """Build the serialized space configuration object."""
        
        def gen_uuid():
            return uuid.uuid4().hex
        
        # --- Sample Questions (ALL of them, no limit) ---
        sample_questions = []
        for question in config.get('sample_questions', []):
            q_text = question.get('question', '') if isinstance(question, dict) else str(question)
            if q_text:
                sample_questions.append({
                    "id": gen_uuid(),
                    "question": [q_text]
                })
        sample_questions = sorted(sample_questions, key=lambda x: x['id'])
        
        # --- Table data sources ---
        tables_config = []
        table_descriptions = config.get('table_descriptions', {})
        for table_id in table_identifiers:
            table_name = table_id.split('.')[-1]
            table_desc = table_descriptions.get(table_name, '')
            tables_config.append({
                "identifier": table_id,
                "description": [table_desc] if table_desc else []
            })
        
        # --- Instructions ---
        text_instructions = [{
            "id": gen_uuid(),
            "content": [self._build_instructions_text(config, business_context)]
        }]
        
        # --- Example SQL queries (ALL questions that have SQL) ---
        example_sqls = []
        for question in config.get('sample_questions', []):
            if isinstance(question, dict):
                q_text = question.get('question', '')
                q_sql = question.get('sql', '')
                if q_text and q_sql:  # Only include if both question and SQL exist
                    example_sqls.append({
                        "id": gen_uuid(),
                        "question": [q_text],
                        "sql": [q_sql]
                    })
        example_sqls = sorted(example_sqls, key=lambda x: x['id'])
        
        # --- Measures from LLM-generated measures (FIX: was 'metrics', now 'measures') ---
        measures = []
        for metric in config.get('measures', []):
            measure = {
                "id": gen_uuid(),
                "alias": metric['name'],
                "sql": [metric.get('formula', '')],
                "display_name": metric.get('display_name', metric['name'])
            }
            if metric.get('synonyms'):
                measure['synonyms'] = metric['synonyms'][:5]
            measures.append(measure)
        measures = sorted(measures, key=lambda x: x['id'])
        
        return {
            "version": 1,
            "config": {
                "sample_questions": sample_questions
            },
            "data_sources": {
                "tables": tables_config
            },
            "instructions": {
                "text_instructions": text_instructions,
                "example_question_sqls": example_sqls,
                "sql_snippets": {
                    "measures": measures
                }
            }
        }
    
    def _build_instructions_text(self, config: Dict[str, Any], business_context: str) -> str:
        """Use LLM-generated business instructions, with fallback."""
        if 'business_instructions' in config and config['business_instructions']:
            return config['business_instructions']
        
        # Fallback
        print("⚠️  No LLM-generated instructions found, using fallback")
        instructions = f"{business_context}\n\n### Key Measures:\n"
        for m in config.get('measures', [])[:10]:
            instructions += f"- {m.get('display_name', m['name'])}: {m.get('description', '')}\n"
        instructions += "\n### Dimensions:\n"
        for d in config.get('dimensions', [])[:10]:
            instructions += f"- {d['name']}: {d.get('description', '')}\n"
        return instructions
    
    def _get_workspace_url(self) -> str:
        return self.workspace_client.config.host
