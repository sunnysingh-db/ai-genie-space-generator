"""
Genie Space Creator Module
Creates enriched Databricks Genie spaces with comprehensive configuration.
"""

from typing import Dict, List, Any
from databricks.sdk import WorkspaceClient
import json
import uuid
import re


class GenieSpaceCreator:
    """Creates and configures Databricks Genie spaces."""
    
    def __init__(self, catalog: str, schema: str, table_fq_map: dict = None):
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        self.table_fq_map = table_fq_map or {}
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
        
        # --- Table identifiers (metric-view-first strategy for Genie Space limit) ---
        GENIE_SPACE_MAX_ITEMS = 30  # API hard limit for tables + metric_views
        
        # Build fully-qualified metric view identifiers
        metric_view_ids = [f"{self.full_schema}.{mv}" for mv in (metric_views or [])]
        
        # Build fully-qualified raw table identifiers
        if self.table_fq_map:
            raw_table_ids = [self.table_fq_map[t] for t in config.get('relevant_tables', []) if t in self.table_fq_map]
        else:
            raw_table_ids = [f"{self.full_schema}.{t}" for t in config.get('relevant_tables', [])]
        
        # --- PRIORITY: ALL metric views come first, tables fill remaining slots ---
        if len(metric_view_ids) >= GENIE_SPACE_MAX_ITEMS:
            # More metric views than the limit - smart-rank and pick top 30, 0 tables
            metric_view_ids = self._rank_metric_views(
                metric_view_ids, config, GENIE_SPACE_MAX_ITEMS
            )
            selected_tables = []
            print(f"   \u26a0\ufe0f  {len(metric_views or [])} metric views exceed Genie Space limit of {GENIE_SPACE_MAX_ITEMS}. "
                  f"Selected top {len(metric_view_ids)} metric views by measure count & join connectivity (0 raw tables).")
        else:
            # All metric views fit - fill remaining slots with raw tables
            remaining_slots = GENIE_SPACE_MAX_ITEMS - len(metric_view_ids)
            
            # Identify tables NOT already covered by a metric view (uncovered = higher priority)
            covered_tables = set()
            for mv in (metric_views or []):
                base = re.sub(r'^metrics_', '', mv)
                base = re.sub(r'_v\d+$', '', base)
                covered_tables.add(base)
            
            uncovered = [t for t in raw_table_ids if t.split('.')[-1] not in covered_tables]
            covered = [t for t in raw_table_ids if t.split('.')[-1] in covered_tables]
            
            # Score raw tables by join connectivity for tiebreaking
            join_tables = {j.get('left_table') for j in config.get('joins', [])} | \
                          {j.get('right_table') for j in config.get('joins', [])}
            
            # Uncovered tables first (they add unique data), then covered tables
            uncovered_ranked = sorted(uncovered, key=lambda t: (t.split('.')[-1] in join_tables,), reverse=True)
            covered_ranked = sorted(covered, key=lambda t: (t.split('.')[-1] in join_tables,), reverse=True)
            candidate_tables = uncovered_ranked + covered_ranked
            
            selected_tables = candidate_tables[:remaining_slots]
            
            total_original = len(metric_view_ids) + len(raw_table_ids)
            total_selected = len(metric_view_ids) + len(selected_tables)
            if total_original > GENIE_SPACE_MAX_ITEMS:
                pruned = total_original - total_selected
                print(f"   \u26a0\ufe0f  Genie Space limit is {GENIE_SPACE_MAX_ITEMS} items. "
                      f"Pruned {pruned} lower-priority tables "
                      f"(kept {len(metric_view_ids)} metric views + {len(selected_tables)} tables).")
        
        table_identifiers = sorted(metric_view_ids + selected_tables)
        print(f"   Built Genie Space with {len(metric_view_ids)} metric views + {len(selected_tables)} tables = {len(table_identifiers)} items")

        # --- Build serialized space ---
        serialized_space = self._build_serialized_space(
            config=config,
            table_identifiers=table_identifiers,
            business_context=business_context,
            metric_views=metric_views
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
            join_count = len(serialized_space.get('instructions', {}).get('join_specs', []))
            print(f"   Sample Questions: {sq_count}")
            print(f"   SQL Expressions: {sql_count}")
            print(f"   Measures: {meas_count}")
            print(f"   Join Definitions: {join_count}")
            
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
    
    def _rank_metric_views(
        self,
        metric_view_ids: list,
        config: dict,
        max_items: int
    ) -> list:
        """
        Rank metric views by richness and pick the top max_items.
        
        Scoring:
          1. Number of measures assigned to the base table (primary signal)
          2. Join connectivity — how many joins reference the base table (tiebreaker)
        """
        import re as _re
        
        # Count measures per table
        measures_by_table = {}
        for m in config.get('measures', config.get('metrics', [])):
            table = m.get('table', '')
            if table:
                measures_by_table[table] = measures_by_table.get(table, 0) + 1
        
        # Count joins per table
        joins_by_table = {}
        for j in config.get('joins', []):
            for side in ('left_table', 'right_table'):
                t = j.get(side, '')
                if t:
                    joins_by_table[t] = joins_by_table.get(t, 0) + 1
        
        def _score(mv_id: str) -> tuple:
            """Higher score = more valuable metric view."""
            mv_name = mv_id.split('.')[-1]
            # Derive base table from metric view name: metrics_<table> or metrics_<table>_vN
            base = _re.sub(r'^metrics_', '', mv_name)
            base = _re.sub(r'_v\d+$', '', base)
            return (
                measures_by_table.get(base, 0),   # primary: more measures = richer
                joins_by_table.get(base, 0),       # secondary: more joins = more connected
            )
        
        ranked = sorted(metric_view_ids, key=_score, reverse=True)
        return ranked[:max_items]
    
    def _build_serialized_space(
        self, 
        config: Dict[str, Any], 
        table_identifiers: List[str],
        business_context: str,
        metric_views: List[str] = None
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
        metric_views_config = []
        table_descriptions = config.get('table_descriptions', {})
        metric_view_names = set(metric_views or [])
        
        for table_id in table_identifiers:
            table_name = table_id.split('.')[-1]
            table_desc = table_descriptions.get(table_name, '')
            entry = {
                "identifier": table_id,
                "description": [table_desc] if table_desc else []
            }
            if table_name in metric_view_names:
                metric_views_config.append(entry)
            else:
                tables_config.append(entry)
        
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
        
        # --- Measures from LLM-generated measures ---
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
        
        # --- Join Specs from LLM-generated joins ---
        join_specs = self._build_join_specs(config.get('joins', []))
        
        # --- Build data_sources ---
        data_sources = {"tables": tables_config}
        if metric_views_config:
            data_sources["metric_views"] = metric_views_config
        
        result = {
            "version": 2,
            "config": {
                "sample_questions": sample_questions
            },
            "data_sources": data_sources,
            "instructions": {
                "text_instructions": text_instructions,
                "example_question_sqls": example_sqls,
                "join_specs": join_specs,
                "sql_snippets": {
                    "measures": measures
                }
            }
        }
        
        return result
    
    def _build_join_specs(self, joins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convert LLM-generated join definitions to Genie Space API join_specs format.
        
        LLM format:
            {"left_table": "t1", "right_table": "t2", "join_type": "INNER|LEFT|RIGHT",
             "condition": "t1.col = t2.col", "relationship_type": "MANY_TO_ONE|ONE_TO_MANY|ONE_TO_ONE"}
        
        API format:
            {"id": "hex", "left": {"identifier": "cat.sch.t1", "alias": "t1"},
             "right": {"identifier": "cat.sch.t2", "alias": "t2"},
             "sql": ["`t1`.`col` = `t2`.`col`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]}
        """
        join_specs = []
        
        for join in joins:
            left_table = join.get('left_table', '')
            right_table = join.get('right_table', '')
            condition = join.get('condition', '')
            
            if not left_table or not right_table or not condition:
                continue
            
            # Build fully qualified identifiers
            # Use table_fq_map for cross-schema support
            if self.table_fq_map:
                left_fq = self.table_fq_map.get(left_table, f"{self.full_schema}.{left_table}" if '.' not in left_table else left_table)
                right_fq = self.table_fq_map.get(right_table, f"{self.full_schema}.{right_table}" if '.' not in right_table else right_table)
            else:
                left_fq = f"{self.full_schema}.{left_table}" if '.' not in left_table else left_table
                right_fq = f"{self.full_schema}.{right_table}" if '.' not in right_table else right_table
            
            # Extract alias (last part of the table name)
            left_alias = left_table.split('.')[-1]
            right_alias = right_table.split('.')[-1]
            
            # Backtick-quote the condition using table aliases
            # The condition from LLM is like "table1.col = table2.col"
            # We need to convert to "`table1`.`col` = `table2`.`col`"
            quoted_condition = self._backtick_quote_condition(condition)
            
            # Map relationship type
            # LLM may provide relationship_type directly, or we infer from join_type
            relationship_type = join.get('relationship_type', '').upper()
            if not relationship_type or relationship_type not in ('MANY_TO_ONE', 'ONE_TO_MANY', 'ONE_TO_ONE'):
                # Infer from join_type: LEFT JOIN usually means many-to-one from left perspective
                join_type = join.get('join_type', 'INNER').upper()
                if join_type in ('LEFT', 'LEFT OUTER'):
                    relationship_type = 'MANY_TO_ONE'
                elif join_type in ('RIGHT', 'RIGHT OUTER'):
                    relationship_type = 'ONE_TO_MANY'
                else:
                    relationship_type = 'MANY_TO_ONE'  # Default
            
            join_spec = {
                "id": uuid.uuid4().hex,
                "left": {
                    "identifier": left_fq,
                    "alias": left_alias
                },
                "right": {
                    "identifier": right_fq,
                    "alias": right_alias
                },
                "sql": [
                    quoted_condition,
                    f"--rt=FROM_RELATIONSHIP_TYPE_{relationship_type}--"
                ]
            }
            join_specs.append(join_spec)
        
        # Sort by id — required by Genie Space API
        join_specs = sorted(join_specs, key=lambda x: x['id'])
        
        if join_specs:
            print(f"   Built {len(join_specs)} join specifications for Genie Space")
        
        return join_specs
    
    def _backtick_quote_condition(self, condition: str) -> str:
        """
        Convert a join condition to use backtick-quoted identifiers.
        
        Input:  "table1.column1 = table2.column2"
        Output: "`table1`.`column1` = `table2`.`column2`"
        """
        # Match patterns like table.column or schema.table.column
        # Replace each identifier segment with backtick-quoted version
        def quote_identifier(match):
            parts = match.group(0).split('.')
            return '.'.join(f'`{p.strip("`")}`' for p in parts)
        
        # Match word.word patterns (identifiers with dots)
        result = re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)\b', quote_identifier, condition)
        return result
    
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
