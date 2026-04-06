"""
Genie Space Creator Module (Enhanced with column_configs + rich descriptions)
Creates enriched Databricks Genie spaces with comprehensive configuration
including column-level metadata for entity matching and format assistance.
"""

from typing import Dict, List, Any
from databricks.sdk import WorkspaceClient
import json
import uuid
import re


# Column name patterns to auto-exclude (internal/system columns)
_EXCLUDE_PATTERNS = re.compile(
    r'^(_|__)|_(etl|dwh|audit|meta|sys|internal)|'
    r'^(created_at|updated_at|modified_at|inserted_at|deleted_at|_rescued_data)$',
    re.IGNORECASE
)

# Types eligible for format_assistance
_FORMAT_ASSIST_TYPES = {'DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ',
                        'STRING', 'VARCHAR', 'CHAR', 'BOOLEAN'}

# Roles eligible for entity_matching
_ENTITY_MATCH_ROLES = {'categorical'}


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
        genie_description: str = None,
        column_profiles: Dict = None,
    ) -> Dict[str, Any]:
        """
        Create enriched Genie space with comprehensive configuration.

        Args:
            config: LLM-generated configuration
            metric_views: List of created metric view names
            business_context: Business context for instructions
            genie_space_name: Display name for the Genie space
            genie_description: Descriptive text for the Genie space
            column_profiles: Deep scan profiles {table: {col: {role, cardinality, ...}}}
        """
        print("\u2728 Creating Genie Space...")

        # --- Warehouse selection (auto-detect best available) ---
        warehouses = list(self.workspace_client.warehouses.list())
        if not warehouses:
            raise ValueError("No SQL warehouses available in this workspace.")

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

        # --- Table identifiers (metric-view-first strategy) ---
        GENIE_SPACE_MAX_ITEMS = 30

        metric_view_ids = [f"{self.full_schema}.{mv}" for mv in (metric_views or [])]

        if self.table_fq_map:
            raw_table_ids = [self.table_fq_map[t] for t in config.get('relevant_tables', []) if t in self.table_fq_map]
        else:
            raw_table_ids = [f"{self.full_schema}.{t}" for t in config.get('relevant_tables', [])]

        if len(metric_view_ids) >= GENIE_SPACE_MAX_ITEMS:
            metric_view_ids = self._rank_metric_views(metric_view_ids, config, GENIE_SPACE_MAX_ITEMS)
            selected_tables = []
            print(f"   \u26a0\ufe0f  Metric views exceed limit. Selected top {len(metric_view_ids)} (0 raw tables).")
        else:
            remaining_slots = GENIE_SPACE_MAX_ITEMS - len(metric_view_ids)

            covered_tables = set()
            for mv in (metric_views or []):
                base = re.sub(r'^metrics_', '', mv)
                base = re.sub(r'_v\d+$', '', base)
                covered_tables.add(base)

            uncovered = [t for t in raw_table_ids if t.split('.')[-1] not in covered_tables]
            covered = [t for t in raw_table_ids if t.split('.')[-1] in covered_tables]

            join_tables = {j.get('left_table') for j in config.get('joins', [])} | \
                          {j.get('right_table') for j in config.get('joins', [])}

            uncovered_ranked = sorted(uncovered, key=lambda t: (t.split('.')[-1] in join_tables,), reverse=True)
            covered_ranked = sorted(covered, key=lambda t: (t.split('.')[-1] in join_tables,), reverse=True)
            candidate_tables = uncovered_ranked + covered_ranked
            selected_tables = candidate_tables[:remaining_slots]

            total_original = len(metric_view_ids) + len(raw_table_ids)
            if total_original > GENIE_SPACE_MAX_ITEMS:
                pruned = total_original - (len(metric_view_ids) + len(selected_tables))
                print(f"   \u26a0\ufe0f  Pruned {pruned} lower-priority tables.")

        table_identifiers = sorted(metric_view_ids + selected_tables)
        print(f"   Built Genie Space with {len(metric_view_ids)} metric views + {len(selected_tables)} tables = {len(table_identifiers)} items")

        # --- Build serialized space ---
        serialized_space = self._build_serialized_space(
            config=config,
            table_identifiers=table_identifiers,
            business_context=business_context,
            metric_views=metric_views,
            column_profiles=column_profiles,
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

            print(f"\u2705 Created Genie Space: {display_name}")
            print(f"   Space ID: {space_id}")
            print(f"   URL: {url}")
            print(f"   Tables: {len(table_identifiers)}")

            sq_count = len(serialized_space.get('config', {}).get('sample_questions', []))
            sql_count = len(serialized_space.get('instructions', {}).get('example_question_sqls', []))
            meas_count = len(serialized_space.get('instructions', {}).get('sql_snippets', {}).get('measures', []))
            filt_count = len(serialized_space.get('instructions', {}).get('sql_snippets', {}).get('filters', []))
            expr_count = len(serialized_space.get('instructions', {}).get('sql_snippets', {}).get('expressions', []))
            join_count = len(serialized_space.get('instructions', {}).get('join_specs', []))
            bench_count = len(serialized_space.get('benchmarks', {}).get('questions', []))

            # Count column_configs across all tables
            cc_count = 0
            for ds_type in ('tables', 'metric_views'):
                for entry in serialized_space.get('data_sources', {}).get(ds_type, []):
                    cc_count += len(entry.get('column_configs', []))

            print(f"   Sample Questions: {sq_count}")
            print(f"   SQL Expressions: {sql_count}")
            print(f"   Measures: {meas_count} | Filters: {filt_count} | Expressions: {expr_count}")
            print(f"   Join Definitions: {join_count}")
            print(f"   Column Configs: {cc_count}")
            print(f"   Benchmarks: {bench_count}")

            return {
                'genie_space_id': space_id,
                'url': url,
                'display_name': display_name,
                'table_identifiers': table_identifiers
            }

        except Exception as e:
            print(f"\u274c Failed to create Genie space: {str(e)}")
            return {
                'genie_space_id': None,
                'url': None,
                'display_name': display_name,
                'table_identifiers': table_identifiers,
                'instructions': self._build_instructions_text(config, business_context)
            }

    def _rank_metric_views(self, metric_view_ids: list, config: dict, max_items: int) -> list:
        """Rank metric views by richness and pick the top max_items."""
        import re as _re

        measures_by_table = {}
        for m in config.get('measures', config.get('metrics', [])):
            table = m.get('table', '')
            if table:
                measures_by_table[table] = measures_by_table.get(table, 0) + 1

        joins_by_table = {}
        for j in config.get('joins', []):
            for side in ('left_table', 'right_table'):
                t = j.get(side, '')
                if t:
                    joins_by_table[t] = joins_by_table.get(t, 0) + 1

        def _score(mv_id: str) -> tuple:
            mv_name = mv_id.split('.')[-1]
            base = _re.sub(r'^metrics_', '', mv_name)
            base = _re.sub(r'_v\d+$', '', base)
            return (
                measures_by_table.get(base, 0),
                joins_by_table.get(base, 0),
            )

        ranked = sorted(metric_view_ids, key=_score, reverse=True)
        return ranked[:max_items]

    # ─── Column Configs Builder ─────────────────────────────────────────

    def _build_column_configs(
        self,
        table_name: str,
        column_profiles: Dict,
        column_descriptions: Dict,
    ) -> List[Dict]:
        """Build column_configs for a single table from deep scan profiles + LLM descriptions.

        Per Genie API spec, each column_config can have:
          - column_name (required)
          - description: [str]
          - synonyms: [str]
          - enable_format_assistance: bool
          - enable_entity_matching: bool
          - exclude: bool
        """
        short_name = table_name.split('.')[-1]
        profiles = column_profiles.get(short_name, {})

        if not profiles:
            return []

        configs = []
        for col_name, prof in profiles.items():
            role = prof.get('role', 'unknown')
            data_type = prof.get('data_type', '').upper().split('(')[0].strip()

            # Should this column be excluded?
            if _EXCLUDE_PATTERNS.search(col_name) or role == 'complex':
                configs.append({
                    "column_name": col_name,
                    "exclude": True
                })
                continue

            cc = {"column_name": col_name}

            # Description from LLM semantics
            desc_key = f"{short_name}.{col_name}"
            desc = column_descriptions.get(desc_key, '')
            if desc:
                cc["description"] = [desc]

            # Synonyms: generate human-friendly aliases
            synonyms = self._generate_column_synonyms(col_name, desc)
            if synonyms:
                cc["synonyms"] = synonyms

            # Format assistance: ALWAYS enable for non-excluded columns.
            # Per Genie API docs, format assistance is auto-enabled on tables,
            # but creating a column_config entry WITHOUT this field disables it.
            cc["enable_format_assistance"] = True

            # Entity matching: enabled for low-cardinality categorical columns
            cardinality = prof.get('cardinality') or 0
            if (role in _ENTITY_MATCH_ROLES
                    and 0 < cardinality <= 1024):
                # Check type if available, otherwise rely on role classification
                if not data_type or data_type in ('STRING', 'VARCHAR', 'CHAR'):
                    cc["enable_entity_matching"] = True

            configs.append(cc)

        # Genie API requires column_configs sorted by column_name
        configs.sort(key=lambda c: c.get('column_name', ''))
        return configs

    def _generate_column_synonyms(self, col_name: str, description: str) -> List[str]:
        """Generate business-friendly synonyms from a column name."""
        synonyms = set()

        # Convert snake_case to readable form
        readable = col_name.replace('_', ' ').strip()
        if readable != col_name:
            synonyms.add(readable)

        # Common abbreviation expansions
        expansions = {
            'qty': 'quantity', 'amt': 'amount', 'pct': 'percentage',
            'num': 'number', 'dt': 'date', 'ts': 'timestamp',
            'desc': 'description', 'cat': 'category', 'dept': 'department',
            'org': 'organization', 'acct': 'account', 'txn': 'transaction',
            'pax': 'passenger', 'flt': 'flight', 'arr': 'arrival',
            'dep': 'departure', 'dest': 'destination', 'src': 'source',
        }
        parts = col_name.lower().split('_')
        expanded = [expansions.get(p, p) for p in parts]
        expanded_name = ' '.join(expanded)
        if expanded_name != readable:
            synonyms.add(expanded_name)

        # Extract key terms from description
        if description:
            # Take key noun phrases (simple heuristic)
            words = description.lower().split()
            if len(words) >= 2:
                # Take first 3-word phrase that isn't stop words
                stop = {'the', 'a', 'an', 'of', 'for', 'in', 'to', 'and', 'or', 'is', 'with'}
                key_words = [w for w in words if w not in stop and len(w) > 2]
                if key_words:
                    synonyms.add(' '.join(key_words[:3]))

        # Remove the original column name if present
        synonyms.discard(col_name)
        return list(synonyms)[:5]  # Genie API recommends max 5

    # ─── Build Serialized Space ───────────────────────────────────────

    def _build_serialized_space(
        self,
        config: Dict[str, Any],
        table_identifiers: List[str],
        business_context: str,
        metric_views: List[str] = None,
        column_profiles: Dict = None,
    ) -> Dict[str, Any]:
        """Build the serialized space configuration object."""

        def gen_uuid():
            return uuid.uuid4().hex

        column_profiles = column_profiles or {}
        column_descriptions = config.get('column_descriptions', {})

        # --- Sample Questions ---
        sample_questions = []
        for question in config.get('sample_questions', []):
            q_text = question.get('question', '') if isinstance(question, dict) else str(question)
            if q_text:
                sample_questions.append({
                    "id": gen_uuid(),
                    "question": [q_text]
                })
        sample_questions = sorted(sample_questions, key=lambda x: x['id'])

        # --- Table data sources with column_configs ---
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

            # Add column_configs if we have profile data
            col_configs = self._build_column_configs(
                table_id, column_profiles, column_descriptions
            )
            if col_configs:
                entry["column_configs"] = col_configs

            if table_name in metric_view_names:
                metric_views_config.append(entry)
            else:
                tables_config.append(entry)

        # --- Instructions ---
        text_instructions = [{
            "id": gen_uuid(),
            "content": [self._build_instructions_text(config, business_context)]
        }]

        # --- Example SQL queries ---
        example_sqls = []
        for question in config.get('sample_questions', []):
            if isinstance(question, dict):
                q_text = question.get('question', '')
                q_sql = question.get('sql', '')
                if q_text and q_sql:
                    eq = {
                        "id": gen_uuid(),
                        "question": [q_text],
                        "sql": [q_sql]
                    }
                    # Add usage_guidance if present
                    ug = question.get('usage_guidance', '')
                    if ug:
                        eq["usage_guidance"] = [ug]
                    # Add parameters if present
                    params = question.get('parameters', [])
                    if params:
                        eq["parameters"] = params
                    example_sqls.append(eq)
        example_sqls = sorted(example_sqls, key=lambda x: x['id'])

        # --- Measures with comment + instruction ---
        measures = []
        seen_display_names = set()
        for metric in config.get('measures', []):
            display_name = metric.get('display_name', metric['name'])
            # Deduplicate by display_name (case-insensitive)
            if display_name.lower() in seen_display_names:
                continue
            seen_display_names.add(display_name.lower())
            # Backtick-quote column references with spaces in formula SQL
            formula = metric.get('formula', '')
            formula = self._backtick_quote_sql(formula)
            measure = {
                "id": gen_uuid(),
                "alias": metric['name'],
                "sql": [formula],
                "display_name": display_name
            }
            if metric.get('synonyms'):
                measure['synonyms'] = metric['synonyms'][:5]
            if metric.get('description'):
                measure['comment'] = [metric['description']]
            if metric.get('instruction'):
                measure['instruction'] = [metric['instruction']]
            measures.append(measure)
        measures = sorted(measures, key=lambda x: x['id'])

        # --- Filters ---
        filters = []
        for f in config.get('filters', []):
            filt = {
                "id": gen_uuid(),
                "sql": [f.get('sql', '')],
                "display_name": f.get('display_name', ''),
            }
            if f.get('synonyms'):
                filt['synonyms'] = f['synonyms'][:5]
            if f.get('comment'):
                filt['comment'] = [f['comment']]
            if f.get('instruction'):
                filt['instruction'] = [f['instruction']]
            filters.append(filt)
        filters = sorted(filters, key=lambda x: x['id'])

        # --- Expressions ---
        expressions = []
        for expr in config.get('expressions', []):
            e = {
                "id": gen_uuid(),
                "alias": expr.get('alias', ''),
                "sql": [expr.get('sql', '')],
                "display_name": expr.get('display_name', ''),
            }
            if expr.get('synonyms'):
                e['synonyms'] = expr['synonyms'][:5]
            if expr.get('comment'):
                e['comment'] = [expr['comment']]
            if expr.get('instruction'):
                e['instruction'] = [expr['instruction']]
            expressions.append(e)
        expressions = sorted(expressions, key=lambda x: x['id'])

        # --- Join Specs with comment + instruction ---
        join_specs = self._build_join_specs(config.get('joins', []))

        # --- Build data_sources ---
        data_sources = {"tables": tables_config}
        if metric_views_config:
            data_sources["metric_views"] = metric_views_config

        # --- Build sql_snippets ---
        sql_snippets = {"measures": measures}
        if filters:
            sql_snippets["filters"] = filters
        if expressions:
            sql_snippets["expressions"] = expressions

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
                "sql_snippets": sql_snippets
            }
        }

        # --- Benchmarks ---
        benchmarks = config.get('benchmarks', [])
        if benchmarks:
            result["benchmarks"] = {
                "questions": benchmarks
            }

        return result

    def _build_join_specs(self, joins: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert LLM-generated join definitions to Genie Space API join_specs format."""
        join_specs = []

        for join in joins:
            left_table = join.get('left_table', '')
            right_table = join.get('right_table', '')
            condition = join.get('condition', '')

            if not left_table or not right_table or not condition:
                continue

            if self.table_fq_map:
                left_fq = self.table_fq_map.get(left_table, f"{self.full_schema}.{left_table}" if '.' not in left_table else left_table)
                right_fq = self.table_fq_map.get(right_table, f"{self.full_schema}.{right_table}" if '.' not in right_table else right_table)
            else:
                left_fq = f"{self.full_schema}.{left_table}" if '.' not in left_table else left_table
                right_fq = f"{self.full_schema}.{right_table}" if '.' not in right_table else right_table

            left_alias = left_table.split('.')[-1]
            right_alias = right_table.split('.')[-1]

            quoted_condition = self._backtick_quote_condition(condition)

            relationship_type = join.get('relationship_type', '').upper()
            if not relationship_type or relationship_type not in ('MANY_TO_ONE', 'ONE_TO_MANY', 'ONE_TO_ONE'):
                join_type = join.get('join_type', 'INNER').upper()
                if join_type in ('LEFT', 'LEFT OUTER'):
                    relationship_type = 'MANY_TO_ONE'
                elif join_type in ('RIGHT', 'RIGHT OUTER'):
                    relationship_type = 'ONE_TO_MANY'
                else:
                    relationship_type = 'MANY_TO_ONE'

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

            # Add comment and instruction if present
            comment = join.get('comment', '')
            if comment:
                join_spec["comment"] = [comment]
            instruction = join.get('instruction', '')
            if instruction:
                join_spec["instruction"] = [instruction]

            join_specs.append(join_spec)

        join_specs = sorted(join_specs, key=lambda x: x['id'])

        if join_specs:
            print(f"   Built {len(join_specs)} join specifications for Genie Space")

        return join_specs

    def _backtick_quote_condition(self, condition: str) -> str:
        """
        Convert a join condition to use backtick-quoted identifiers.

        Handles both simple identifiers (table.column) and identifiers with
        spaces (table.[Column Name] or table.Column Name With Spaces).

        Output always uses backtick notation: `table`.`column`
        """
        # First, convert any bracket notation [Column Name] to backtick notation
        condition = re.sub(
            r'(\w+)\.\[([^\]]+)\]',
            r'`\1`.`\2`',
            condition
        )

        # Then handle simple dot-separated identifiers (no spaces in names)
        def quote_simple_identifier(match):
            parts = match.group(0).split('.')
            return '.'.join(f'`{p.strip("`")}`' for p in parts)

        condition = re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)\b', quote_simple_identifier, condition)
        return condition

    @staticmethod
    def _backtick_quote_sql(sql: str) -> str:
        """
        Add backtick quoting to column references with spaces in SQL expressions.

        Converts: table.Column Name With Spaces  →  `table`.`Column Name With Spaces`
        Preserves already-quoted references.
        """
        # Match table_alias.UnquotedColumnWithSpaces (where column starts with uppercase
        # and contains spaces, ending before SQL keywords/operators)
        pattern = r'(\b\w+)\.([A-Za-z][A-Za-z0-9_ ]+?)(?=\s*(?:[)<>,=!*+\-/]|$|\bIS\b|\bELSE\b|\bTHEN\b|\bWHEN\b|\bAND\b|\bOR\b|\bEND\b|\bIN\b|\bAS\b|\bNOT\b|\bLIKE\b|\bILIKE\b|\bWITHIN\b|\bBETWEEN\b))'

        def replacer(m):
            alias = m.group(1)
            col = m.group(2).rstrip()
            if ' ' in col:
                return f'`{alias}`.`{col}`'
            return m.group(0)

        return re.sub(pattern, replacer, sql)

    def _build_instructions_text(self, config: Dict[str, Any], business_context: str) -> str:
        """Use LLM-generated business instructions, with fallback."""
        if 'business_instructions' in config and config['business_instructions']:
            return config['business_instructions']

        print("\u26a0\ufe0f  No LLM-generated instructions found, using fallback")
        instructions = f"{business_context}\n\n### Key Measures:\n"
        for m in config.get('measures', [])[:10]:
            instructions += f"- {m.get('display_name', m['name'])}: {m.get('description', '')}\n"
        instructions += "\n### Dimensions:\n"
        for d in config.get('dimensions', [])[:10]:
            instructions += f"- {d['name']}: {d.get('description', '')}\n"
        return instructions

    def _get_workspace_url(self) -> str:
        return self.workspace_client.config.host
