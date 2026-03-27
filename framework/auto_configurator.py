"""Auto-Configurator Module (Enhanced with Deep Scan + Data Model)
Accepts a list of fully qualified table names (catalog.schema.table),
scans their metadata, performs deep column profiling, classifies dimensions,
infers a data model, and auto-generates config.yaml values using AI.
Supports tables spanning multiple catalogs and schemas.
"""

import os
import io
import json
import re
import yaml
import time
import contextlib
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List, Optional


class AutoConfigurator:
    """Analyses a list of tables and auto-populates config.yaml using an LLM."""

    VALUE_FIELDS = [
        'genie_space_name', 'business_domain', 'data_description',
        'stakeholders_and_decisions', 'additional_context',
        'sample_questions'
    ]

    # Default model pool for distributing calls across endpoints
    DEFAULT_MODEL_POOL = [
        "databricks-claude-sonnet-4-6",
        "databricks-claude-opus-4-6",
        "databricks-claude-sonnet-4-5",
    ]

    # Column classification thresholds
    LOW_CARDINALITY_THRESHOLD = 30
    IDENTIFIER_PATTERNS = re.compile(
        r'(^id$|_id$|^pk$|_pk$|_key$|_fk$|_uuid$|_guid$)', re.IGNORECASE
    )
    TEMPORAL_TYPES = {'DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ'}
    NUMERIC_TYPES = {'INT', 'INTEGER', 'BIGINT', 'LONG', 'SMALLINT', 'TINYINT',
                     'FLOAT', 'DOUBLE', 'DECIMAL', 'DEC', 'NUMERIC'}

    def __init__(self, table_list: List[str], config_path: str,
                 llm_model: str = None, model_pool: list = None,
                 sample_rows: int = 5, max_workers: int = 10):
        self.table_list = table_list
        self.config_path = self._resolve_path(config_path)
        self.sample_rows = sample_rows
        self.max_workers = max_workers
        self.spark = SparkSession.builder.getOrCreate()

        # Parse table list into components
        self.parsed_tables = []
        for fq in table_list:
            parts = fq.split('.')
            if len(parts) != 3:
                raise ValueError(f"Table name must be fully qualified (catalog.schema.table): {fq}")
            self.parsed_tables.append({
                'catalog': parts[0], 'schema': parts[1],
                'table': parts[2], 'fq_name': fq
            })

        # Derive primary catalog.schema from first table (used for display)
        self.primary_catalog = self.parsed_tables[0]['catalog'] if self.parsed_tables else ''
        self.primary_schema = self.parsed_tables[0]['schema'] if self.parsed_tables else ''

        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.existing_config = yaml.safe_load(f.read()) or {}
        else:
            self.existing_config = {}

        self.existing_llm = self.existing_config.get('llm_model', 'databricks-claude-opus-4-6')
        self.model = llm_model or self.existing_llm
        self.model_pool = model_pool if model_pool else self.DEFAULT_MODEL_POOL
        if self.model and self.model not in self.model_pool:
            self.model_pool = [self.model] + self.model_pool

    # ─── Public API ────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """Execute the full auto-configure pipeline with deep scan."""
        print("\n" + "=" * 80)
        print("\U0001f9e0 AUTO-CONFIGURATOR WITH DEEP SCAN")
        print("=" * 80)

        # Phase 1: Basic metadata scan
        print("\n\U0001f50d Phase 1: Scanning table metadata...")
        t0 = time.time()
        all_tables, all_columns, samples = self._scan_metadata()
        table_names = [t['table_name'] for t in all_tables]
        struct_info = self._extract_struct_info(table_names)
        print(f"   Done ({time.time()-t0:.1f}s) — {len(all_tables)} tables, {len(all_columns)} columns")

        # Phase 2: Deep column profiling
        print("\n\U0001f4ca Phase 2: Deep column profiling...")
        t0 = time.time()
        column_profiles = self._deep_profile_columns(table_names, all_columns)
        print(f"   Done ({time.time()-t0:.1f}s) — {sum(len(v) for v in column_profiles.values())} column profiles")

        # Phase 3: Classify dimensions from profiles
        print("\n\U0001f3af Phase 3: Classifying dimensions...")
        t0 = time.time()
        dimensions = self._classify_dimensions(column_profiles, all_columns)
        print(f"   Done ({time.time()-t0:.1f}s) — {len(dimensions)} dimensions identified")

        # Phase 4: Infer data model (relationships + entity types)
        print("\n\U0001f517 Phase 4: Inferring data model...")
        t0 = time.time()
        data_model = self._infer_data_model(table_names, all_columns, column_profiles, samples)
        print(f"   Done ({time.time()-t0:.1f}s) — {len(data_model.get('relationships', []))} relationships")

        # Phase 5: LLM-generated business context
        print("\n\U0001f916 Phase 5: Generating business context with AI...")
        t0 = time.time()
        # Use existing profile method for backward compat with _build_summary
        profiles = self._profile_tables(table_names, all_columns)
        metadata_summary = self._build_summary(all_tables, all_columns, samples, struct_info, profiles)
        generated = self._call_llm(metadata_summary)
        print(f"   Done ({time.time()-t0:.1f}s)")

        # Phase 6: Write config with all new sections
        print("\n\U0001f4be Phase 6: Writing config.yaml...")
        updated = self._write_config(generated, column_profiles, dimensions, data_model)
        self._display_result(updated, dimensions, data_model)
        return updated

    # ─── Phase 1: Metadata Scan (unchanged) ────────────────────────────────

    def _scan_metadata(self):
        """Scan metadata for the specific tables in table_list."""
        schema_groups = {}
        for pt in self.parsed_tables:
            key = (pt['catalog'], pt['schema'])
            schema_groups.setdefault(key, []).append(pt['table'])

        all_tables = []
        all_columns = []

        for (cat, sch), table_names in schema_groups.items():
            qcat, qsch = self._quote(cat), self._quote(sch)
            names_sql = ', '.join(f"\'{t}\'" for t in table_names)

            try:
                tables_df = self.spark.sql(f"""
                    SELECT table_name, comment
                    FROM {qcat}.information_schema.tables
                    WHERE table_schema = '{sch}'
                      AND table_name IN ({names_sql})
                    ORDER BY table_name
                """)
                for row in tables_df.collect():
                    all_tables.append(row.asDict())
            except Exception as e:
                print(f"  \u26a0\ufe0f Could not query information_schema for {cat}.{sch}: {e}")
                for tname in table_names:
                    all_tables.append({'table_name': tname, 'comment': None})

            try:
                columns_df = self.spark.sql(f"""
                    SELECT table_name, column_name, data_type, comment
                    FROM {qcat}.information_schema.columns
                    WHERE table_schema = '{sch}'
                      AND table_name IN ({names_sql})
                    ORDER BY table_name, ordinal_position
                """)
                all_columns.extend([row.asDict() for row in columns_df.collect()])
            except Exception as e:
                print(f"  \u26a0\ufe0f Could not query columns for {cat}.{sch}: {e}")

        fq_lookup = {pt['table']: pt['fq_name'] for pt in self.parsed_tables}

        samples = {}
        def _sample(pt):
            try:
                fq = f"{self._quote(pt['catalog'])}.{self._quote(pt['schema'])}.{self._quote(pt['table'])}"
                rows = self.spark.table(fq).limit(self.sample_rows).collect()
                return pt['table'], [row.asDict() for row in rows], None
            except Exception as e:
                return pt['table'], [], str(e)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(_sample, pt): pt for pt in self.parsed_tables}
            for fut in as_completed(futures):
                name, data, err = fut.result()
                samples[name] = data

        return all_tables, all_columns, samples

    # ─── Phase 2: Deep Column Profiling ────────────────────────────────────

    MAX_PROFILE_SAMPLE = 50000  # rows sampled per table

    def _deep_profile_columns(self, table_names: list, all_columns: list) -> dict:
        """Profile every column in every table: cardinality, null%, min/max, top-N values.

        Returns:
            dict: {table_name: {column_name: {cardinality, null_pct, min, max,
                   top_values, data_type, role}}} 
        """
        fq_lookup = {
            pt['table']: f"{self._quote(pt['catalog'])}.{self._quote(pt['schema'])}.{self._quote(pt['table'])}"
            for pt in self.parsed_tables
        }

        cols_by_table = {}
        for c in all_columns:
            cols_by_table.setdefault(c['table_name'], []).append(c)

        all_profiles = {}

        def _profile_one_table(tname):
            tbl_cols = cols_by_table.get(tname, [])
            if not tbl_cols:
                return tname, {}
            fqn = fq_lookup.get(tname)
            if not fqn:
                return tname, {}

            # Build a single SQL query that profiles all columns at once
            exprs = [f"COUNT(*) AS `__row_count__`"]
            col_meta = []  # track (col_name, data_type, base_type)

            for c in tbl_cols:
                cn = c['column_name']
                dt = c['data_type'].upper()
                qcn = self._quote(cn)
                base_type = dt.split('(')[0].strip()  # DECIMAL(10,2) → DECIMAL

                # Skip complex types (STRUCT, ARRAY, MAP)
                if base_type in ('STRUCT', 'ARRAY', 'MAP', 'BINARY'):
                    col_meta.append((cn, dt, base_type, True))
                    continue

                col_meta.append((cn, dt, base_type, False))
                exprs.append(f"APPROX_COUNT_DISTINCT({qcn}) AS `acd__{cn}`")
                exprs.append(f"SUM(CASE WHEN {qcn} IS NULL THEN 1 ELSE 0 END) AS `null__{cn}`")

                if base_type in self.TEMPORAL_TYPES:
                    exprs.append(f"CAST(MIN({qcn}) AS STRING) AS `min__{cn}`")
                    exprs.append(f"CAST(MAX({qcn}) AS STRING) AS `max__{cn}`")
                elif base_type in self.NUMERIC_TYPES:
                    exprs.append(f"CAST(MIN({qcn}) AS STRING) AS `min__{cn}`")
                    exprs.append(f"CAST(MAX({qcn}) AS STRING) AS `max__{cn}`")

            try:
                sql = f"SELECT {', '.join(exprs)} FROM (SELECT * FROM {fqn} LIMIT {self.MAX_PROFILE_SAMPLE})"
                row = self.spark.sql(sql).collect()[0].asDict()
            except Exception as e:
                print(f"     \u26a0\ufe0f Profile query failed for {tname}: {str(e)[:100]}")
                return tname, {}

            row_count = row.get('__row_count__', 0) or 0
            profiles = {}

            for cn, dt, base_type, is_complex in col_meta:
                if is_complex:
                    profiles[cn] = {
                        'data_type': dt, 'role': 'complex',
                        'cardinality': None, 'null_pct': None,
                        'min': None, 'max': None, 'top_values': []
                    }
                    continue

                card = row.get(f'acd__{cn}', 0) or 0
                null_count = row.get(f'null__{cn}', 0) or 0
                null_pct = round(null_count * 100.0 / row_count, 1) if row_count > 0 else 0.0
                mn = row.get(f'min__{cn}')
                mx = row.get(f'max__{cn}')

                # Classify column role
                role = self._classify_column_role(cn, base_type, card, row_count)

                profiles[cn] = {
                    'data_type': dt, 'role': role,
                    'cardinality': card, 'null_pct': null_pct,
                    'min': str(mn) if mn is not None else None,
                    'max': str(mx) if mx is not None else None,
                    'top_values': []  # filled below for low-cardinality
                }

            # Second pass: collect distinct values for low-cardinality columns
            low_card_cols = [
                cn for cn, p in profiles.items()
                if p['role'] in ('categorical', 'temporal') and
                   p['cardinality'] is not None and
                   0 < p['cardinality'] <= self.LOW_CARDINALITY_THRESHOLD
            ]
            if low_card_cols:
                try:
                    dv_exprs = [
                        f"COLLECT_SET({self._quote(cn)}) AS `vals__{cn}`"
                        for cn in low_card_cols
                    ]
                    dv_sql = f"SELECT {', '.join(dv_exprs)} FROM (SELECT * FROM {fqn} LIMIT {self.MAX_PROFILE_SAMPLE})"
                    dv_row = self.spark.sql(dv_sql).collect()[0].asDict()
                    for cn in low_card_cols:
                        vals = dv_row.get(f'vals__{cn}', []) or []
                        profiles[cn]['top_values'] = sorted(
                            [str(v) for v in vals if v is not None], key=str
                        )[:20]
                except Exception:
                    pass

            return tname, profiles

        # Run profiling in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(_profile_one_table, t): t for t in table_names}
            for fut in as_completed(futures):
                tname, profiles = fut.result()
                if profiles:
                    all_profiles[tname] = profiles

        return all_profiles

    def _classify_column_role(self, col_name: str, base_type: str,
                              cardinality: int, row_count: int) -> str:
        """Classify a column's role: temporal, categorical, numeric, identifier."""
        if base_type in self.TEMPORAL_TYPES:
            return 'temporal'

        if self.IDENTIFIER_PATTERNS.search(col_name):
            return 'identifier'

        if base_type in ('STRING', 'VARCHAR', 'CHAR'):
            if row_count > 0 and cardinality <= self.LOW_CARDINALITY_THRESHOLD:
                return 'categorical'
            # High-cardinality strings: could be names, descriptions
            ratio = cardinality / max(row_count, 1)
            if ratio > 0.8:
                return 'identifier'  # likely unique IDs or free text
            return 'categorical'

        if base_type == 'BOOLEAN':
            return 'categorical'

        if base_type in self.NUMERIC_TYPES:
            # Low-cardinality numeric = categorical (e.g., rating 1-5)
            if cardinality <= 20:
                return 'categorical'
            return 'numeric'

        return 'categorical'  # default for unknown types

    # ─── Phase 3: Dimension Classification ─────────────────────────────────

    def _classify_dimensions(self, column_profiles: dict, all_columns: list) -> list:
        """Classify columns as dimensions suitable for Genie Space analysis.

        Returns list of dimension dicts: {name, column, table, type, description,
        sample_values, cardinality}
        """
        dimensions = []
        seen_names = set()

        for tname, cols in column_profiles.items():
            for cname, profile in cols.items():
                role = profile.get('role', '')

                # Skip identifiers and complex types — not useful as dimensions
                if role in ('identifier', 'complex', 'numeric'):
                    continue

                # Only temporal and categorical columns become dimensions
                if role == 'temporal':
                    dim_type = 'temporal'
                elif role == 'categorical':
                    dim_type = 'categorical'
                else:
                    continue

                # Build a unique dimension name
                dim_name = cname
                if dim_name in seen_names:
                    dim_name = f"{tname}_{cname}"
                seen_names.add(dim_name)

                dim = {
                    'name': dim_name,
                    'column': cname,
                    'table': tname,
                    'type': dim_type,
                    'data_type': profile.get('data_type', ''),
                    'cardinality': profile.get('cardinality', 0),
                    'null_pct': profile.get('null_pct', 0),
                    'sample_values': profile.get('top_values', [])[:10],
                }
                if profile.get('min'):
                    dim['min'] = profile['min']
                if profile.get('max'):
                    dim['max'] = profile['max']

                dimensions.append(dim)

        # Sort: temporal first, then categorical, alphabetically within
        dimensions.sort(key=lambda d: (0 if d['type'] == 'temporal' else 1, d['table'], d['name']))
        return dimensions

    # ─── Phase 4: Data Model Inference ─────────────────────────────────────

    def _infer_data_model(self, table_names: list, all_columns: list,
                          column_profiles: dict, samples: dict) -> dict:
        """Infer relationships and entity types across tables.

        Returns:
            dict with:
                relationships: list of {left_table, left_column, right_table,
                    right_column, join_type, cardinality, confidence, validated}
                entity_types: dict {table_name: 'fact'|'dimension'|'bridge'}
        """
        cols_by_table = {}
        for c in all_columns:
            cols_by_table.setdefault(c['table_name'], []).append(c)

        relationships = []
        seen_pairs = set()  # avoid duplicate relationships

        # ── Pattern-based FK detection ──
        # Pattern 1: column_name matches {other_table}_id (singular or plural)
        # Pattern 2: column_name matches {other_table}_key, {other_table}_fk
        # Pattern 3: same column_name ending in _id exists in two tables

        fk_patterns = [
            (r'^(.+?)_id$', 'id'),
            (r'^(.+?)_key$', 'id'),
            (r'^(.+?)_fk$', 'id'),
            (r'^fk_(.+)$', 'id'),
        ]

        table_set = set(table_names)

        for tname, tcols in cols_by_table.items():
            for col in tcols:
                cn = col['column_name']
                dt = col['data_type'].upper()

                for pattern, target_col in fk_patterns:
                    match = re.match(pattern, cn, re.IGNORECASE)
                    if not match:
                        continue

                    ref_base = match.group(1).lower()

                    # Try plural, singular, and exact match
                    candidates = [
                        ref_base + 's',       # passenger_id → passengers
                        ref_base,              # airline_id → airline
                        ref_base + 'es',       # status_id → statuses
                        ref_base.rstrip('s'),  # flights_id → flight
                    ]

                    for ref_table in candidates:
                        if ref_table not in table_set or ref_table == tname:
                            continue

                        # Check if the referenced table has a matching PK column
                        ref_cols = [c['column_name'] for c in cols_by_table.get(ref_table, [])]
                        pk_candidates = ['id', f'{ref_base}_id', cn]

                        for pk_col in pk_candidates:
                            if pk_col in ref_cols:
                                pair_key = tuple(sorted([(tname, cn), (ref_table, pk_col)]))
                                if pair_key in seen_pairs:
                                    break
                                seen_pairs.add(pair_key)

                                rel = {
                                    'left_table': tname,
                                    'left_column': cn,
                                    'right_table': ref_table,
                                    'right_column': pk_col,
                                    'join_type': 'LEFT',
                                    'cardinality': 'MANY_TO_ONE',
                                    'confidence': 'high',
                                    'validated': False
                                }
                                relationships.append(rel)
                                break
                        if pair_key in seen_pairs:
                            break

        # Pattern 3: Shared column names across tables (e.g., both have 'airport_code')
        col_tables = {}  # column_name → list of (table, data_type)
        for tname, tcols in cols_by_table.items():
            for col in tcols:
                cn = col['column_name']
                if cn == 'id':  # skip generic 'id' — too ambiguous
                    continue
                col_tables.setdefault(cn, []).append((tname, col['data_type']))

        for cn, table_list_for_col in col_tables.items():
            if len(table_list_for_col) < 2:
                continue
            if not cn.endswith(('_id', '_key', '_code')):
                continue
            for i in range(len(table_list_for_col)):
                for j in range(i + 1, len(table_list_for_col)):
                    t1, dt1 = table_list_for_col[i]
                    t2, dt2 = table_list_for_col[j]
                    pair_key = tuple(sorted([(t1, cn), (t2, cn)]))
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    relationships.append({
                        'left_table': t1, 'left_column': cn,
                        'right_table': t2, 'right_column': cn,
                        'join_type': 'INNER',
                        'cardinality': 'MANY_TO_ONE',
                        'confidence': 'medium',
                        'validated': False
                    })

        # ── Data overlap validation ──
        fq_lookup = {
            pt['table']: f"{self._quote(pt['catalog'])}.{self._quote(pt['schema'])}.{self._quote(pt['table'])}"
            for pt in self.parsed_tables
        }

        for rel in relationships:
            lt, lc = rel['left_table'], rel['left_column']
            rt, rc = rel['right_table'], rel['right_column']
            lfq = fq_lookup.get(lt)
            rfq = fq_lookup.get(rt)
            if not lfq or not rfq:
                continue
            try:
                overlap_sql = f"""
                    SELECT COUNT(*) AS overlap_count FROM (
                        SELECT DISTINCT {self._quote(lc)} AS k
                        FROM {lfq} WHERE {self._quote(lc)} IS NOT NULL LIMIT 1000
                    ) a
                    INNER JOIN (
                        SELECT DISTINCT {self._quote(rc)} AS k
                        FROM {rfq} WHERE {self._quote(rc)} IS NOT NULL LIMIT 1000
                    ) b ON a.k = b.k
                """
                overlap = self.spark.sql(overlap_sql).collect()[0]['overlap_count']
                if overlap > 0:
                    rel['validated'] = True
                    if rel['confidence'] == 'medium':
                        rel['confidence'] = 'high'
                else:
                    rel['confidence'] = 'low'
            except Exception:
                pass  # keep unvalidated

        # ── Entity type classification ──
        entity_types = {}
        # Tables referenced as right side of MANY_TO_ONE are likely dimensions
        dim_tables = set()
        fact_tables = set()
        for rel in relationships:
            if rel['cardinality'] == 'MANY_TO_ONE':
                dim_tables.add(rel['right_table'])
                fact_tables.add(rel['left_table'])

        for tname in table_names:
            profiles = column_profiles.get(tname, {})
            n_numeric = sum(1 for p in profiles.values() if p.get('role') == 'numeric')
            n_temporal = sum(1 for p in profiles.values() if p.get('role') == 'temporal')

            if tname in fact_tables and tname not in dim_tables:
                entity_types[tname] = 'fact'
            elif tname in dim_tables and tname not in fact_tables:
                entity_types[tname] = 'dimension'
            elif tname in fact_tables and tname in dim_tables:
                entity_types[tname] = 'bridge'
            elif n_numeric >= 3 and n_temporal >= 1:
                entity_types[tname] = 'fact'
            else:
                entity_types[tname] = 'dimension'

        # Log findings
        for rel in relationships:
            status = '\u2705' if rel['validated'] else '\u2753'
            print(f"    {status} {rel['left_table']}.{rel['left_column']} \u2192 {rel['right_table']}.{rel['right_column']} ({rel['confidence']})")

        for tname, etype in sorted(entity_types.items()):
            icon = '\U0001f4e6' if etype == 'fact' else '\U0001f4d6' if etype == 'dimension' else '\U0001f517'
            print(f"    {icon} {tname}: {etype}")

        return {
            'relationships': relationships,
            'entity_types': entity_types
        }

    # ─── Phase 2 (legacy): Build Summary ───────────────────────────────────

    MAX_SUMMARY_CHARS = 80_000

    def _build_summary(self, all_tables, all_columns, samples,
                       struct_info=None, profiles=None) -> str:
        """Build a compact but rich metadata summary for the LLM."""
        struct_info = struct_info or {}
        profiles = profiles or {}
        n_tables = max(len(all_tables), 1)

        per_table = self.MAX_SUMMARY_CHARS // n_tables
        max_struct  = min(12, max(4, per_table // 500))
        max_dv_vals = min(10, max(3, per_table // 600))
        max_dv_cols = min(6, max(2, per_table // 800))
        sample_rows = 1 if per_table > 1500 else 0
        sample_chars = min(800, max(0, per_table - 400))

        cols_by_table = {}
        for c in all_columns:
            cols_by_table.setdefault(c['table_name'], []).append(c)

        parts = []
        for tbl in all_tables:
            tname = tbl['table_name']
            tbl_cols = cols_by_table.get(tname, [])

            col_parts = []
            for c in tbl_cols:
                cname, dtype = c['column_name'], c['data_type']
                struct_fields = struct_info.get(tname, {}).get(cname)
                if struct_fields:
                    shown = struct_fields[:max_struct]
                    suffix = f", +{len(struct_fields)-max_struct} more" if len(struct_fields) > max_struct else ""
                    col_parts.append(f"{cname} ({dtype}: {', '.join(shown)}{suffix})")
                else:
                    col_parts.append(f"{cname} ({dtype})")

            part = f"TABLE: {tname}\n  Columns: {', '.join(col_parts)}"

            if tbl.get('comment'):
                part += f"\n  Comment: {tbl['comment'][:120]}"

            tbl_profile = profiles.get(tname, {})
            if tbl_profile.get('date_ranges'):
                dr = tbl_profile['date_ranges']
                ranges_str = "; ".join(f"{col}: {mn} to {mx}" for col, mn, mx in dr)
                part += f"\n  Date Ranges: {ranges_str}"

            if tbl_profile.get('distinct_values'):
                dv_parts = []
                for col, vals in list(tbl_profile['distinct_values'].items())[:max_dv_cols]:
                    vals_str = ", ".join(str(v) for v in vals[:max_dv_vals])
                    if len(vals) > max_dv_vals:
                        vals_str += f", +{len(vals)-max_dv_vals} more"
                    dv_parts.append(f"{col}=[{vals_str}]")
                if dv_parts:
                    part += f"\n  Key Values: {'; '.join(dv_parts)}"

            if sample_rows and samples.get(tname) and sample_chars > 0:
                sample_str = json.dumps(samples[tname][:sample_rows], default=str)
                if len(sample_str) > sample_chars:
                    sample_str = sample_str[:sample_chars] + "...(truncated)"
                part += f"\n  Sample: {sample_str}"

            parts.append(part)

        summary = "\n\n".join(parts)
        if len(summary) > self.MAX_SUMMARY_CHARS:
            summary = summary[:self.MAX_SUMMARY_CHARS] + "\n...(truncated to fit token budget)"
        return summary

    # ─── Phase 2a: Struct Field Extraction ─────────────────────────────────

    def _extract_struct_info(self, table_names) -> dict:
        """Extract nested field names for STRUCT/MAP columns using spark schema."""
        result = {}
        fq_lookup = {pt['table']: f"{self._quote(pt['catalog'])}.{self._quote(pt['schema'])}.{self._quote(pt['table'])}"
                     for pt in self.parsed_tables}

        def _get_nested_fields(dtype, prefix="", depth=0, max_depth=2):
            fields = []
            if hasattr(dtype, 'fields') and depth < max_depth:
                for f in dtype.fields:
                    if hasattr(f.dataType, 'fields') and depth + 1 < max_depth:
                        fields.extend(_get_nested_fields(f.dataType, f.name + ".", depth + 1, max_depth))
                    else:
                        fields.append(f"{prefix}{f.name}")
            return fields

        for tname in table_names:
            fq = fq_lookup.get(tname)
            if not fq:
                continue
            try:
                schema = self.spark.table(fq).schema
                tbl_structs = {}
                for field in schema.fields:
                    if hasattr(field.dataType, 'fields'):
                        sub = _get_nested_fields(field.dataType)
                        if sub:
                            tbl_structs[field.name] = sub
                    elif hasattr(field.dataType, 'keyType'):
                        tbl_structs[field.name] = [f"key:{field.dataType.keyType.simpleString()}", f"value:{field.dataType.valueType.simpleString()}"]
                if tbl_structs:
                    result[tname] = tbl_structs
            except Exception:
                pass
        return result

    # ─── Phase 2b: Lightweight Data Profiling (backward compat) ────────────

    def _profile_tables(self, table_names, all_columns) -> dict:
        """Profile tables to extract date ranges and distinct values."""
        fq_lookup = {pt['table']: f"{self._quote(pt['catalog'])}.{self._quote(pt['schema'])}.{self._quote(pt['table'])}"
                     for pt in self.parsed_tables}
        profiles = {}
        MAX_SAMPLE = 50000

        cols_by_table = {}
        for c in all_columns:
            cols_by_table.setdefault(c['table_name'], []).append(c)

        def _profile_one(tname):
            tbl_cols = cols_by_table.get(tname, [])
            date_cols = [c['column_name'] for c in tbl_cols if c['data_type'] in ('DATE', 'TIMESTAMP')]
            str_cols = [c['column_name'] for c in tbl_cols if c['data_type'] == 'STRING']

            if not date_cols and not str_cols:
                return tname, {}

            fqn = fq_lookup.get(tname)
            if not fqn:
                return tname, {}

            exprs = []
            for dc in date_cols:
                qdc = self._quote(dc)
                exprs.append(f"CAST(MIN({qdc}) AS STRING) AS `min__{dc}`")
                exprs.append(f"CAST(MAX({qdc}) AS STRING) AS `max__{dc}`")
            for sc in str_cols:
                qsc = self._quote(sc)
                exprs.append(f"APPROX_COUNT_DISTINCT({qsc}) AS `acd__{sc}`")

            if not exprs:
                return tname, {}

            try:
                sql = f"SELECT {', '.join(exprs)} FROM (SELECT * FROM {fqn} LIMIT {MAX_SAMPLE})"
                row = self.spark.sql(sql).collect()[0]
                row_dict = row.asDict()

                date_ranges = []
                for dc in date_cols:
                    mn = row_dict.get(f"min__{dc}")
                    mx = row_dict.get(f"max__{dc}")
                    if mn and mx:
                        date_ranges.append((dc, str(mn), str(mx)))

                low_card = [sc for sc in str_cols if (row_dict.get(f"acd__{sc}") or 999) <= self.LOW_CARDINALITY_THRESHOLD]

                distinct_values = {}
                if low_card:
                    dv_exprs = [f"COLLECT_SET({self._quote(sc)}) AS `vals__{sc}`" for sc in low_card]
                    dv_sql = f"SELECT {', '.join(dv_exprs)} FROM (SELECT * FROM {fqn} LIMIT {MAX_SAMPLE})"
                    dv_row = self.spark.sql(dv_sql).collect()[0].asDict()
                    for sc in low_card:
                        vals = dv_row.get(f"vals__{sc}", [])
                        if vals:
                            distinct_values[sc] = sorted([v for v in vals if v], key=str)

                return tname, {'date_ranges': date_ranges, 'distinct_values': distinct_values}
            except Exception:
                return tname, {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(_profile_one, t): t for t in table_names}
            for fut in as_completed(futures):
                tname, profile = fut.result()
                if profile:
                    profiles[tname] = profile

        return profiles

    # ─── Phase 5: LLM Call ─────────────────────────────────────────────────

    def _call_llm(self, metadata_summary: str) -> Dict[str, Any]:
        from .resilient_llm import ResilientLLM
        import warnings
        warnings.filterwarnings('ignore', category=Warning)

        llm = ResilientLLM(model_pool=self.model_pool, temperature=0, timeout=900)

        tables_desc = "\n".join(f"  - {pt['fq_name']}" for pt in self.parsed_tables)

        prompt = f"""Analyse this Unity Catalog table metadata and generate concise configuration values for a Genie Space.
You are writing for BUSINESS ANALYSTS, not engineers. All user-facing text must be in plain natural language.

TABLES (from potentially multiple catalogs/schemas):
{tables_desc}

--- METADATA ---
{metadata_summary}
--- END METADATA ---

Return a JSON object with EXACTLY these keys. STRICT LENGTH LIMITS \u2014 exceed them and the output is unusable:

1. "genie_space_name": Max 50 chars. Short analytics space name. Example: "Aviation & Revenue Analytics"

2. "business_domain": MAX 30 WORDS. Plain business language. What the organisation does \u2014 mention real products, services, or operations found in the data. Example: "Global food delivery service operating across 5 cities with 200 partner restaurants and 50 delivery drivers."

3. "data_description": MAX 50 WORDS. Written for a business analyst \u2014 describe WHAT business concepts are tracked, not column names. Reference the richness of nested/detailed data without exposing technical names. Example: "Customer orders with itemised line details, delivery tracking including driver performance and vehicle information, payment transactions with method breakdowns, and restaurant profiles with cuisine and rating data."

4. "stakeholders_and_decisions": MAX 40 WORDS. Map stakeholders to business decisions in plain language. Example: "Operations managers monitor delivery speed and driver ratings; finance teams track revenue by payment channel; marketing analyses customer segments and booking patterns."

5. "additional_context": MAX 50 WORDS. Include: actual date ranges, key business categories from the data, and how tables relate to each other \u2014 all in plain business language. Example: "Data covers Jan 2024 to Mar 2025. Booking classes include Economy, Business, and First. Orders link to customers, deliveries, and payments. Flight statuses: On-time, Delayed, Cancelled."

6. "sample_questions": JSON array of exactly 10 questions written as a BUSINESS USER would naturally ask.
   RULES for questions:
   - Write in everyday business language \u2014 NO column names, NO table names, NO technical syntax
   - Questions must sound like a manager or analyst speaking aloud
   - Cover a mix: totals, trends, comparisons, breakdowns, top/bottom performers
   - Leverage the depth of the data (nested details, tags, categories) but express it in business terms
   - BAD: "What is usage_quantity by billing_origin_product?" (uses column names)
   - GOOD: "Which product categories are driving the highest consumption?"

RULES:
- ALL text must be in plain business language \u2014 no column names, table names, or technical identifiers anywhere.
- Use ONLY actual business concepts from the metadata \u2014 do not hallucinate or invent.
- Internally understand the struct/nested fields to generate RICH domain-aware content, but express everything in business terms.
- Return ONLY valid JSON. No markdown, no explanation, no code fences.
"""

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            response = llm.invoke(prompt)
        raw_text = response.content.strip()

        cleaned = raw_text
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            match = re.search(r'\{[\s\S]*\}', raw_text)
            if match:
                return json.loads(match.group())
            raise ValueError("Could not parse LLM response as JSON. Please retry.")

    # ─── Phase 6: Write Config ─────────────────────────────────────────────

    def _write_config(self, generated: Dict[str, Any],
                      column_profiles: dict = None,
                      dimensions: list = None,
                      data_model: dict = None) -> Dict[str, Any]:
        """Write config.yaml with all sections including deep scan results."""
        updated = dict(self.existing_config)

        updated['table_list'] = self.table_list
        updated['catalog'] = self.primary_catalog
        updated['schema'] = self.primary_schema
        updated['llm_model'] = self.existing_llm

        for field in self.VALUE_FIELDS:
            if field in generated:
                updated[field] = generated[field]

        if column_profiles:
            updated['column_profiles'] = column_profiles
        if dimensions:
            updated['dimensions'] = dimensions
        if data_model:
            updated['data_model'] = data_model

        lines = [
            "# ============================================================",
            "# AI-Powered Genie Space Generator \u2014 Configuration",
            "# ============================================================",
            "# Auto-generated by auto_configure() with deep scan",
            "# ============================================================",
            "",
            "# \u2500\u2500 Table List (fully qualified: catalog.schema.table) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
            "table_list:",
        ]
        for fq in self.table_list:
            lines.append(f'  - "{fq}"')

        lines += [
            "",
            "# \u2500\u2500 Derived Target Schema (for metric view creation) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
            f"catalog: {self.primary_catalog}",
            f"schema: {self.primary_schema}",
            "",
            "# \u2500\u2500 Genie Space \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
            f'genie_space_name: "{updated.get("genie_space_name", "Analytics Space")}"',
            "",
            "# \u2500\u2500 AI Model \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500",
            f"llm_model: {updated['llm_model']}",
            "",
            "# ============================================================",
            "# Business Context (auto-generated from table analysis)",
            "# ============================================================",
            "",
            "# Q1: What does your business do?",
            f'business_domain: "{updated.get("business_domain", "")}"',
            "",
            "# Q2: What data is tracked in these tables?",
            f'data_description: "{updated.get("data_description", "")}"',
            "",
            "# Q3: Who uses this data and what decisions do they make?",
            f'stakeholders_and_decisions: "{updated.get("stakeholders_and_decisions", "")}"',
            "",
            "# Q4: Any other important context? (optional)",
            f'additional_context: "{updated.get("additional_context", "")}"',
            "",
            "# ============================================================",
            "# Sample Questions (auto-generated)",
            "# ============================================================",
            "sample_questions:",
        ]
        for q in updated.get('sample_questions', []):
            q_escaped = str(q).replace('"', '\\"')
            lines.append(f'  - "{q_escaped}"')

        # ── New: Dimensions section ──
        if dimensions:
            lines += [
                "",
                "# ============================================================",
                "# Dimensions (auto-detected from deep scan)",
                "# ============================================================",
                "# Review and adjust: remove irrelevant dimensions, add missing ones",
                "dimensions:",
            ]
            for dim in dimensions:
                lines.append(f'  - name: "{dim["name"]}"')
                lines.append(f'    column: "{dim["column"]}"')
                lines.append(f'    table: "{dim["table"]}"')
                lines.append(f'    type: "{dim["type"]}"')
                lines.append(f'    data_type: "{dim.get("data_type", "")}"')
                lines.append(f'    cardinality: {dim.get("cardinality", 0)}')
                if dim.get('min'):
                    lines.append(f'    min: "{dim["min"]}"')
                if dim.get('max'):
                    lines.append(f'    max: "{dim["max"]}"')
                if dim.get('sample_values'):
                    sv = json.dumps(dim['sample_values'][:10])
                    lines.append(f'    sample_values: {sv}')

        # ── New: Data Model section ──
        if data_model:
            lines += [
                "",
                "# ============================================================",
                "# Data Model (auto-inferred from deep scan)",
                "# ============================================================",
            ]

            if data_model.get('entity_types'):
                lines.append("# Entity types: fact tables have measures, dimension tables are lookups")
                lines.append("entity_types:")
                for tname, etype in sorted(data_model['entity_types'].items()):
                    lines.append(f'  {tname}: "{etype}"')

            if data_model.get('relationships'):
                lines += ["", "# Relationships (validated = data overlap confirmed)"]
                lines.append("relationships:")
                for rel in data_model['relationships']:
                    lines.append(f'  - left_table: "{rel["left_table"]}"')
                    lines.append(f'    left_column: "{rel["left_column"]}"')
                    lines.append(f'    right_table: "{rel["right_table"]}"')
                    lines.append(f'    right_column: "{rel["right_column"]}"')
                    lines.append(f'    join_type: "{rel.get("join_type", "LEFT")}"')
                    lines.append(f'    cardinality: "{rel.get("cardinality", "MANY_TO_ONE")}"')
                    lines.append(f'    confidence: "{rel.get("confidence", "medium")}"')
                    lines.append(f'    validated: {str(rel.get("validated", False)).lower()}')

        # ── Column Profiles (compact — written as YAML block for downstream use) ──
        if column_profiles:
            lines += [
                "",
                "# ============================================================",
                "# Column Profiles (auto-generated deep scan — used by framework)",
                "# ============================================================",
            ]
            # Write as a compact YAML structure
            for tname in sorted(column_profiles.keys()):
                lines.append(f"# {tname}")

            # Store as embedded YAML via yaml.dump for correctness
            # But keep it compact — only role, cardinality, null_pct per column
            compact_profiles = {}
            for tname, cols in column_profiles.items():
                compact_profiles[tname] = {}
                for cname, prof in cols.items():
                    cp = {'role': prof.get('role', 'unknown')}
                    if prof.get('data_type'):
                        cp['data_type'] = prof['data_type']
                    if prof.get('cardinality') is not None:
                        cp['cardinality'] = prof['cardinality']
                    if prof.get('null_pct', 0) > 0:
                        cp['null_pct'] = prof['null_pct']
                    if prof.get('top_values'):
                        cp['top_values'] = prof['top_values'][:5]
                    compact_profiles[tname][cname] = cp

            profile_yaml = yaml.dump(
                {'column_profiles': compact_profiles},
                default_flow_style=False, sort_keys=False, width=120
            )
            for line in profile_yaml.split('\n'):
                if line.strip():
                    lines.append(line)

        with open(self.config_path, 'w') as f:
            f.write("\n".join(lines) + "\n")

        return updated

    # ─── HTML Result Display ───────────────────────────────────────────────

    def _display_result(self, updated: Dict[str, Any],
                        dimensions: list = None, data_model: dict = None):
        """Display HTML banner with config link and deep scan stats."""
        from IPython.display import display, HTML

        config_abs = self.config_path
        config_ws_path = config_abs.replace('/Workspace', '', 1) if config_abs.startswith('/Workspace') else config_abs
        try:
            workspace_url = self.spark.conf.get("spark.databricks.workspaceUrl")
        except Exception:
            workspace_url = None
        space_name = updated.get('genie_space_name', 'Analytics Space')
        n_tables = len(self.table_list)
        n_questions = len(updated.get('sample_questions', []))
        n_dimensions = len(dimensions or [])
        n_relationships = len((data_model or {}).get('relationships', []))

        html = f"""
        <div style="
            margin: 16px 0;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 4px 24px rgba(0,0,0,0.10);
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        ">
            <!-- Header -->
            <div style="
                background: linear-gradient(135deg, #1b5e20 0%, #388e3c 50%, #43a047 100%);
                padding: 20px 24px;
                display: flex;
                align-items: center;
                gap: 12px;
            ">
                <div style="
                    width: 44px; height: 44px;
                    background: rgba(255,255,255,0.2);
                    border-radius: 50%;
                    display: flex; align-items: center; justify-content: center;
                    font-size: 22px;
                ">&#x2705;</div>
                <div>
                    <div style="color: white; font-size: 17px; font-weight: 600; letter-spacing: -0.2px;">
                        Deep Scan Configuration Generated
                    </div>
                    <div style="color: rgba(255,255,255,0.8); font-size: 13px; margin-top: 2px;">
                        {n_tables} tables &nbsp;&#x2192;&nbsp; <strong>{space_name}</strong>
                    </div>
                </div>
            </div>

            <!-- Stats Row -->
            <div style="
                background: #f1f8e9;
                padding: 14px 24px;
                display: flex;
                gap: 32px;
                border-bottom: 1px solid #c8e6c9;
            ">
                <div style="font-size: 13px; color: #33691e;">
                    &#x1f4ca; <strong>{n_tables}</strong> tables
                </div>
                <div style="font-size: 13px; color: #33691e;">
                    &#x1f3af; <strong>{n_dimensions}</strong> dimensions
                </div>
                <div style="font-size: 13px; color: #33691e;">
                    &#x1f517; <strong>{n_relationships}</strong> relationships
                </div>
                <div style="font-size: 13px; color: #33691e;">
                    &#x2753; <strong>{n_questions}</strong> sample questions
                </div>
            </div>

            <!-- Action -->
            <div style="
                background: white;
                padding: 18px 24px;
                display: flex;
                align-items: center;
                justify-content: space-between;
            ">
                <span style="font-size: 13px; color: #555;">
                    Review dimensions, data model & business context, then run <strong>Run Framework</strong>.
                </span>
                <a href="{"https://" + workspace_url + "#workspace" + config_ws_path if workspace_url else "#workspace" + config_ws_path}" target="_blank" style="
                    display: inline-flex; align-items: center; gap: 6px;
                    padding: 10px 20px;
                    background: linear-gradient(135deg, #1b5e20, #2e7d32);
                    color: white;
                    text-decoration: none;
                    border-radius: 8px;
                    font-size: 13px;
                    font-weight: 600;
                    box-shadow: 0 2px 8px rgba(27,94,32,0.3);
                    transition: transform 0.15s;
                ">&#x1f4c4; Open config.yaml</a>
            </div>
        </div>
        """
        display(HTML(html))

    # ─── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _resolve_path(path: str) -> str:
        if os.path.isabs(path):
            return path
        module_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(module_dir)
        return os.path.normpath(os.path.join(project_dir, path))

    @staticmethod
    def _quote(identifier: str) -> str:
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            return identifier
        return f"`{identifier}`"


# ─── Convenience Function ──────────────────────────────────────────────────

def auto_configure(table_list: list, config_path: str,
                   llm_model: str = None, model_pool: list = None,
                   sample_rows: int = 5, max_workers: int = 10) -> Dict[str, Any]:
    """
    Analyse specified tables and auto-update config.yaml values.

    Args:
        table_list: List of fully qualified table names (catalog.schema.table)
        config_path: Path to config.yaml
    """
    configurator = AutoConfigurator(
        table_list=table_list, config_path=config_path,
        llm_model=llm_model, model_pool=model_pool,
        sample_rows=sample_rows, max_workers=max_workers
    )
    return configurator.run()
