"""
Auto-Configurator Module
Accepts a list of fully qualified table names (catalog.schema.table),
scans their metadata, and auto-generates config.yaml values using AI.
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

    def __init__(self, table_list: List[str], config_path: str,
                 llm_model: str = None, model_pool: list = None,
                 sample_rows: int = 5, max_workers: int = 10,
                 metric_views_catalog: str = None, 
                 metric_views_schema: str = None):
        self.table_list = table_list
        self.config_path = self._resolve_path(config_path)
        self.sample_rows = sample_rows
        self.max_workers = max_workers
        self.spark = SparkSession.builder.getOrCreate()
        self.metric_views_catalog = metric_views_catalog
        self.metric_views_schema = metric_views_schema

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
        """Execute the full auto-configure pipeline."""
        all_tables, all_columns, samples = self._scan_metadata()
        table_names = [t['table_name'] for t in all_tables]
        struct_info = self._extract_struct_info(table_names)
        profiles = self._profile_tables(table_names, all_columns)
        metadata_summary = self._build_summary(all_tables, all_columns, samples, struct_info, profiles)
        generated = self._call_llm(metadata_summary)
        updated = self._write_config(generated)
        self._display_result(updated)
        return updated

    # ─── Phase 1: Metadata Scan ────────────────────────────────────────────

    def _scan_metadata(self):
        """Scan metadata for the specific tables in table_list.
        Groups tables by catalog.schema for efficient information_schema queries."""

        # Group tables by catalog.schema
        schema_groups = {}
        for pt in self.parsed_tables:
            key = (pt['catalog'], pt['schema'])
            schema_groups.setdefault(key, []).append(pt['table'])

        all_tables = []
        all_columns = []

        for (cat, sch), table_names in schema_groups.items():
            qcat, qsch = self._quote(cat), self._quote(sch)
            names_sql = ', '.join(f"\'{t}\'" for t in table_names)

            # Get table metadata
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
                print(f"  ⚠️ Could not query information_schema for {cat}.{sch}: {e}")
                # Fallback: create minimal table entries
                for tname in table_names:
                    all_tables.append({'table_name': tname, 'comment': None})

            # Get columns
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
                print(f"  ⚠️ Could not query columns for {cat}.{sch}: {e}")

        # Build FQ name lookup for sampling
        fq_lookup = {pt['table']: pt['fq_name'] for pt in self.parsed_tables}

        # Sample tables in parallel using FQ names
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

    # ─── Phase 2: Build Summary ────────────────────────────────────────────

    MAX_SUMMARY_CHARS = 80_000

    MAX_SUMMARY_CHARS = 80_000  # safe under 4MB payload with prompt overhead

    def _build_summary(self, all_tables, all_columns, samples,
                       struct_info=None, profiles=None) -> str:
        """Build a compact but rich metadata summary for the LLM.

        Automatically scales detail level based on number of tables
        to stay within token budgets. Few tables → rich detail;
        many tables → compact summaries.
        """
        struct_info = struct_info or {}
        profiles = profiles or {}
        n_tables = max(len(all_tables), 1)

        # ── Dynamic budget per table ──
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

            # ── Column listing with struct expansion ──
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

            # ── Profile: date ranges + distinct values ──
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

            # ── Sample rows (only if budget allows) ──
            if sample_rows and samples.get(tname) and sample_chars > 0:
                sample_str = json.dumps(samples[tname][:sample_rows], default=str)
                if len(sample_str) > sample_chars:
                    sample_str = sample_str[:sample_chars] + "...(truncated)"
                part += f"\n  Sample: {sample_str}"

            parts.append(part)

        summary = "\n\n".join(parts)

        # ── Final safety cap ──
        if len(summary) > self.MAX_SUMMARY_CHARS:
            summary = summary[:self.MAX_SUMMARY_CHARS] + "\n...(truncated to fit token budget)"

        return summary


    # ─── Phase 2a: Struct Field Extraction ─────────────────────────────────

    def _extract_struct_info(self, table_names) -> dict:
        """Extract nested field names for STRUCT/MAP columns using spark schema.
        Uses FQ names from parsed_tables for cross-schema support."""
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

    # ─── Phase 2b: Lightweight Data Profiling ──────────────────────────────

    def _profile_tables(self, table_names, all_columns) -> dict:
        """Profile tables to extract date ranges and distinct values."""
        fq_lookup = {pt['table']: f"{self._quote(pt['catalog'])}.{self._quote(pt['schema'])}.{self._quote(pt['table'])}"
                     for pt in self.parsed_tables}
        profiles = {}
        MAX_SAMPLE = 50000
        LOW_CARDINALITY_THRESHOLD = 30

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

                low_card = [sc for sc in str_cols if (row_dict.get(f"acd__{sc}") or 999) <= LOW_CARDINALITY_THRESHOLD]

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

    # ─── Phase 3: LLM Call ─────────────────────────────────────────────────

    def _call_llm(self, metadata_summary: str) -> Dict[str, Any]:
        from .resilient_llm import ResilientLLM
        import warnings
        warnings.filterwarnings('ignore', category=Warning)

        llm = ResilientLLM(model_pool=self.model_pool, temperature=0, timeout=900)

        # Build table list description for the prompt
        tables_desc = "\n".join(f"  - {pt['fq_name']}" for pt in self.parsed_tables)

        prompt = f"""Analyse this Unity Catalog table metadata and generate concise configuration values for a Genie Space.
You are writing for BUSINESS ANALYSTS, not engineers. All user-facing text must be in plain natural language.

TABLES (from potentially multiple catalogs/schemas):
{tables_desc}

--- METADATA ---
{metadata_summary}
--- END METADATA ---

Return a JSON object with EXACTLY these keys. STRICT LENGTH LIMITS — exceed them and the output is unusable:

1. "genie_space_name": Max 50 chars. Short analytics space name. Example: "Aviation & Revenue Analytics"

2. "business_domain": MAX 30 WORDS. Plain business language. What the organisation does — mention real products, services, or operations found in the data. Example: "Global food delivery service operating across 5 cities with 200 partner restaurants and 50 delivery drivers."

3. "data_description": MAX 50 WORDS. Written for a business analyst — describe WHAT business concepts are tracked, not column names. Reference the richness of nested/detailed data without exposing technical names. Example: "Customer orders with itemised line details, delivery tracking including driver performance and vehicle information, payment transactions with method breakdowns, and restaurant profiles with cuisine and rating data."

4. "stakeholders_and_decisions": MAX 40 WORDS. Map stakeholders to business decisions in plain language. Example: "Operations managers monitor delivery speed and driver ratings; finance teams track revenue by payment channel; marketing analyses customer segments and booking patterns."

5. "additional_context": MAX 50 WORDS. Include: actual date ranges, key business categories from the data, and how tables relate to each other — all in plain business language. Example: "Data covers Jan 2024 to Mar 2025. Booking classes include Economy, Business, and First. Orders link to customers, deliveries, and payments. Flight statuses: On-time, Delayed, Cancelled."

6. "sample_questions": JSON array of exactly 10 questions written as a BUSINESS USER would naturally ask.
   RULES for questions:
   - Write in everyday business language — NO column names, NO table names, NO technical syntax
   - Questions must sound like a manager or analyst speaking aloud
   - Cover a mix: totals, trends, comparisons, breakdowns, top/bottom performers
   - Leverage the depth of the data (nested details, tags, categories) but express it in business terms
   - BAD: "What is usage_quantity by billing_origin_product?" (uses column names)
   - GOOD: "Which product categories are driving the highest consumption?"

RULES:
- ALL text must be in plain business language — no column names, table names, or technical identifiers anywhere.
- Use ONLY actual business concepts from the metadata — do not hallucinate or invent.
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

    # ─── Phase 4: Write Config ─────────────────────────────────────────────

    def _write_config(self, generated: Dict[str, Any]) -> Dict[str, Any]:
        updated = dict(self.existing_config)

        # Write table_list instead of catalog/schema
        updated['table_list'] = self.table_list
        # Derive catalog/schema from first table for metric view target
        updated['catalog'] = self.metric_views_catalog or self.primary_catalog
        updated['schema'] = self.metric_views_schema or self.primary_schema
        updated['llm_model'] = self.existing_llm

        for field in self.VALUE_FIELDS:
            if field in generated:
                updated[field] = generated[field]

        lines = [
            "# ============================================================",
            "# AI-Powered Genie Space Generator — Configuration",
            "# ============================================================",
            "# Auto-generated by auto_configure() — review and adjust as needed.",
            "# ============================================================",
            "",
            "# ── Table List (fully qualified: catalog.schema.table) ──────────────",
            "table_list:",
        ]

        for fq in self.table_list:
            lines.append(f'  - "{fq}"')

        lines += [
            "",
            "# ── Derived Target Schema (for metric view creation) ──────────────",
            f"catalog: {self.metric_views_catalog or self.primary_catalog}",
            f"schema: {self.metric_views_schema or self.primary_schema}",
            "",
            "# ── Genie Space ────────────────────────────────────────────────────────",
            f'genie_space_name: "{updated.get("genie_space_name", "Analytics Space")}"',
            "",
            "# ── AI Model ───────────────────────────────────────────────────────────",
            "# Options: databricks-claude-opus-4-6, databricks-meta-llama-3-1-405b-instruct",
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

        with open(self.config_path, 'w') as f:
            f.write("\n".join(lines) + "\n")

        return updated

    # ─── HTML Result Display ───────────────────────────────────────────────

    def _display_result(self, updated: Dict[str, Any]):
        """Display only an HTML banner with config link — no text output."""
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
                        Configuration Generated Successfully
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
                    &#x1f4ca; <strong>{n_tables}</strong> tables configured
                </div>
                <div style="font-size: 13px; color: #33691e;">
                    &#x2753; <strong>{n_questions}</strong> sample questions
                </div>
                <div style="font-size: 13px; color: #33691e;">
                    &#x1f916; <strong>{self.model}</strong>
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
                    Review the generated values, then run the <strong>Run Framework</strong> cell.
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
                   sample_rows: int = 5, max_workers: int = 10,
                   metric_views_catalog: str = None, 
                   metric_views_schema: str = None) -> Dict[str, Any]:
    """
    Analyse specified tables and auto-update config.yaml values.
    
    Args:
        table_list: List of fully qualified table names (catalog.schema.table)
        config_path: Path to config.yaml
    """
    configurator = AutoConfigurator(
        table_list=table_list, config_path=config_path,
        llm_model=llm_model, model_pool=model_pool,
        sample_rows=sample_rows, max_workers=max_workers,
        metric_views_catalog = metric_views_catalog,
        metric_views_schema = metric_views_schema
    )
    return configurator.run()
