"""
Auto-Configurator Module
Scans Unity Catalog schema metadata and auto-generates config.yaml values using AI.
Eliminates the need for manual config editing.
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
    """Analyses a catalog.schema and auto-populates config.yaml using an LLM."""

    VALUE_FIELDS = [
        'genie_space_name', 'exclude_table_patterns',
        'business_domain', 'data_description',
        'stakeholders_and_decisions', 'additional_context',
        'sample_questions'
    ]

    def __init__(self, catalog: str, schema: str, config_path: str,
                 llm_model: str = None, sample_rows: int = 5, max_workers: int = 10):
        self.catalog = catalog
        self.schema = schema
        self.config_path = config_path
        self.sample_rows = sample_rows
        self.max_workers = max_workers
        self.spark = SparkSession.builder.getOrCreate()

        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.existing_config = yaml.safe_load(f.read()) or {}
        else:
            self.existing_config = {}

        self.existing_llm = self.existing_config.get('llm_model', 'databricks-claude-opus-4-6')
        self.model = llm_model or self.existing_llm

    # ─── Public API ────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """
        Execute the full auto-configure pipeline silently.
        Only output is an HTML banner with a link to open config.yaml.
        """
        all_tables, all_columns, samples = self._scan_metadata()
        metadata_summary = self._build_summary(all_tables, all_columns, samples)
        generated = self._call_llm(metadata_summary)
        updated = self._write_config(generated)
        self._display_result(updated)
        return updated

    # ─── Phase 1: Metadata Scan ────────────────────────────────────────────

    def _scan_metadata(self):
        qcat, qsch = self._quote(self.catalog), self._quote(self.schema)

        tables_df = self.spark.sql(f"""
            SELECT table_name, comment
            FROM {qcat}.information_schema.tables
            WHERE table_schema = '{self.schema}' AND table_type = 'MANAGED'
            ORDER BY table_name
        """)
        all_tables = [row.asDict() for row in tables_df.collect()]
        table_names = [t['table_name'] for t in all_tables]

        columns_df = self.spark.sql(f"""
            SELECT table_name, column_name, data_type, comment
            FROM {qcat}.information_schema.columns
            WHERE table_schema = '{self.schema}'
            ORDER BY table_name, ordinal_position
        """)
        all_columns = [row.asDict() for row in columns_df.collect()]

        samples = {}
        def _sample(tbl_name):
            try:
                rows = self.spark.table(
                    f"{qcat}.{qsch}.{self._quote(tbl_name)}"
                ).limit(self.sample_rows).collect()
                return tbl_name, [row.asDict() for row in rows], None
            except Exception as e:
                return tbl_name, [], str(e)

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(_sample, t): t for t in table_names}
            for fut in as_completed(futures):
                name, data, err = fut.result()
                samples[name] = data

        return all_tables, all_columns, samples

    # ─── Phase 2: Build Summary ────────────────────────────────────────────

    def _build_summary(self, all_tables, all_columns, samples) -> str:
        cols_by_table = {}
        for c in all_columns:
            cols_by_table.setdefault(c['table_name'], []).append(c)

        parts = []
        for tbl in all_tables:
            tname = tbl['table_name']
            tbl_cols = cols_by_table.get(tname, [])
            col_info = ", ".join(f"{c['column_name']} ({c['data_type']})" for c in tbl_cols)
            part = f"TABLE: {tname}\n  Columns: {col_info}"
            if tbl.get('comment'):
                part += f"\n  Comment: {tbl['comment']}"
            if samples.get(tname):
                sample_str = json.dumps(samples[tname][:3], default=str)
                if len(sample_str) > 1500:
                    sample_str = sample_str[:1500] + "...(truncated)"
                part += f"\n  Sample: {sample_str}"
            parts.append(part)

        return "\n\n".join(parts)

    # ─── Phase 3: LLM Call ─────────────────────────────────────────────────

    def _call_llm(self, metadata_summary: str) -> Dict[str, Any]:
        from langchain_databricks import ChatDatabricks
        import warnings
        warnings.filterwarnings('ignore', category=Warning)

        llm = ChatDatabricks(endpoint=self.model, temperature=0, timeout=900)

        prompt = f"""Analyse this Unity Catalog schema metadata and generate concise configuration values for a Genie Space.

CATALOG: {self.catalog}
SCHEMA: {self.schema}

--- METADATA ---
{metadata_summary}
--- END METADATA ---

Return a JSON object with EXACTLY these keys. STRICT LENGTH LIMITS — exceed them and the output is unusable:

1. "genie_space_name": Max 50 chars. Short analytics space name. Example: "Aviation & Revenue Analytics"

2. "exclude_table_patterns": JSON array of SQL LIKE patterns for system/monitoring/logging/test tables to exclude. Empty array [] if all are business-relevant.

3. "business_domain": MAX 25 WORDS. What the business does. Example: "Food delivery platform operating across 5 cities with 200 restaurants, 50 drivers, and integrated payment processing."

4. "data_description": MAX 30 WORDS. What data the tables track. Example: "Orders, restaurants, drivers, payments, and reviews for food delivery; flights, bookings, airlines, and passengers for aviation operations."

5. "stakeholders_and_decisions": MAX 25 WORDS. Who uses this data and for what. Example: "Ops managers track delivery times and ratings; revenue analysts monitor booking fares and occupancy rates."

6. "additional_context": MAX 25 WORDS. Key date ranges, categories, or patterns. Example: "Orders span Oct 2025–Feb 2026; loyalty tiers: bronze, silver, gold; flight statuses: Completed, Delayed, Cancelled."

7. "sample_questions": JSON array of exactly 10 SHORT questions (MAX 15 WORDS EACH). Example: "What is average delivery time by restaurant?" NOT "What is the average delivery_time_minutes broken down by restaurant_name and how does it compare across different cuisine_type categories?"

RULES:
- Be extremely concise. Every field must respect its word limit.
- Use actual values from the sample data — do not hallucinate.
- Return ONLY valid JSON. No markdown, no explanation, no code fences.
"""

        # Suppress any stdout from langchain internals
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
        updated['catalog'] = self.catalog
        updated['schema'] = self.schema
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
            "# ── Data Location ──────────────────────────────────────────────────────",
            f"catalog: {updated['catalog']}",
            f"schema: {updated['schema']}",
            "",
            "# ── Genie Space ────────────────────────────────────────────────────────",
            f'genie_space_name: "{updated.get("genie_space_name", "Analytics Space")}"',
            "",
            "# ── AI Model ───────────────────────────────────────────────────────────",
            "# Options: databricks-claude-opus-4-6, databricks-meta-llama-3-1-405b-instruct",
            f"llm_model: {updated['llm_model']}",
            "",
            "# ── Table Filtering ────────────────────────────────────────────────────",
            "exclude_table_patterns:",
        ]

        for pat in updated.get('exclude_table_patterns', []):
            lines.append(f'  - "{pat}"')

        lines += [
            "",
            "# ============================================================",
            "# Business Context (auto-generated from schema analysis)",
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

        config_ws_path = self.config_path.replace('/Workspace', '', 1) if self.config_path.startswith('/Workspace') else self.config_path
        space_name = updated.get('genie_space_name', 'Analytics Space')
        n_tables = len(updated.get('exclude_table_patterns', []))
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
                        {self.catalog}.{self.schema} &nbsp;&#x2192;&nbsp; <strong>{space_name}</strong>
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
                    &#x1f4ca; <strong>{n_tables}</strong> exclude patterns
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
                <a href="#workspace{config_ws_path}" target="_blank" style="
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
    def _quote(identifier: str) -> str:
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            return identifier
        return f"`{identifier}`"


# ─── Convenience Function ──────────────────────────────────────────────────

def auto_configure(catalog: str, schema: str, config_path: str,
                   llm_model: str = None, sample_rows: int = 5, max_workers: int = 10) -> Dict[str, Any]:
    """
    Analyse all tables in catalog.schema and auto-update config.yaml values.
    Only output is an HTML banner with a link to open the config file.
    """
    configurator = AutoConfigurator(
        catalog=catalog, schema=schema, config_path=config_path,
        llm_model=llm_model, sample_rows=sample_rows, max_workers=max_workers
    )
    return configurator.run()
