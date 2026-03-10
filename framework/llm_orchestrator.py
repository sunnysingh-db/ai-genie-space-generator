"""
LLM Orchestrator Module (Multi-Step Pipeline)
Manages AI-powered generation of metric definitions, semantics, and Genie space configurations
using Databricks Foundation Model API with 6 focused LLM calls. Supports multiple models including
Claude Opus 4, Llama 3.1, and DBRX.
"""

import warnings
# Suppress LangChain deprecation warnings
warnings.filterwarnings('ignore', message='.*databricks_langchain.*')

from langchain_databricks import ChatDatabricks
from typing import Dict, Any, List
import json
import re
import time

class LLMOrchestrator:
    """Orchestrates multi-step LLM-powered generation of metric views and Genie configurations."""
    
    def __init__(self, business_context: str, llm_model: str = "databricks-claude-opus-4-6", sample_questions: list = None):
        """
        Initialize LLM orchestrator with specified foundation model.
        
        Args:
            business_context: Business context describing what metrics matter
            llm_model: Databricks Foundation Model endpoint (default: databricks-claude-opus-4-6)
            sample_questions: User-provided sample questions from config (drives metric generation)
        """
        self.business_context = business_context
        self.llm_model = llm_model
        self.sample_questions = sample_questions or []
        
        # Build sample questions text for injection into LLM prompts
        if self.sample_questions:
            self._questions_prompt = "\nSAMPLE BUSINESS QUESTIONS (generate metrics to answer these):\n" + "\n".join(
                f"  - {q}" for q in self.sample_questions
            ) + "\n"
        else:
            self._questions_prompt = ""
        
        # Suppress LangChain deprecation warning
        import warnings
        warnings.filterwarnings('ignore', category=Warning)
        
        self.llm = ChatDatabricks(
            endpoint=llm_model,
            temperature=0,
            timeout=900  # 15 minutes for longer generations (increased from 10)
            # No max_tokens limit - Foundation models can generate comprehensive outputs
        )
    
    def generate_metrics_config(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate comprehensive metrics configuration using multi-step LLM pipeline.
        Optimized with maximum parallelism across 3 phases.
        
        Phase 1: Filter tables (sequential - all other steps depend on this)
        Phase 2: Dimensions + Measures + Joins (sequential to avoid rate limits)
        Phase 3: Sample questions + Business instructions (both in parallel)
        
        Args:
            metadata: Scanned metadata including tables, columns, relationships, samples
            
        Returns:
            Dictionary with:
                - relevant_tables: Tables selected for metrics
                - dimensions: Dimension definitions (15+)
                - measures: Measure definitions (20+)
                - joins: Join definitions between tables
                - table_descriptions: Descriptions for each table
                - column_descriptions: Descriptions for each column
                - sample_questions: Sample questions with SQL
                - business_instructions: LLM-generated analyst-focused documentation
        """
        print(f"\U0001f916 Starting multi-step LLM pipeline with {self.llm_model}...")
        print()
        step_timings = {}
        
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            # ================================================================
            # Phase 1: Filter relevant tables (sequential)
            # ================================================================
            print("Step 1/6: Filtering relevant tables...")
            t0 = time.time()
            relevant_tables = self.filter_relevant_tables(metadata)
            step_timings['filter_tables'] = time.time() - t0
            print(f"\u2705 Selected {len(relevant_tables)} relevant tables ({step_timings['filter_tables']:.1f}s)\n")
            
            # Filter metadata to only relevant tables
            filtered_metadata = self._filter_metadata(metadata, relevant_tables)
            
            # ================================================================
            # Phase 2: Dimensions → Measures → Joins (sequential)
            # Runs sequentially to avoid overwhelming the foundation model
            # endpoint with too many concurrent calls.
            # generate_measures internally uses 3 parallel sub-workers
            # which is the max concurrency (3 instead of 5).
            # ================================================================
            t_phase2 = time.time()

            # Phase 2a: Dimensions (1 LLM call)
            print("Step 2/6: Generating dimensions...")
            t0 = time.time()
            dimensions = self.generate_dimensions(filtered_metadata)
            step_timings['dimensions'] = time.time() - t0
            print(f"  \u2705 Generated {len(dimensions)} dimensions ({step_timings['dimensions']:.1f}s)")

            # Phase 2b: Measures (3 parallel sub-workers internally — max 3 concurrent)
            print("Step 3/6: Generating measures...")
            t0 = time.time()
            measures = self.generate_measures(filtered_metadata)
            step_timings['measures'] = time.time() - t0
            print(f"  \u2705 Generated {len(measures)} measures ({step_timings['measures']:.1f}s)")

            # Phase 2c: Joins & Semantics (1 LLM call, no contention)
            print("Step 4/6: Generating joins and semantics...")
            t0 = time.time()
            semantics = self.generate_joins_and_semantics(filtered_metadata)
            step_timings['joins'] = time.time() - t0
            print(f"  \u2705 Generated {len(semantics.get('joins', []))} joins ({step_timings['joins']:.1f}s)")

            phase2_time = time.time() - t_phase2
            print(f"  \u23f1\ufe0f  Phase 2 total (wall clock): {phase2_time:.1f}s\n")
            
            # ================================================================
            # Phase 3: Sample questions + Business instructions (parallel)
            # ================================================================
            print("Steps 5 & 6: Generating questions and instructions in parallel...")
            t_phase3 = time.time()
            questions = None
            business_instructions = None
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_questions = executor.submit(
                    self.generate_sample_questions, dimensions, measures, semantics['joins']
                )
                future_instructions = executor.submit(
                    self.generate_business_instructions, dimensions, measures
                )
                
                futures_map = {
                    future_questions: 'questions',
                    future_instructions: 'instructions'
                }
                
                for future in as_completed(futures_map.keys()):
                    step_name = futures_map[future]
                    elapsed = time.time() - t_phase3
                    if step_name == 'questions':
                        questions = future.result()
                        step_timings['questions'] = elapsed
                        print(f"  \u2705 Generated {len(questions)} sample questions ({elapsed:.1f}s)")
                    elif step_name == 'instructions':
                        business_instructions = future.result()
                        step_timings['instructions'] = elapsed
                        print(f"  \u2705 Generated business instructions ({len(business_instructions)} chars, {elapsed:.1f}s)")
            
            phase3_time = time.time() - t_phase3
            print(f"  \u23f1\ufe0f  Phase 3 total (wall clock): {phase3_time:.1f}s\n")
            
            # All questions (config + additional) now come from LLM with SQL
            # Deduplicate by question text
            merged_questions = []
            seen = set()
            for q in questions:
                q_text = q.get('question', '').lower().strip()
                if q_text and q_text not in seen:
                    merged_questions.append(q)
                    seen.add(q_text)
            
            result = {
                'relevant_tables': relevant_tables,
                'dimensions': dimensions,
                'measures': measures,
                'joins': semantics['joins'],
                'table_descriptions': semantics['table_descriptions'],
                'column_descriptions': semantics['column_descriptions'],
                'sample_questions': merged_questions,
                'business_instructions': business_instructions
            }
            
            print("="*80)
            print("\U0001f4ca Multi-Step LLM Pipeline Complete")
            print("="*80)
            print(f"  \u2022 Relevant Tables: {len(relevant_tables)}")
            print(f"  \u2022 Dimensions: {len(dimensions)}")
            print(f"  \u2022 Measures: {len(measures)}")
            print(f"  \u2022 Joins: {len(semantics['joins'])}")
            print(f"  \u2022 Sample Questions: {len(merged_questions)} ({len(self.sample_questions)} from config + {len(questions)} LLM-generated)")
            print(f"  \u2022 Business Instructions: Generated")
            print()
            print("  \u23f1\ufe0f  Step Timings:")
            for step, t in step_timings.items():
                print(f"     {step}: {t:.1f}s")
            print("="*80)
            
            return result
        
        except Exception as e:
            print(f"\u274c LLM pipeline failed: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def filter_relevant_tables(self, metadata: Dict[str, Any]) -> List[str]:
        """
        Step 1: Filter and select 5-10 most relevant tables based on business context.
        
        Args:
            metadata: Full metadata from all tables
            
        Returns:
            List of relevant table names
        
        """
        tables_info = "\n".join([
            f"  - {t['table_name']}: {t.get('comment', 'No description')} (Columns: {self._get_column_count(t['table_name'], metadata)})"
            for t in metadata['tables']
        ])
        
        prompt = f"""You are a data analyst. Select 5-10 most relevant tables for the following business context.

BUSINESS CONTEXT:
{self.business_context}
{self._questions_prompt}
AVAILABLE TABLES:
{tables_info}

TASK:
Return a JSON array of the 5-10 most relevant table names for this business context.
Consider which tables are needed to answer the sample business questions above.

CRITICAL: Your response must be ONLY a JSON array of strings. No explanations, no markdown.
Example format: ["table1", "table2", "table3"]

Start your response with [ and end with ]
"""
        
        response = self.llm.invoke(prompt)
        return self._parse_json_response(response.content, expected_type=list)
    
    def generate_dimensions(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Step 2: Generate MINIMUM 20 dimensions covering temporal, categorical, and numeric types.
        
        Args:
            metadata: Filtered metadata for relevant tables
            
        Returns:
            List of dimension definitions with synonyms
        """
        # Build column information
        columns_by_type = {'temporal': [], 'categorical': [], 'numeric': []}
        for col in metadata['columns']:
            col_name = col['column_name']
            data_type = col['data_type'].lower()
            table = col['table_name']
            
            if any(x in data_type for x in ['timestamp', 'date', 'time']):
                columns_by_type['temporal'].append(f"{table}.{col_name} ({data_type})")
            elif any(x in data_type for x in ['int', 'long', 'double', 'float', 'decimal']):
                columns_by_type['numeric'].append(f"{table}.{col_name} ({data_type})")
            else:
                columns_by_type['categorical'].append(f"{table}.{col_name} ({data_type})")
        
        temporal_cols = "\n  ".join(columns_by_type['temporal'][:20])
        categorical_cols = "\n  ".join(columns_by_type['categorical'][:30])
        numeric_cols = "\n  ".join(columns_by_type['numeric'][:20])
        
        prompt = f"""You are a data analyst creating dimensions for analytics.

BUSINESS CONTEXT:
{self.business_context}
{self._questions_prompt}
AVAILABLE COLUMNS:

Temporal Columns (dates, timestamps):
  {temporal_cols}

Categorical Columns (status, type, category):
  {categorical_cols}

Numeric Columns (can be used as dimensions if categorical in nature):
  {numeric_cols}

TASK:
Generate MINIMUM 20 dimensions covering:
- Temporal dimensions (5+): date columns, timestamps, time periods
- Categorical dimensions (7+): status, type, category, classification fields
- Numeric dimensions (3+): ratings, scores, counts that are categorical in nature

For each dimension provide:
{{
  "name": "dimension_name",
  "column": "column_name",
  "table": "table_name",
  "type": "temporal|categorical|numeric",
  "description": "What this dimension represents in business terms",
  "synonyms": ["synonym1", "synonym2", "synonym3"]
}}

CRITICAL: Your response must be ONLY a JSON array. No explanations, no markdown, no code blocks.
Start with [ and end with ].
Generate at least 20 dimensions.

Example start: [{{"name": "order_date", "column": "order_date", "table": "orders", ...
"""
        
        response = self.llm.invoke(prompt)
        dimensions = self._parse_json_response(response.content, expected_type=list)
        
        # Validate minimum 15 dimensions
        if len(dimensions) < 15:
            print(f"\u26a0\ufe0f  Warning: Only {len(dimensions)} dimensions generated, target was 15+")
        
        return dimensions
    
    def generate_measures(self, metadata: Dict[str, Any], dimensions: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Step 3: Generate 30 measures using 3 parallel LLM calls (10 each).
        
        Splits measure generation into 3 focused workers running concurrently:
          - Worker A: Simple Aggregates + Statistical (5 measures)
          - Worker B: Ratios + Percentages (10 measures)
          - Worker C: Derived / Business KPIs (15 measures)
        
        Results are merged and deduplicated by measure name.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Prepare shared context
        numeric_cols = []
        for col in metadata['columns']:
            data_type = col['data_type'].lower()
            if any(x in data_type for x in ['int', 'long', 'double', 'float', 'decimal']):
                numeric_cols.append(f"{col['table_name']}.{col['column_name']}")
        numeric_cols_str = ", ".join(numeric_cols[:30])
        
        if dimensions:
            dimensions_str = ", ".join([d['name'] for d in dimensions[:10]])
        else:
            dim_cols = []
            for col in metadata['columns']:
                data_type = col['data_type'].lower()
                if any(x in data_type for x in ['timestamp', 'date', 'time', 'string', 'varchar']):
                    dim_cols.append(f"{col['table_name']}.{col['column_name']}")
            dimensions_str = ", ".join(dim_cols[:10])
        
        shared_context = {
            "numeric_cols_str": numeric_cols_str,
            "dimensions_str": dimensions_str
        }
        
        import time as _time
        t0 = _time.time()
        all_measures = []
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_a = executor.submit(self._generate_measures_simple, shared_context)
            future_b = executor.submit(self._generate_measures_ratios, shared_context)
            future_c = executor.submit(self._generate_measures_derived, shared_context)
            
            workers = {
                future_a: 'Worker A (Simple+Statistical)',
                future_b: 'Worker B (Ratios+Percentages)',
                future_c: 'Worker C (Derived/Business KPIs)'
            }
            
            for future in as_completed(workers.keys()):
                worker_name = workers[future]
                elapsed = _time.time() - t0
                try:
                    result = future.result()
                    all_measures.extend(result)
                    print(f"     {worker_name}: {len(result)} measures ({elapsed:.1f}s)")
                except Exception as e:
                    print(f"     \u26a0\ufe0f {worker_name} failed: {str(e)[:100]} ({elapsed:.1f}s)")
        
        # Deduplicate by measure name (first occurrence wins)
        seen_names = set()
        unique_measures = []
        for m in all_measures:
            name = m.get('name', '').lower()
            if name and name not in seen_names:
                unique_measures.append(m)
                seen_names.add(name)
        
        total_time = _time.time() - t0
        print(f"     Parallel measures total: {len(unique_measures)} unique ({total_time:.1f}s wall clock)")
        
        if len(unique_measures) < 20:
            print(f"\u26a0\ufe0f  Warning: Only {len(unique_measures)} measures generated, target was 20+")
        
        return unique_measures
    
    def _build_measure_prompt_header(self, shared_context: Dict[str, Any]) -> str:
        """Build the shared header for all measure worker prompts."""
        return f"""You are a data analyst creating measures/metrics for analytics.

BUSINESS CONTEXT:
{self.business_context}
{self._questions_prompt}
AVAILABLE NUMERIC COLUMNS:
{shared_context['numeric_cols_str']}

AVAILABLE DIMENSIONS:
{shared_context['dimensions_str']}
"""
    
    def _generate_measures_simple(self, shared_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Worker A: Generate 10 Simple Aggregate + Statistical measures."""
        header = self._build_measure_prompt_header(shared_context)
        
        prompt = f"""{header}
TASK:
Generate EXACTLY 10 measures covering Simple Aggregates and Statistical metrics ONLY:

- Simple Aggregates (3): COUNT(*), COUNT(DISTINCT column), SUM(column), AVG(column), MIN(column), MAX(column)
- Statistical (2): PERCENTILE_CONT, MEDIAN, STDDEV, VARIANCE

For each measure provide:
{{{{
  "name": "measure_name",
  "display_name": "Human Readable Name",
  "formula": "Valid SQL aggregation formula",
  "type": "simple",
  "description": "What this metric measures in business terms",
  "synonyms": ["synonym1", "synonym2", "synonym3"]
}}}}

CRITICAL: Your response must be ONLY a JSON array. No explanations, no markdown.
Start with [ and end with ].
Generate EXACTLY 10 measures. No more, no less.
Ensure formulas are valid SQL.
NEVER nest aggregate functions inside other aggregates (e.g. AVG(SUM(x)) is INVALID).
"""
        response = self.llm.invoke(prompt)
        return self._parse_json_response(response.content, expected_type=list)
    
    def _generate_measures_ratios(self, shared_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Worker B: Generate 15 Ratio + Percentage measures."""
        header = self._build_measure_prompt_header(shared_context)
        
        prompt = f"""{header}
TASK:
Generate EXACTLY 15 measures covering Ratios and Percentages ONLY:

- Ratios (6): Division between two aggregates (e.g., revenue per order, average fare per passenger)
  Example: {{{{"name": "revenue_per_booking", "formula": "SUM(total_fare) / COUNT(DISTINCT booking_id)", "type": "derived"}}}}

- Percentages (4): Proportion calculations expressed as percentages
  Example: {{{{"name": "cancellation_rate", "formula": "COUNT(CASE WHEN status='Cancelled' THEN 1 END) * 100.0 / COUNT(*)", "type": "derived"}}}}

For each measure provide:
{{{{
  "name": "measure_name",
  "display_name": "Human Readable Name",
  "formula": "Valid SQL aggregation formula",
  "type": "derived",
  "description": "What this metric measures in business terms",
  "synonyms": ["synonym1", "synonym2", "synonym3"]
}}}}

CRITICAL: Your response must be ONLY a JSON array. No explanations, no markdown.
Start with [ and end with ].
Generate EXACTLY 15 measures. No more, no less.
Ensure formulas are valid SQL.
NEVER nest aggregate functions inside other aggregates (e.g. AVG(SUM(x)) is INVALID). Use ratios instead.
"""
        response = self.llm.invoke(prompt)
        return self._parse_json_response(response.content, expected_type=list)
    
    def _generate_measures_derived(self, shared_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Worker C: Generate 20 Derived / Business KPI measures."""
        header = self._build_measure_prompt_header(shared_context)
        
        prompt = f"""{header}
TASK:
Generate EXACTLY 20 measures covering Derived and Business-Specific KPIs ONLY:

These are complex, multi-column business calculations such as:
- Conditional aggregations using CASE WHEN
- Multi-step business KPIs (e.g., customer lifetime value, load factor, yield)
- Window-function-style calculations expressed as aggregates
- Compound metrics combining multiple columns or tables

Examples:
- {{{{"name": "premium_cabin_share", "formula": "COUNT(CASE WHEN cabin_class IN ('Business','First') THEN 1 END) * 100.0 / COUNT(*)", "type": "derived"}}}}
- {{{{"name": "ancillary_revenue_per_pax", "formula": "SUM(taxes_and_fees) / COUNT(DISTINCT passenger_id)", "type": "derived"}}}}

For each measure provide:
{{{{
  "name": "measure_name",
  "display_name": "Human Readable Name",
  "formula": "Valid SQL aggregation formula",
  "type": "derived",
  "description": "What this metric measures in business terms",
  "synonyms": ["synonym1", "synonym2", "synonym3"]
}}}}

CRITICAL: Your response must be ONLY a JSON array. No explanations, no markdown.
Start with [ and end with ].
Generate EXACTLY 20 measures. No more, no less.
Ensure formulas are valid SQL.
NEVER nest aggregate functions inside other aggregates (e.g. AVG(SUM(x)) is INVALID). Use ratios instead.
"""
        response = self.llm.invoke(prompt)
        return self._parse_json_response(response.content, expected_type=list)


    def generate_joins_and_semantics(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Step 4: Generate joins between tables and semantic descriptions for tables/columns.
        
        Args:
            metadata: Filtered metadata for relevant tables
            
        Returns:
            Dictionary with joins, table_descriptions, column_descriptions
        """
        # Format tables
        tables_info = "\n".join([
            f"  - {t['table_name']}: {t.get('comment', 'No description')}"
            for t in metadata['tables']
        ])
        
        # Format columns by table
        columns_by_table = {}
        for col in metadata['columns']:
            table = col['table_name']
            if table not in columns_by_table:
                columns_by_table[table] = []
            columns_by_table[table].append(f"{col['column_name']} ({col['data_type']})")
        
        columns_info = "\n".join([
            f"  {table}:\n    " + ", ".join(cols[:15])
            for table, cols in list(columns_by_table.items())[:10]
        ])
        
        # Format detected relationships
        relationships_info = "\n".join([
            f"  - {r['from_table']}.{r['from_column']} \u2192 {r['to_table']}.{r['to_column']}"
            for r in metadata['relationships'][:10]
        ]) or "  None detected"
        
        prompt = f"""You are a data analyst creating joins and semantic descriptions.

BUSINESS CONTEXT:
{self.business_context}

TABLES:
{tables_info}

COLUMNS BY TABLE:
{columns_info}

DETECTED RELATIONSHIPS:
{relationships_info}

TASK:
Generate a JSON object with:
1. joins: Array of join definitions between tables
   - relationship_type indicates the cardinality:
     * MANY_TO_ONE: Multiple left rows map to one right row (e.g., orders → customers)
     * ONE_TO_MANY: One left row maps to multiple right rows (e.g., customers → orders)
     * ONE_TO_ONE: One left row maps to at most one right row (e.g., user → user_profile)
2. table_descriptions: Object mapping table names to business descriptions
3. column_descriptions: Object mapping "table.column" to business descriptions

Format:
{{
  "joins": [
    {{
      "left_table": "table1",
      "right_table": "table2",
      "join_type": "INNER|LEFT|RIGHT",
      "condition": "table1.column = table2.column",
      "relationship_type": "MANY_TO_ONE|ONE_TO_MANY|ONE_TO_ONE"
    }}
  ],
  "table_descriptions": {{
    "table_name": "Detailed business description of what this table contains"
  }},
  "column_descriptions": {{
    "table.column": "What this column represents in business terms"
  }}
}}

CRITICAL: Your response must be ONLY a JSON object. No explanations, no markdown.
Start with {{ and end with }}.
Keep descriptions concise (1-2 sentences each).

Example start: {{"joins": [{{"left_table": "orders", ...
"""
        
        response = self.llm.invoke(prompt)
        return self._parse_json_response(response.content, expected_type=dict)
    
    def generate_sample_questions(self, dimensions: List[Dict], measures: List[Dict], joins: List[Dict]) -> List[Dict[str, str]]:
        """
        Step 5: Generate SQL for config questions + additional questions.
        
        Args:
            dimensions: Generated dimensions
            measures: Generated measures
            joins: Generated joins
            
        Returns:
            List of sample questions with SQL and descriptions
        """
        dimensions_str = ", ".join([d['name'] for d in dimensions[:10]])
        measures_str = ", ".join([m['name'] for m in measures[:10]])
        
        # Include config questions so the LLM generates SQL for them
        config_questions_block = ""
        if self.sample_questions:
            config_questions_block = "\nUSER-PROVIDED QUESTIONS (generate SQL for these first):\n" + "\n".join(
                f"  {i+1}. {q}" for i, q in enumerate(self.sample_questions)
            ) + "\n"
        
        prompt = f"""You are a business analyst creating sample questions for a BI tool.

BUSINESS CONTEXT:
{self.business_context}

AVAILABLE DIMENSIONS:
{dimensions_str}

AVAILABLE MEASURES:
{measures_str}
{config_questions_block}
TASK:
Generate sample questions with SQL. Include ALL user-provided questions above with SQL,
plus 5 additional questions. Each question must have a valid SQL query.

For each question provide:
{{
  "question": "Natural language question users might ask",
  "sql": "Simple SQL query (keep under 150 characters)",
  "description": "What business insight this provides"
}}

CRITICAL: Your response must be ONLY a JSON array. No explanations, no markdown.
Start with [ and end with ].
Keep SQL queries SHORT and simple.

Example start: [{{"question": "What is the total revenue by month?", "sql": "SELECT DATE_TRUNC('month', order_date), SUM(revenue) FROM orders GROUP BY 1", ...
"""
        
        response = self.llm.invoke(prompt)
        return self._parse_json_response(response.content, expected_type=list)
    
    def generate_business_instructions(self, dimensions: List[Dict], measures: List[Dict]) -> str:
        """
        Step 6: Generate comprehensive business analyst instructions (LLM-generated, not hardcoded).
        
        Args:
            dimensions: Generated dimensions
            measures: Generated measures
            
        Returns:
            Markdown-formatted business instructions
        """
        # Prepare dimensions and measures summary
        dims_summary = "\n".join([
            f"  - {d['name']}: {d.get('description', 'N/A')}"
            for d in dimensions[:15]
        ])
        
        measures_summary = "\n".join([
            f"  - {m['name']} ({m.get('formula', 'N/A')}): {m.get('description', 'N/A')}"
            for m in measures[:20]
        ])
        
        # Compact, KPI-driven prompt
        prompt = f"""You are a business intelligence analyst creating a quick-reference guide.

BUSINESS CONTEXT:
{self.business_context}

AVAILABLE DIMENSIONS (first 15):
{dims_summary}

AVAILABLE MEASURES (first 20):
{measures_summary}

TASK:
Create a CONCISE, bulleted business analyst guide. Use this structure:


## \U0001f4a1 Quick Analysis Patterns
* 2 common analysis scenarios (1 line each)


## \u26a1 Best Practices
* 10 actionable tips (1 line each)
* Include date ranges, thresholds, or filters

Generate insstructions such  that requires natural language explanation, such as "When users ask about customer performance without specifying a time range, ask them to clarify the time period" or "Always round percentages to two decimal places in summaries.

Also, include this instruction tailored to use case :  When users ask about any KPI breakdown but don't include time range, or which KPIs in their prompt, you must ask a clarification question first to gather necessary information. For example: "Please specify the time range and sales channel you are looking for."

Keep each bullet to ONE line. Focus on actionable business value. Be concise.

CRITICAL: Return plain text only in structured bullets. No JSON, no code blocks, no markdown.
"""
        
        try:
            print("   Generating comprehensive business instructions...")
            response = self.llm.invoke(prompt)
            instructions = response.content.strip()
            
            # Validate we got meaningful content
            if len(instructions) < 200:
                raise ValueError("Generated instructions too short, using fallback")
            
            return instructions
            
        except Exception as e:
            print(f"   \u26a0\ufe0f  LLM generation failed ({str(e)[:100]}), using fallback template")
            
            # Fallback: Generate simple template-based instructions
            return self._generate_fallback_instructions(dimensions, measures)
    
    # ========================================================================
    # Helper Methods
    # ========================================================================
    
    def _generate_fallback_instructions(self, dimensions: List[Dict], measures: List[Dict]) -> str:
        """Generate simple template-based instructions when LLM fails."""
        instructions = f"""# Business Analyst Guide

## Overview
This Genie space provides access to key metrics and dimensions for {self.business_context[:200]}...

## Key Metrics

"""
        # Add top measures
        for i, measure in enumerate(measures[:10], 1):
            name = measure.get('display_name', measure['name'])
            desc = measure.get('description', 'No description')
            instructions += f"{i}. **{name}**: {desc}\n"
        
        instructions += "\n## Available Dimensions\n\n"
        
        # Add top dimensions
        for dim in dimensions[:10]:
            name = dim['name']
            desc = dim.get('description', 'No description')
            instructions += f"- **{name}**: {desc}\n"
        
        instructions += """

## Best Practices

- Use date filters to focus on recent data (last 7, 30, or 90 days)
- Combine metrics with dimensions to slice data by different categories
- Start with high-level aggregates, then drill down into details
- Review data freshness before making business decisions

## Getting Started

Ask questions in natural language, such as:
- "What is the total revenue this month?"
- "Show me top customers by spending"
- "How do metrics compare across regions?"
"""
        return instructions
    
    def _filter_metadata(self, metadata: Dict[str, Any], relevant_tables: List[str]) -> Dict[str, Any]:
        """Filter metadata to only include relevant tables."""
        return {
            'tables': [t for t in metadata['tables'] if t['table_name'] in relevant_tables],
            'columns': [c for c in metadata['columns'] if c['table_name'] in relevant_tables],
            'samples': {k: v for k, v in metadata['samples'].items() if k in relevant_tables},
            'relationships': [
                r for r in metadata['relationships'] 
                if r['from_table'] in relevant_tables and r['to_table'] in relevant_tables
            ]
        }
    
    def _get_column_count(self, table_name: str, metadata: Dict[str, Any]) -> int:
        """Count columns for a specific table."""
        return len([c for c in metadata['columns'] if c['table_name'] == table_name])
    
    def _parse_json_response(self, response: str, expected_type: type) -> Any:
        """
        Parse LLM JSON response with robust error handling.
        
        Args:
            response: LLM response string
            expected_type: Expected type (list or dict)
            
        Returns:
            Parsed JSON object
        """
        # Remove markdown code blocks if present
        response = response.strip()
        if response.startswith("```json"):
            response = response[7:]
        elif response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]
        
        response = response.strip()
        
        # Attempt 1: Direct JSON parse
        try:
            result = json.loads(response)
            if not isinstance(result, expected_type):
                raise ValueError(f"Expected {expected_type.__name__}, got {type(result).__name__}")
            return result
        except json.JSONDecodeError as e:
            print(f"\u26a0\ufe0f  Initial JSON parse failed: {str(e)[:100]}")
        
        # Attempt 2: Find and extract JSON object/array
        try:
            if expected_type == list:
                start = response.find('[')
                end = response.rfind(']')
            else:
                start = response.find('{')
                end = response.rfind('}')
            
            if start >= 0 and end > start:
                json_str = response[start:end+1]
                result = json.loads(json_str)
                if not isinstance(result, expected_type):
                    raise ValueError(f"Expected {expected_type.__name__}, got {type(result).__name__}")
                return result
        except Exception as e2:
            print(f"\u26a0\ufe0f  JSON extraction failed: {str(e2)[:100]}")
        
        # Attempt 3: Return empty structure
        print(f"\u26a0\ufe0f  Returning empty {expected_type.__name__} due to parse failure")
        return expected_type()
