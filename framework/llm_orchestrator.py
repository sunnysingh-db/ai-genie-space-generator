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
        print(f"🤖 Starting multi-step LLM pipeline with {self.llm_model}...")
        print()
        
        try:
            # Step 1: Filter relevant tables
            print("Step 1/6: Filtering relevant tables...")
            relevant_tables = self.filter_relevant_tables(metadata)
            print(f"✅ Selected {len(relevant_tables)} relevant tables\n")
            
            # Filter metadata to only relevant tables
            filtered_metadata = self._filter_metadata(metadata, relevant_tables)
            
            # Steps 2 & 4: Parallelize independent LLM calls
            print("Steps 2 & 4: Generating dimensions and joins in parallel...")
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            dimensions = None
            semantics = None
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                # Submit both independent tasks
                future_dimensions = executor.submit(self.generate_dimensions, filtered_metadata)
                future_semantics = executor.submit(self.generate_joins_and_semantics, filtered_metadata)
                
                # Collect results as they complete
                for future in as_completed([future_dimensions, future_semantics]):
                    if future == future_dimensions:
                        dimensions = future.result()
                        print(f"  ✅ Generated {len(dimensions)} dimensions")
                    elif future == future_semantics:
                        semantics = future.result()
                        print(f"  ✅ Generated {len(semantics.get('joins', []))} joins")
            
            print()
            
            # Step 3: Generate measures (depends on dimensions)
            print("Step 3/6: Generating measures (target: 20+)...")
            measures = self.generate_measures(filtered_metadata, dimensions)
            print(f"✅ Generated {len(measures)} measures\n")
            
            # Step 5: Generate sample questions
            print("Step 5/6: Generating sample questions...")
            questions = self.generate_sample_questions(dimensions, measures, semantics['joins'])
            print(f"✅ Generated {len(questions)} sample questions\n")
            
            # Step 6: Generate business instructions
            print("Step 6/6: Generating business analyst instructions...")
            business_instructions = self.generate_business_instructions(dimensions, measures)
            print(f"✅ Generated business instructions ({len(business_instructions)} chars)\n")
            
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
            print("📊 Multi-Step LLM Pipeline Complete")
            print("="*80)
            print(f"  • Relevant Tables: {len(relevant_tables)}")
            print(f"  • Dimensions: {len(dimensions)}")
            print(f"  • Measures: {len(measures)}")
            print(f"  • Joins: {len(semantics['joins'])}")
            print(f"  • Sample Questions: {len(merged_questions)} ({len(self.sample_questions)} from config + {len(questions)} LLM-generated)")
            print(f"  • Business Instructions: Generated")
            print("="*80)
            
            return result
        
        except Exception as e:
            print(f"❌ LLM pipeline failed: {str(e)}")
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
        Step 2: Generate MINIMUM 15 dimensions covering temporal, categorical, and numeric types.
        
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
Generate MINIMUM 15 dimensions covering:
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
Generate at least 15 dimensions.

Example start: [{{"name": "order_date", "column": "order_date", "table": "orders", ...
"""
        
        response = self.llm.invoke(prompt)
        dimensions = self._parse_json_response(response.content, expected_type=list)
        
        # Validate minimum 15 dimensions
        if len(dimensions) < 15:
            print(f"⚠️  Warning: Only {len(dimensions)} dimensions generated, target was 15+")
        
        return dimensions
    
    def generate_measures(self, metadata: Dict[str, Any], dimensions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Step 3: Generate MINIMUM 20 measures including simple, ratios, percentages, and derived metrics.
        
        Args:
            metadata: Filtered metadata for relevant tables
            dimensions: Generated dimensions
            
        Returns:
            List of measure definitions with formulas and synonyms
        """
        # Get numeric columns for measures
        numeric_cols = []
        for col in metadata['columns']:
            data_type = col['data_type'].lower()
            if any(x in data_type for x in ['int', 'long', 'double', 'float', 'decimal']):
                numeric_cols.append(f"{col['table_name']}.{col['column_name']}")
        
        numeric_cols_str = ", ".join(numeric_cols[:30])
        dimensions_str = ", ".join([d['name'] for d in dimensions[:10]])
        
        prompt = f"""You are a data analyst creating comprehensive measures/metrics for analytics.

BUSINESS CONTEXT:
{self.business_context}
{self._questions_prompt}
AVAILABLE NUMERIC COLUMNS:
{numeric_cols_str}

AVAILABLE DIMENSIONS:
{dimensions_str}

TASK:
Generate MINIMUM 20 measures covering:

1. Simple Aggregates (8+):
   - COUNT(*), COUNT(DISTINCT column), SUM(column), AVG(column), MIN(column), MAX(column)
   - Example: {{"name": "total_orders", "formula": "COUNT(*)", "type": "simple", ...}}

2. Ratios (4+):
   - Division between two aggregates
   - Example: {{"name": "revenue_per_order", "formula": "SUM(revenue) / COUNT(DISTINCT order_id)", "type": "derived", ...}}

3. Percentages (3+):
   - Proportion calculations
   - Example: {{"name": "completion_rate", "formula": "COUNT(CASE WHEN status='completed' THEN 1 END) / COUNT(*) * 100", "type": "derived", ...}}

4. Statistical Aggregates (3+):
   - PERCENTILE_CONT, MEDIAN, STDDEV, VARIANCE
   - Example: {{"name": "median_delivery_time", "formula": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY delivery_time)", "type": "simple", ...}}

5. Derived Metrics (2+):
   - Complex business calculations
   - Example: {{"name": "customer_lifetime_value", "formula": "SUM(revenue) / COUNT(DISTINCT customer_id)", "type": "derived", ...}}

For each measure provide:
{{
  "name": "measure_name",
  "display_name": "Human Readable Name",
  "formula": "Valid SQL aggregation formula",
  "type": "simple|derived",
  "description": "What this metric measures in business terms",
  "synonyms": ["synonym1", "synonym2", "synonym3"]
}}

CRITICAL: Your response must be ONLY a JSON array. No explanations, no markdown.
Start with [ and end with ].
Generate at least 20 measures.
Ensure formulas are valid SQL.

Example start: [{{"name": "total_orders", "display_name": "Total Orders", "formula": "COUNT(*)", ...
"""
        
        response = self.llm.invoke(prompt)
        measures = self._parse_json_response(response.content, expected_type=list)
        
        # Validate minimum 20 measures
        if len(measures) < 20:
            print(f"⚠️  Warning: Only {len(measures)} measures generated, target was 20+")
        
        return measures

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
            f"  - {r['from_table']}.{r['from_column']} → {r['to_table']}.{r['to_column']}"
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
1. joins: Array of join definitions
2. table_descriptions: Object mapping table names to business descriptions
3. column_descriptions: Object mapping "table.column" to business descriptions

Format:
{{
  "joins": [
    {{
      "left_table": "table1",
      "right_table": "table2",
      "join_type": "INNER|LEFT|RIGHT",
      "condition": "table1.column = table2.column"
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

## 📊 Key Performance Indicators
* List 5-7 most critical KPIs with brief definitions (1 line each)
* Format: "**KPI Name**: Brief description and business purpose"

## 🔍 Analysis Dimensions
* List 4-6 key dimensions for slicing data (1 line each)
* Format: "**Dimension**: Use case or filtering purpose"

## 💡 Quick Analysis Patterns
* 4-5 common analysis scenarios (1 line each)
* Format: "**Scenario**: Dimensions + Measures to use"

## ⚡ Best Practices
* 3-4 actionable tips (1 line each)
* Include date ranges, thresholds, or filters

Keep each bullet to ONE line. Focus on actionable business value. Be concise.

CRITICAL: Return markdown text only. No JSON, no code blocks.
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
            print(f"   ⚠️  LLM generation failed ({str(e)[:100]}), using fallback template")
            
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
            print(f"⚠️  Initial JSON parse failed: {str(e)[:100]}")
        
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
            print(f"⚠️  JSON extraction failed: {str(e2)[:100]}")
        
        # Attempt 3: Return empty structure
        print(f"⚠️  Returning empty {expected_type.__name__} due to parse failure")
        return expected_type()
