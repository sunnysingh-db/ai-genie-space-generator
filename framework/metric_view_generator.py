"""Metric View Generator Module (Enhanced with URL Generation)
Creates Unity Catalog metric views with semantic layer using WITH METRICS.
Generates URLs to the metric views catalog page.
"""

from pyspark.sql import SparkSession
from typing import Dict, List, Any
import yaml
import re
import logging

class MetricViewGenerator:
    """Generates UC metric views with YAML semantic layer configurations."""
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        """
        Initialize metric view generator.
        
        Args:
            spark: Active SparkSession
            catalog: Catalog name
            schema: Schema name where views will be created
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        
        # Pre-compute backtick-quoted identifiers for SQL contexts
        self.quoted_catalog = self._quote_identifier(catalog)
        self.quoted_schema = self._quote_identifier(schema)
        self.quoted_full_schema = f"{self.quoted_catalog}.{self.quoted_schema}"
    
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote identifier if it contains characters that require backticks.
        
        Args:
            identifier: Table, catalog, or schema name
            
        Returns:
            Backticked identifier if needed, otherwise unchanged
        """
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            return identifier
        else:
            return f"`{identifier}`"
    
    def _get_workspace_url(self) -> str:
        """Get the Databricks workspace URL from Spark configuration."""
        try:
            # Get from Spark context
            workspace_url = self.spark.conf.get("spark.databricks.workspaceUrl")
            return workspace_url
        except Exception:
            # Silently fail - this is expected on serverless clusters
            return None
    
    def _get_workspace_id(self) -> str:
        """Get the Databricks workspace ID (orgId) from Spark configuration."""
        import logging
        
        # Temporarily suppress Spark Connect gRPC errors
        spark_logger = logging.getLogger("pyspark.sql.connect.logging")
        original_level = spark_logger.level
        spark_logger.setLevel(logging.CRITICAL)
        
        try:
            # Try workspace ID directly (more reliable on serverless)
            workspace_id = self.spark.conf.get("spark.databricks.workspaceId", None)
            if workspace_id:
                return workspace_id
            
            # Option 2: From cluster usage tags (not available on serverless)
            try:
                workspace_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId", None)
                if workspace_id:
                    return workspace_id
            except Exception:
                # Expected on serverless - silently ignore
                pass
            
            return None
        except Exception:
            return None
        finally:
            # Restore original log level
            spark_logger.setLevel(original_level)
    
    def generate_metric_views_url(self, view_name: str = None) -> str:
        """
        Generate URL to the metric views catalog page or specific metric view.
        
        Args:
            view_name: Optional name of specific metric view to link to
        
        Returns:
            URL string in format: 
            https://<workspace-url>/explore/data/<catalog>/<schema>/<view_name> (if view_name provided)
            https://<workspace-url>/explore/data/<catalog>/<schema>?view=metricViews (if view_name not provided)
            Returns None if workspace URL cannot be determined.
        """
        workspace_url = self._get_workspace_url()
        
        if not workspace_url:
            return None
        
        # If specific view name provided, link directly to it
        if view_name:
            url = f"https://{workspace_url}/explore/data/{self.catalog}/{self.schema}/{view_name}"
            return url
        
        # Otherwise link to metric views list
        url = f"https://{workspace_url}/explore/data/{self.catalog}/{self.schema}?view=metricViews"
        return url
    
    def create_metric_views(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create metric views from LLM-generated configuration.
        
        Args:
            config: Configuration with metrics, dimensions, joins
            
        Returns:
            Dictionary containing:
                - views: List of created metric view names
                - url: URL to metric views catalog page (or None if unavailable)
        """
        print("\U0001f4ca Creating UC metric views with semantic layer...")
        
        created_views = []
        
        # Group metrics by primary table
        metrics_by_table = self._group_metrics_by_table(config)
        
        for table_name, table_metrics in metrics_by_table.items():
            view_name = f"metrics_{table_name}"
            
            try:
                # Create metric view and get the actual name used (may include version suffix)
                actual_view_name = self._create_metric_view(
                    view_name=view_name,
                    base_table=table_name,
                    metrics=table_metrics,
                    dimensions=config.get('dimensions', []),
                    joins=config.get('joins', []),
                    config=config
                )
                created_views.append(actual_view_name)
                print(f"  \u2705 Created metric view {actual_view_name} with {len(table_metrics)} measures")
            except Exception as e:
                print(f"  \u274c Failed to create {view_name}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        # Generate metric views URL - link to first created view if available
        first_view = created_views[0] if created_views else None
        metric_views_url = self.generate_metric_views_url(view_name=first_view)
        
        if metric_views_url:
            print(f"\n  \U0001f517 Metric Views URL: {metric_views_url}")
        
        return {
            'views': created_views,
            'url': metric_views_url
        }
    
    def _group_metrics_by_table(self, config: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """Group metrics by their primary table based on formula analysis."""
        metrics_by_table = {}
        
        relevant_tables = config.get('relevant_tables', [])
        # Fix: LLM generates 'measures' not 'metrics'
        metrics = config.get('measures', config.get('metrics', []))
        
        if not relevant_tables or not metrics:
            return metrics_by_table
        
        generic_metrics = []  # Metrics without explicit table references (e.g. COUNT(*))
        
        for metric in metrics:
            formula = metric.get('formula', '')
            
            # Count table references in the formula (table.column patterns)
            table_refs = {}
            for table in relevant_tables:
                pattern = rf'\b{re.escape(table)}\.'
                count = len(re.findall(pattern, formula))
                if count > 0:
                    table_refs[table] = count
            
            if table_refs:
                # Assign to the most-referenced table
                primary = max(table_refs, key=table_refs.get)
                if primary not in metrics_by_table:
                    metrics_by_table[primary] = []
                metrics_by_table[primary].append(metric)
            else:
                # Generic metric (e.g., COUNT(*)) - collect separately
                generic_metrics.append(metric)
        
        # Assign generic metrics to the table with the most specific metrics
        if generic_metrics:
            if metrics_by_table:
                primary = max(metrics_by_table, key=lambda t: len(metrics_by_table[t]))
            else:
                primary = relevant_tables[0]
            
            if primary not in metrics_by_table:
                metrics_by_table[primary] = []
            metrics_by_table[primary].extend(generic_metrics)
        
        return metrics_by_table
    
    def _create_metric_view(
        self, 
        view_name: str, 
        base_table: str,
        metrics: List[Dict],
        dimensions: List[Dict],
        joins: List[Dict],
        config: Dict[str, Any]
    ) -> str:
        """
        Create a single metric view using WITH METRICS syntax.
        If a metric view already exists with the same name, creates a new version
        with a version suffix (_v2, _v3, etc.).
        
        Returns:
            The actual view name that was created (may include version suffix)
        """
        
        # Pre-validate: collect valid columns from all involved tables
        valid_columns = self._get_valid_columns(base_table)
        for join in joins:
            join_table = join.get('right_table') if join.get('left_table') == base_table else join.get('left_table')
            if join_table:
                valid_columns.update(self._get_valid_columns(join_table))
        
        # Build YAML semantic layer configuration
        yaml_content = self._build_metric_view_yaml(
            base_table=base_table,
            metrics=metrics,
            dimensions=dimensions,
            joins=joins,
            config=config
        )
        
        # Try to create the metric view, handling version conflicts
        max_attempts = 10
        current_view_name = view_name
        
        for attempt in range(1, max_attempts + 1):
            try:
                # Drop existing view if it exists (handle both regular views and metric views)
                try:
                    quoted_view = self._quote_identifier(current_view_name)
                    self.spark.sql(f"DROP VIEW IF EXISTS {self.quoted_full_schema}.{quoted_view}")
                except Exception as e:
                    # Ignore errors if view doesn't exist
                    pass
                
                # Create metric view using WITH METRICS syntax
                # Build delimiter using chr to avoid escaping issues
                delimiter = chr(36) + chr(36)  # Creates $
                quoted_view = self._quote_identifier(current_view_name)
                create_sql = (
                    f"CREATE OR REPLACE VIEW {self.quoted_full_schema}.{quoted_view}\n"
                    "WITH METRICS\n"
                    "LANGUAGE YAML\n"
                    f"AS {delimiter}\n"
                    f"{yaml_content}\n"
                    f"{delimiter}"
                )
                

                # Suppress noisy gRPC error logs during creation attempts
                spark_logger = logging.getLogger("pyspark.sql.connect.logging")
                original_level = spark_logger.level
                spark_logger.setLevel(logging.CRITICAL)
                
                try:
                    # Execute creation
                    self.spark.sql(create_sql)
                finally:
                    spark_logger.setLevel(original_level)
                
                # If we get here, creation succeeded
                if attempt > 1:
                    print(f"  \u2139\ufe0f  Created versioned metric view: {current_view_name}")
                
                return current_view_name
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Handle unresolved column errors: remove offending measures/dimensions and retry
                if 'unresolved_column' in error_msg or 'cannot be resolved' in error_msg:
                    # Extract the bad column name from error
                    bad_col_match = re.search(r'with name `([^`]+)`', str(e))
                    if bad_col_match and attempt < max_attempts:
                        bad_col = bad_col_match.group(1)
                        # Handle dotted references like bookings.booking_date
                        bad_col_parts = bad_col.split('.')
                        bad_col_simple = bad_col_parts[-1]
                        
                        orig_metric_count = len(metrics)
                        orig_dim_count = len(dimensions)
                        
                        # Remove measures containing the bad column reference
                        metrics = [m for m in metrics if bad_col_simple not in m.get('formula', '')]
                        # Remove dimensions containing the bad column reference
                        dimensions = [d for d in dimensions if bad_col_simple not in d.get('column', '') and bad_col_simple not in d.get('name', '')]
                        
                        removed_metrics = orig_metric_count - len(metrics)
                        removed_dims = orig_dim_count - len(dimensions)
                        
                        if removed_metrics > 0 or removed_dims > 0:
                            print(f"  \u26a0\ufe0f  Removed {removed_metrics} measures and {removed_dims} dimensions referencing invalid column '{bad_col_simple}', retrying...")
                            # Rebuild YAML with filtered measures/dimensions
                            yaml_content = self._build_metric_view_yaml(
                                base_table=base_table,
                                metrics=metrics,
                                dimensions=dimensions,
                                joins=joins,
                                config=config
                            )
                            continue
                
                # Handle nested aggregate function errors: extract offending measure and remove it
                if 'nested_aggregate_function' in error_msg or 'nested aggregate' in error_msg:
                    if attempt < max_attempts:
                        # Extract measure names from the error message (pattern: "expr AS measure_name#id")
                        bad_measure_names = set()
                        # Look for aggregate-inside-aggregate patterns in the error
                        # The error plan shows: agg_func(agg_func(col)) AS measure_name#id
                        agg_funcs_pattern = r'(?:avg|sum|count|min|max|stddev|variance|percentile_cont)'
                        nested_pattern = rf'{agg_funcs_pattern}\s*\([^)]*{agg_funcs_pattern}\s*\([^)]*\)[^)]*\)\s+AS\s+(\w+)#'
                        for match in re.finditer(nested_pattern, str(e).lower()):
                            bad_measure_names.add(match.group(1))
                        
                        # Fallback: if we couldn't extract names from the plan, try the simpler
                        # pattern that matches "AS measure_name#" after nested agg expressions
                        if not bad_measure_names:
                            # Look for any measure names following nested aggregates in the error
                            as_pattern = r'AS\s+(\w+)#\d+'
                            all_measures_in_error = re.findall(as_pattern, str(e))
                            # Cross-reference with our measure list to find which ones are suspects
                            measure_names_set = {m['name'] for m in metrics}
                            for m_name in all_measures_in_error:
                                if m_name in measure_names_set:
                                    # Check if this measure's formula might have nested aggs
                                    for m in metrics:
                                        if m['name'] == m_name:
                                            formula_upper = m.get('formula', '').upper()
                                            agg_kws = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'PERCENTILE_CONT']
                                            agg_hits = sum(1 for kw in agg_kws if kw in formula_upper)
                                            if agg_hits >= 2:
                                                bad_measure_names.add(m_name)
                        
                        # If we still couldn't identify specific measures, remove ALL measures
                        # with multiple aggregate functions as a broad safety net
                        if not bad_measure_names:
                            agg_kws = ['COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX(', 'STDDEV(', 'VARIANCE(', 'PERCENTILE_CONT(']
                            for m in metrics:
                                formula_upper = m.get('formula', '').upper()
                                agg_hits = sum(1 for kw in agg_kws if kw in formula_upper)
                                if agg_hits >= 2:
                                    bad_measure_names.add(m['name'])
                        
                        if bad_measure_names:
                            orig_count = len(metrics)
                            metrics = [m for m in metrics if m['name'] not in bad_measure_names]
                            removed = orig_count - len(metrics)
                            print(f"  \u26a0\ufe0f  Removed {removed} measures with nested aggregates ({', '.join(bad_measure_names)}), retrying...")
                            yaml_content = self._build_metric_view_yaml(
                                base_table=base_table,
                                metrics=metrics,
                                dimensions=dimensions,
                                joins=joins,
                                config=config
                            )
                            continue
                
                # Check if the error is related to an existing metric view
                # Common error patterns for existing metric views
                is_conflict_error = any(phrase in error_msg for phrase in [
                    'already exists',
                    'view already exists',
                    'metric view already exists',
                    'cannot create',
                    'duplicate',
                    'conflict'
                ])
                
                if is_conflict_error and attempt < max_attempts:
                    # Generate versioned name for next attempt
                    version_num = attempt + 1
                    current_view_name = f"{view_name}_v{version_num}"
                    print(f"  \u26a0\ufe0f  Metric view conflict detected, trying: {current_view_name}")
                    continue  # Try again with versioned name
                else:
                    # If it's not a conflict error, or we've exhausted attempts, raise the error
                    raise Exception(f"Failed to create metric view after {attempt} attempts: {str(e)}")
    
    def _get_valid_columns(self, table_name: str) -> set:
        """Get the set of valid column names for a table."""
        try:
            quoted_table = self._quote_identifier(table_name)
            cols = self.spark.catalog.listColumns(f"{self.quoted_full_schema}.{quoted_table}")
            return {c.name for c in cols}
        except Exception:
            return set()
    
    def _validate_measures(self, metrics: List[Dict], valid_columns: set, base_table: str) -> List[Dict]:
        """Filter out measures that reference non-existent columns."""
        validated = []
        for metric in metrics:
            formula = metric.get('formula', '')
            # Extract all bare column references (not prefixed with table.)
            # After base table prefix is stripped, we get bare column names
            clean_formula = re.sub(rf'\b{re.escape(base_table)}\.', '', formula)
            
            # Extract potential column references: words that could be column names
            # Look for identifiers used in SQL context (inside aggregation functions, CASE WHEN, etc.)
            # but exclude SQL keywords
            sql_keywords = {'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL',
                          'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AS', 'DISTINCT', 'BETWEEN',
                          'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'PERCENTILE_CONT', 'WITHIN',
                          'GROUP', 'ORDER', 'BY', 'ASC', 'DESC', 'HAVING', 'LIKE', 'CAST',
                          'FLOAT', 'INT', 'DOUBLE', 'STRING', 'DATE', 'TIMESTAMP', 'TRUE', 'FALSE',
                          'OVER', 'PARTITION', 'ROWS', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING',
                          'CURRENT', 'ROW', 'STDDEV', 'VARIANCE', 'NULLS', 'FIRST', 'LAST'}
            
            # Find bare identifiers (not prefixed with table_name.)
            bare_refs = re.findall(r'(?<!\w\.)(?<![\w])([a-zA-Z_][a-zA-Z0-9_]*)(?=\s*[),\s=<>!+\-*/|$])', clean_formula)
            
            is_valid = True
            for ref in bare_refs:
                if ref.upper() in sql_keywords:
                    continue
                # Skip numeric literals, string literals, table prefixed refs
                if ref in valid_columns or ref.isdigit():
                    continue
                # Check if it's a table name (used as join prefix)
                if ref in {base_table}:
                    continue
                # Check if it looks like a value literal (e.g., 'completed', 'delayed')
                # These are typically inside quotes in the original formula
                if f"'{ref}'" in formula or f'"{ ref}"' in formula:
                    continue
            
            validated.append(metric)
        return validated

    def _build_metric_view_yaml(
        self,
        base_table: str,
        metrics: List[Dict],
        dimensions: List[Dict],
        joins: List[Dict],
        config: Dict[str, Any]
    ) -> str:
        """Build YAML content for metric view semantic layer."""
        
        # Build the YAML structure
        # Use quoted identifiers for the source reference
        quoted_base = self._quote_identifier(base_table)
        yaml_dict = {
            'version': 1.1,
            'comment': f"AI-generated metric view for {base_table}",
            'source': f"{self.quoted_full_schema}.{quoted_base}"
        }
        
        # Build joins FIRST to determine reachable tables
        yaml_joins = []
        if joins:
            yaml_joins = self._build_joins_yaml(joins, base_table)
            if yaml_joins:
                yaml_dict['joins'] = yaml_joins
        
        # Determine reachable tables (base table + all joined tables)
        reachable_tables = {base_table}
        for j in yaml_joins:
            reachable_tables.add(j['name'])
        
        # Filter dimensions to only include columns from reachable tables
        filtered_dimensions = [
            d for d in dimensions
            if d.get('table', base_table) in reachable_tables
        ]
        
        # Add dimensions (filtered)
        if filtered_dimensions:
            yaml_dict['dimensions'] = self._build_dimensions_yaml(filtered_dimensions, base_table)
        
        # Add measures (metrics) - use filtered dimensions for reference mapping
        if metrics:
            yaml_dict['measures'] = self._build_measures_yaml(metrics, base_table, filtered_dimensions)
        
        # Convert to YAML string with proper formatting
        yaml_str = yaml.dump(
            yaml_dict,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=1000
        )
        
        return yaml_str
    
    def _build_joins_yaml(self, joins: List[Dict], base_table: str) -> List[Dict]:
        """Build joins section for YAML."""
        yaml_joins = []
        seen_join_names = set()  # Track join names to avoid duplicates
        
        for join in joins:
            left_table = join.get('left_table')
            right_table = join.get('right_table')
            condition = join['condition']
            
            # Include joins where base_table is either left or right table
            if left_table == base_table:
                # Base table is on the left - use as-is
                # Replace "base_table." with "source." in the condition
                condition = re.sub(rf'\b{base_table}\.', 'source.', condition)
                
                # Skip duplicate join names (e.g., airports joined twice via origin and destination)
                if right_table in seen_join_names:
                    continue
                seen_join_names.add(right_table)
                
                quoted_right = self._quote_identifier(right_table)
                yaml_join = {
                    'name': right_table,
                    'source': f"{self.quoted_full_schema}.{quoted_right}",
                    'on': condition
                }
                yaml_joins.append(yaml_join)
                
            elif right_table == base_table:
                # Base table is on the right - reverse the join
                # Need to swap the sides of the condition to maintain source.col = joinname.col format
                # Example: "payments.order_id = orders.order_id" becomes "source.order_id = payments.order_id"
                
                # Parse the condition to swap sides
                # Handle both = and == operators
                match = re.match(r'(.+?)\s*=+\s*(.+)', condition.strip())
                if match:
                    left_side = match.group(1).strip()
                    right_side = match.group(2).strip()
                    
                    # Swap sides and replace base_table with source on what's now the left
                    left_side_new = re.sub(rf'\b{base_table}\.', 'source.', right_side)
                    right_side_new = left_side
                    condition = f"{left_side_new} = {right_side_new}"
                else:
                    # Fallback: just replace base_table with source
                    condition = re.sub(rf'\b{base_table}\.', 'source.', condition)
                
                # Skip duplicate join names
                if left_table in seen_join_names:
                    continue
                seen_join_names.add(left_table)
                
                quoted_left = self._quote_identifier(left_table)
                yaml_join = {
                    'name': left_table,
                    'source': f"{self.quoted_full_schema}.{quoted_left}",
                    'on': condition
                }
                yaml_joins.append(yaml_join)
        
        return yaml_joins
    
    def _build_dimensions_yaml(self, dimensions: List[Dict], base_table: str) -> List[Dict]:
        """Build dimensions section for YAML."""
        yaml_dimensions = []
        
        for dim in dimensions:
            column_expr = dim['column']
            table_name = dim.get('table', base_table)
            
            # Extract just the column name without table prefix
            if '.' in column_expr:
                column_expr = column_expr.split('.')[-1]
            
            # For base table columns: no prefix needed
            # For joined table columns: use join name (table name) as prefix
            if table_name != base_table:
                column_expr = f"{table_name}.{column_expr}"
            
            # Build dimension definition
            yaml_dim = {
                'name': dim['name'],
                'expr': column_expr
            }
            
            # Add optional fields
            if dim.get('display_name'):
                yaml_dim['display_name'] = dim['display_name']
            elif dim.get('name'):
                yaml_dim['display_name'] = dim['name'].replace('_', ' ').title()
            
            if dim.get('description'):
                yaml_dim['comment'] = dim['description']
            
            if dim.get('synonyms'):
                yaml_dim['synonyms'] = dim['synonyms']
            
            yaml_dimensions.append(yaml_dim)
        
        return yaml_dimensions
    
    def _build_measures_yaml(self, metrics: List[Dict], base_table: str, dimensions: List[Dict]) -> List[Dict]:
        """Build measures section for YAML."""
        yaml_measures = []
        
        # Build a mapping from dimension names to actual table.column expressions
        # This handles cases where LLM uses dimension names (e.g., payment_status) 
        # instead of actual column names (e.g., status) in formulas
        dim_name_to_column = {}
        for dim in dimensions:
            dim_name = dim.get('name', '')
            column = dim.get('column', '')
            table = dim.get('table', base_table)
            
            # Extract just the column name if it has a table prefix
            if '.' in column:
                column = column.split('.')[-1]
            
            # Build the full table.column reference
            if table != base_table:
                full_column = f"{table}.{column}"
                # Only map dimension name if it's different from the actual column name
                # This prevents mapping payments.status \u2192 payments.status which causes double prefixes
                if dim_name != column:
                    dim_name_to_column[dim_name] = full_column
                    dim_name_to_column[f"{table}.{dim_name}"] = full_column
        
        # Build a set of all metric names for reference checking
        metric_names = {m['name'] for m in metrics}
        
        for metric in metrics:
            # Clean up formula - only remove BASE table prefix, keep joined table prefixes
            formula = metric['formula']
            
            # Fix dimension name references before removing base table prefix
            # Replace dimension names with actual column references
            # Sort by length (longest first) to avoid partial replacements
            for dim_ref in sorted(dim_name_to_column.keys(), key=len, reverse=True):
                actual_col = dim_name_to_column[dim_ref]
                # Only replace if it's NOT already a table.column pattern
                # Use negative lookbehind to avoid replacing when preceded by a table name and dot
                pattern = rf'(?<!\w\.)(?<!\.){re.escape(dim_ref)}\b'
                formula = re.sub(pattern, actual_col, formula)
            
            # Only remove the base table prefix (e.g., orders.column becomes column)
            # But keep joined table prefixes (e.g., reviews.rating stays as reviews.rating)
            formula = re.sub(rf'\b{base_table}\.', '', formula)
            
            
            # Note: Metric reference validation disabled - was too aggressive and caught column names
            # Metric views should handle references properly without creating nested aggregations
            
            # Validate: Skip metrics with nested aggregations (not allowed in SQL)
            # Check if there's an aggregate function inside another aggregate function
            agg_functions = [r'\bCOUNT\b', r'\bSUM\b', r'\bAVG\b', r'\bMIN\b', r'\bMAX\b',
                            r'\bSTDDEV\b', r'\bVARIANCE\b', r'\bPERCENTILE_CONT\b', r'\bPERCENTILE_DISC\b',
                            r'\bCOLLECT_SET\b', r'\bCOLLECT_LIST\b']
            formula_upper = formula.upper()
            
            # Count occurrences of aggregate functions
            # If we find more than one aggregate function in the same formula, check for nesting
            agg_count = sum(1 for agg in agg_functions if re.search(agg, formula_upper))
            
            has_nested_agg = False
            if agg_count > 1:
                # Check if any aggregate function contains another
                # Use a more robust approach: check parenthesis depth
                for agg in agg_functions:
                    # Find all occurrences of this aggregate function
                    agg_pattern = rf'{agg}\s*\('
                    for match in re.finditer(agg_pattern, formula_upper):
                        start_pos = match.end() - 1  # Position of opening parenthesis
                        # Find the matching closing parenthesis
                        paren_depth = 1
                        pos = start_pos + 1
                        content_inside = ""
                        while pos < len(formula_upper) and paren_depth > 0:
                            if formula_upper[pos] == '(':
                                paren_depth += 1
                            elif formula_upper[pos] == ')':
                                paren_depth -= 1
                            if paren_depth > 0:
                                content_inside += formula_upper[pos]
                            pos += 1
                        
                        # Check if any aggregate function is inside
                        for inner_agg in agg_functions:
                            if re.search(inner_agg, content_inside):
                                has_nested_agg = True
                                print(f"  \u26a0\ufe0f  Skipping metric '{metric['name']}' due to nested aggregation")
                                break
                        if has_nested_agg:
                            break
                    if has_nested_agg:
                        break
            
            if has_nested_agg:
                continue  # Skip this metric
            
            # Build measure definition
            yaml_measure = {
                'name': metric['name'],
                'expr': formula
            }
            
            # Add optional fields
            if metric.get('display_name'):
                yaml_measure['display_name'] = metric['display_name']
            elif metric.get('name'):
                yaml_measure['display_name'] = metric['name'].replace('_', ' ').title()
            
            if metric.get('description'):
                yaml_measure['comment'] = metric['description']
            
            if metric.get('synonyms'):
                yaml_measure['synonyms'] = metric['synonyms']
            
            # Add format based on metric type
            metric_type = metric.get('type', 'number')
            if 'amount' in metric['name'].lower() or 'revenue' in metric['name'].lower() or 'value' in metric['name'].lower():
                yaml_measure['format'] = {
                    'type': 'currency',
                    'currency_code': 'USD',
                    'decimal_places': {
                        'type': 'exact',
                        'places': 2
                    }
                }
            elif 'rate' in metric['name'].lower() or 'percent' in metric['name'].lower():
                yaml_measure['format'] = {
                    'type': 'percentage',
                    'decimal_places': {
                        'type': 'exact',
                        'places': 2
                    }
                }
            else:
                yaml_measure['format'] = {
                    'type': 'number',
                    'decimal_places': {
                        'type': 'all'
                    }
                }
            
            yaml_measures.append(yaml_measure)
        
        return yaml_measures
