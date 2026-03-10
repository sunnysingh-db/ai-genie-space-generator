"""
Metadata Scanner Module (Enhanced with Parallel Processing)
Scans Unity Catalog information_schema for table metadata, samples data in parallel,
and infers relationships between tables.
"""

from pyspark.sql import SparkSession
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

# Suppress pyspark connect session warnings
import logging
logging.getLogger("pyspark.sql.connect.logging").setLevel(logging.ERROR)

class MetadataScanner:
    """Scans schema metadata and infers table relationships with parallel processing."""
    
    def __init__(self, spark: SparkSession, catalog: str, schema: str, exclude_table_patterns: List[str] = None):
        """
        Initialize metadata scanner.
        
        Args:
            spark: Active SparkSession
            catalog: Catalog name
            schema: Schema name
            exclude_table_patterns: List of SQL LIKE patterns to exclude tables (e.g., ['%_logs%', 'monitor_%'])
                                   Empty list or None scans all tables
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        self.exclude_patterns = exclude_table_patterns if exclude_table_patterns else []
        
        # Pre-compute backtick-quoted identifiers for SQL contexts
        self.quoted_catalog = self._quote_identifier(catalog)
        self.quoted_schema = self._quote_identifier(schema)
        self.quoted_full_schema = f"{self.quoted_catalog}.{self.quoted_schema}"
    
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote identifier if it contains characters that require backticks.
        
        Args:
            identifier: Table or column name
            
        Returns:
            Backticked identifier if needed, otherwise unchanged
        """
        # Check if identifier contains invalid characters (hyphens, spaces, etc.)
        # Valid unquoted identifiers: alphanumeric + underscore, not starting with digit
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            # Valid identifier, no backticks needed
            return identifier
        else:
            # Invalid identifier, wrap in backticks
            return f"`{identifier}`"
    
    def scan(self) -> Dict[str, Any]:
        """
        Perform complete metadata scan including tables, columns, samples, and relationships.
        
        Returns:
            Dictionary containing:
                - tables: List of table metadata
                - columns: List of column metadata
                - samples: Dictionary of table samples
                - relationships: Inferred FK relationships
        """
        print(f"🔍 Scanning metadata for {self.full_schema}...")
        
        tables = self._get_tables()
        columns = self._get_columns()
        samples = self._sample_tables(tables)
        relationships = self._infer_relationships(columns, samples)
        
        result = {
            'tables': tables,
            'columns': columns,
            'samples': samples,
            'relationships': relationships
        }
        
        print(f"✅ Scan complete: {len(tables)} tables, {len(columns)} columns, {len(relationships)} relationships")
        return result
    
    def _get_tables(self) -> List[Dict[str, Any]]:
        """
        Query information_schema.tables for table metadata.
        Supports scanning 100+ tables without limits.
        Applies configurable exclusion patterns.
        """
        # Build WHERE clause with dynamic exclusion patterns
        where_clauses = [
            f"table_schema = '{self.schema}'",
            "table_type = 'MANAGED'"
        ]
        
        # Add NOT LIKE clauses for each exclusion pattern
        for pattern in self.exclude_patterns:
            where_clauses.append(f"table_name NOT LIKE '{pattern}'")
        
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
            SELECT 
                table_catalog,
                table_schema,
                table_name,
                table_type,
                comment
            FROM {self.quoted_catalog}.information_schema.tables
            WHERE {where_clause}
            ORDER BY table_name
        """
        
        print(f"  📊 Querying tables in {self.full_schema}...")
        if self.exclude_patterns:
            print(f"  🔍 Excluding patterns: {', '.join(self.exclude_patterns)}")
        else:
            print(f"  🔍 Scanning ALL tables (no exclusions)")
        
        df = self.spark.sql(query)
        tables = [row.asDict() for row in df.collect()]
        
        # Enhanced logging
        table_count = len(tables)
        print(f"  ✅ Found {table_count} tables")
        
        if table_count == 0:
            print(f"  ⚠️  Warning: No tables found in {self.full_schema}")
        elif table_count > 50:
            print(f"  📈 Large schema detected: {table_count} tables will be scanned")
        
        return tables
    
    def _get_columns(self) -> List[Dict[str, Any]]:
        """Query information_schema.columns for column metadata."""
        query = f"""
            SELECT 
                table_catalog,
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                data_type,
                is_nullable,
                column_default,
                comment
            FROM {self.quoted_catalog}.information_schema.columns
            WHERE table_schema = '{self.schema}'
            ORDER BY table_name, ordinal_position
        """
        
        df = self.spark.sql(query)
        columns = [row.asDict() for row in df.collect()]
        print(f"  📋 Found {len(columns)} columns across all tables")
        return columns
    
    def _sample_tables(self, tables: List[Dict[str, Any]], limit: int = 100, max_workers: int = 10) -> Dict[str, List[Dict]]:
        """
        Sample data from tables in PARALLEL for faster execution.
        
        Args:
            tables: List of table metadata
            limit: Number of rows to sample per table
            max_workers: Number of parallel workers (default: 10)
            
        Returns:
            Dictionary mapping table names to sample data
        """
        if not tables:
            print(f"  ⚠️  No tables to sample")
            return {}
        
        samples = {}
        total_tables = len(tables)
        
        print(f"  🎲 Sampling {limit} rows from {total_tables} tables...")
        print(f"  ⚡ Using parallel processing ({max_workers} workers)")
        start_time = time.time()
        
        def sample_single_table(table: Dict[str, Any]) -> tuple:
            """Sample a single table (executed in parallel)."""
            table_name = table['table_name']
            # Quote table name if it contains invalid characters (hyphens, etc.)
            quoted_table_name = self._quote_identifier(table_name)
            full_name = f"{self.quoted_full_schema}.{quoted_table_name}"
            
            try:
                df = self.spark.table(full_name).limit(limit)
                sample_data = [row.asDict() for row in df.collect()]
                return table_name, sample_data, None
            except Exception as e:
                return table_name, [], str(e)
        
        # Execute sampling in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all sampling tasks
            future_to_table = {executor.submit(sample_single_table, t): t for t in tables}
            
            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_table):
                table_name, sample_data, error = future.result()
                samples[table_name] = sample_data
                completed += 1
                
                # Progress logging
                if error:
                    print(f"    ✗ [{completed}/{total_tables}] {table_name}: {error[:100]}")
                else:
                    print(f"    ✓ [{completed}/{total_tables}] {table_name}: {len(sample_data)} rows")
        
        elapsed = time.time() - start_time
        tables_per_sec = total_tables / elapsed if elapsed > 0 else 0
        print(f"  ⚡ Parallel sampling complete in {elapsed:.2f}s ({tables_per_sec:.1f} tables/sec)")
        
        # Summary stats
        successful = len([s for s in samples.values() if len(s) > 0])
        failed = total_tables - successful
        print(f"  📊 Sampling summary: {successful} successful, {failed} failed")
        
        return samples
    
    def _infer_relationships(self, columns: List[Dict[str, Any]], samples: Dict[str, List[Dict]]) -> List[Dict[str, Any]]:
        """
        Infer foreign key relationships based on naming patterns and data overlap.
        
        Args:
            columns: List of column metadata
            samples: Dictionary of table samples
            
        Returns:
            List of inferred relationships
        """
        relationships = []
        print(f"  🔗 Inferring relationships...")
        
        # Group columns by table
        tables_columns = {}
        for col in columns:
            table_name = col['table_name']
            if table_name not in tables_columns:
                tables_columns[table_name] = []
            tables_columns[table_name].append(col)
        
        # Look for FK patterns: {table}_id columns
        for table_name, table_cols in tables_columns.items():
            for col in table_cols:
                col_name = col['column_name']
                
                # Pattern: {other_table}_id
                match = re.match(r'(.+)_id$', col_name)
                if match:
                    referenced_table = match.group(1) + 's'  # Try plural form
                    
                    # Check if referenced table exists
                    if referenced_table in tables_columns:
                        # Check if referenced table has an 'id' column
                        ref_cols = [c['column_name'] for c in tables_columns[referenced_table]]
                        if 'id' in ref_cols or f'{referenced_table[:-1]}_id' in ref_cols:
                            relationships.append({
                                'from_table': table_name,
                                'from_column': col_name,
                                'to_table': referenced_table,
                                'to_column': 'id',
                                'confidence': 'high'
                            })
                            print(f"    ✓ {table_name}.{col_name} → {referenced_table}.id")
        
        if len(relationships) == 0:
            print(f"    ⚠️  No relationships detected")
        
        return relationships
