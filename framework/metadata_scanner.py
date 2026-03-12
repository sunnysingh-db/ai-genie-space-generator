"""
Metadata Scanner Module (Enhanced with Table List Support)
Accepts a list of fully qualified table names (catalog.schema.table),
scans their metadata, samples data in parallel, and infers relationships.
Supports tables spanning multiple catalogs and schemas.
"""

from pyspark.sql import SparkSession
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

import logging
logging.getLogger("pyspark.sql.connect.logging").setLevel(logging.ERROR)


class MetadataScanner:
    """Scans metadata for a list of tables and infers relationships."""

    def __init__(self, spark: SparkSession, table_list: List[str] = None,
                 catalog: str = None, schema: str = None,
                 exclude_table_patterns: List[str] = None,
                 table_fq_map: Dict[str, str] = None):
        """
        Initialize metadata scanner.

        Args:
            spark: Active SparkSession
            table_list: List of fully qualified table names (catalog.schema.table).
                        Primary input — when provided, catalog/schema/exclude_patterns are ignored.
            catalog: (Legacy) Catalog name — used only if table_list is not provided
            schema: (Legacy) Schema name — used only if table_list is not provided
            exclude_table_patterns: (Legacy) Patterns to exclude — ignored when table_list is provided
            table_fq_map: Pre-built short_name->fq_name mapping (optional, built internally if not provided)
        """
        self.spark = spark

        if table_list:
            # ── New mode: explicit table list ──
            self.table_list = table_list
            self.parsed_tables = []
            for fq in table_list:
                parts = fq.split('.')
                if len(parts) != 3:
                    raise ValueError(f"Table must be fully qualified (catalog.schema.table): {fq}")
                self.parsed_tables.append({
                    'catalog': parts[0], 'schema': parts[1],
                    'table': parts[2], 'fq_name': fq
                })

            # Build or accept the short_name -> fq_name map
            if table_fq_map:
                self.table_fq_map = table_fq_map
            else:
                self.table_fq_map = self._build_fq_map()

            # For display purposes, use first table's catalog.schema
            self.catalog = self.parsed_tables[0]['catalog']
            self.schema = self.parsed_tables[0]['schema']
            self.full_schema = f"{self.catalog}.{self.schema}"
            self.exclude_patterns = []
        else:
            # ── Legacy mode: scan full schema ──
            self.table_list = None
            self.parsed_tables = None
            self.table_fq_map = {}
            self.catalog = catalog
            self.schema = schema
            self.full_schema = f"{catalog}.{schema}"
            self.exclude_patterns = exclude_table_patterns or []

        self.quoted_catalog = self._quote_identifier(self.catalog)
        self.quoted_schema = self._quote_identifier(self.schema)
        self.quoted_full_schema = f"{self.quoted_catalog}.{self.quoted_schema}"

    def _build_fq_map(self) -> Dict[str, str]:
        """Build short_name -> fq_name mapping, handling collisions."""
        short_counts = {}
        for pt in self.parsed_tables:
            short_counts[pt['table']] = short_counts.get(pt['table'], 0) + 1

        collisions = {s for s, c in short_counts.items() if c > 1}

        fq_map = {}
        for pt in self.parsed_tables:
            short = pt['table']
            if short in collisions:
                short = f"{pt['schema']}_{pt['table']}"
            fq_map[short] = pt['fq_name']

        return fq_map

    def _quote_identifier(self, identifier: str) -> str:
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            return identifier
        return f"`{identifier}`"

    def scan(self) -> Dict[str, Any]:
        """Perform complete metadata scan."""
        if self.table_list:
            print(f"Scanning metadata for {len(self.table_list)} specified tables...")
        else:
            print(f"Scanning metadata for {self.full_schema}...")

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

        print(f"Scan complete: {len(tables)} tables, {len(columns)} columns, {len(relationships)} relationships")
        return result

    def _get_tables(self) -> List[Dict[str, Any]]:
        """Get table metadata for the specified tables or full schema."""
        if self.table_list:
            return self._get_tables_from_list()
        else:
            return self._get_tables_from_schema()

    def _get_tables_from_list(self) -> List[Dict[str, Any]]:
        """Get table metadata for specific tables in table_list."""
        print(f"  Querying metadata for {len(self.parsed_tables)} tables...")

        # Group by catalog.schema for efficient queries
        schema_groups = {}
        for pt in self.parsed_tables:
            key = (pt['catalog'], pt['schema'])
            schema_groups.setdefault(key, []).append(pt['table'])

        tables = []
        for (cat, sch), table_names in schema_groups.items():
            qcat = self._quote_identifier(cat)
            names_sql = ', '.join(f"\'{t}\'" for t in table_names)

            try:
                info_df = self.spark.sql(f"""
                    SELECT table_name, table_type, comment
                    FROM {qcat}.information_schema.tables
                    WHERE table_schema = '{sch}'
                      AND table_name IN ({names_sql})
                """)
                info_lookup = {row.table_name: row.asDict() for row in info_df.collect()}
            except Exception:
                info_lookup = {}

            for tname in table_names:
                info = info_lookup.get(tname, {})
                table_type = info.get('table_type', 'UNKNOWN')
                if table_type == 'METRIC_VIEW':
                    continue
                tables.append({
                    'table_catalog': cat,
                    'table_schema': sch,
                    'table_name': tname,
                    'table_type': table_type,
                    'comment': info.get('comment', None)
                })

        self._validated_table_names = {t['table_name'] for t in tables}
        print(f"  Found {len(tables)} validated tables")
        return tables

    def _get_tables_from_schema(self) -> List[Dict[str, Any]]:
        """Legacy: scan full schema for tables."""
        print(f"  Querying tables in {self.full_schema}...")
        show_df = self.spark.sql(f"SHOW TABLES IN {self.quoted_full_schema}")
        show_tables = [row.tableName for row in show_df.collect()]

        info_query = f"""
            SELECT table_name, table_type, comment
            FROM {self.quoted_catalog}.information_schema.tables
            WHERE table_schema = '{self.schema}'
        """
        info_df = self.spark.sql(info_query)
        info_lookup = {row.table_name: row.asDict() for row in info_df.collect()}

        if self.exclude_patterns:
            compiled_patterns = []
            for pattern in self.exclude_patterns:
                regex_str = pattern.replace('%', '.*').replace('_', '.')
                compiled_patterns.append(re.compile(regex_str, re.IGNORECASE))
            show_tables = [
                t for t in show_tables
                if not any(p.fullmatch(t) for p in compiled_patterns)
            ]

        tables = []
        for table_name in show_tables:
            info = info_lookup.get(table_name, {})
            table_type = info.get('table_type', 'UNKNOWN')
            if table_type == 'METRIC_VIEW':
                continue
            tables.append({
                'table_catalog': self.catalog,
                'table_schema': self.schema,
                'table_name': table_name,
                'table_type': table_type,
                'comment': info.get('comment', None)
            })

        self._validated_table_names = {t['table_name'] for t in tables}
        print(f"  Found {len(tables)} validated tables")
        return tables

    def _get_columns(self) -> List[Dict[str, Any]]:
        """Query columns for validated tables."""
        if self.table_list:
            return self._get_columns_from_list()
        else:
            return self._get_columns_from_schema()

    def _get_columns_from_list(self) -> List[Dict[str, Any]]:
        """Get columns for specific tables, grouped by catalog.schema."""
        schema_groups = {}
        for pt in self.parsed_tables:
            if pt['table'] in self._validated_table_names:
                key = (pt['catalog'], pt['schema'])
                schema_groups.setdefault(key, []).append(pt['table'])

        all_columns = []
        for (cat, sch), table_names in schema_groups.items():
            qcat = self._quote_identifier(cat)
            names_sql = ', '.join(f"\'{t}\'" for t in table_names)
            try:
                df = self.spark.sql(f"""
                    SELECT table_catalog, table_schema, table_name, column_name,
                           ordinal_position, data_type, is_nullable, column_default, comment
                    FROM {qcat}.information_schema.columns
                    WHERE table_schema = '{sch}'
                      AND table_name IN ({names_sql})
                    ORDER BY table_name, ordinal_position
                """)
                all_columns.extend([row.asDict() for row in df.collect()])
            except Exception as e:
                print(f"  Could not query columns for {cat}.{sch}: {e}")

        print(f"  Found {len(all_columns)} columns across validated tables")
        return all_columns

    def _get_columns_from_schema(self) -> List[Dict[str, Any]]:
        """Legacy: get columns from full schema."""
        query = f"""
            SELECT table_catalog, table_schema, table_name, column_name,
                   ordinal_position, data_type, is_nullable, column_default, comment
            FROM {self.quoted_catalog}.information_schema.columns
            WHERE table_schema = '{self.schema}'
            ORDER BY table_name, ordinal_position
        """
        df = self.spark.sql(query)
        columns = [row.asDict() for row in df.collect()]

        if hasattr(self, '_validated_table_names') and self._validated_table_names:
            columns = [c for c in columns if c['table_name'] in self._validated_table_names]

        print(f"  Found {len(columns)} columns across validated tables")
        return columns

    def _sample_tables(self, tables: List[Dict[str, Any]], limit: int = 100, max_workers: int = 10) -> Dict[str, List[Dict]]:
        """Sample data from tables in parallel using FQ names."""
        if not tables:
            return {}

        samples = {}
        total_tables = len(tables)
        print(f"  Sampling {limit} rows from {total_tables} tables...")
        start_time = time.time()

        # Build FQ lookup: short_name -> quoted FQ name for sampling
        if self.table_list:
            fq_lookup = {}
            for pt in self.parsed_tables:
                fq_lookup[pt['table']] = (
                    f"{self._quote_identifier(pt['catalog'])}."
                    f"{self._quote_identifier(pt['schema'])}."
                    f"{self._quote_identifier(pt['table'])}"
                )
        else:
            fq_lookup = {
                t['table_name']: f"{self.quoted_full_schema}.{self._quote_identifier(t['table_name'])}"
                for t in tables
            }

        def sample_single_table(table):
            table_name = table['table_name']
            fq = fq_lookup.get(table_name)
            if not fq:
                return table_name, [], None
            try:
                rows = self.spark.table(fq).limit(limit).collect()
                return table_name, [row.asDict() for row in rows], None
            except Exception as e:
                return table_name, [], str(e)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(sample_single_table, t): t for t in tables}
            completed = 0
            for future in as_completed(futures):
                table_name, data, error = future.result()
                completed += 1
                if error:
                    print(f"    [{completed}/{total_tables}] {table_name}: Error - {error[:80]}")
                samples[table_name] = data

        elapsed = time.time() - start_time
        print(f"  Sampling complete ({elapsed:.1f}s)")
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
