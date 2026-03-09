"""
Main Orchestrator Module
Ties together all framework components. All configuration flows from config.yaml.
"""

from pyspark.sql import SparkSession
from typing import Dict, Any
import time

from .config_handler import ConfigHandler
from .metadata_scanner import MetadataScanner
from .llm_orchestrator import LLMOrchestrator
from .metric_view_generator import MetricViewGenerator
from .genie_space_creator import GenieSpaceCreator


class GenieSpaceFramework:
    """Main orchestrator for AI-powered Genie space generation."""
    
    def __init__(self, config_yaml: str = None, config_path: str = None):
        if config_path:
            self.config_handler = ConfigHandler(config_path=config_path)
        elif config_yaml:
            self.config_handler = ConfigHandler(config_yaml=config_yaml)
        else:
            raise ValueError("Must provide either config_yaml or config_path")
        
        self.spark = SparkSession.builder.getOrCreate()
        
        # Extract all config values
        self.catalog = self.config_handler.get('catalog')
        self.schema = self.config_handler.get('schema')
        self.business_context = self.config_handler.get('business_context')
        self.llm_model = self.config_handler.get('llm_model', 'databricks-claude-opus-4-6')
        self.sample_questions = self.config_handler.get_sample_questions()
        self.genie_space_name = self.config_handler.get_genie_space_name()
        self.warehouse_id = self.config_handler.get_warehouse_id()
        self.genie_description = self.config_handler.get_genie_description()
        
        print("=" * 80)
        print("🚀 AI-POWERED GENIE SPACE GENERATOR FRAMEWORK")
        print("=" * 80)
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"LLM Model: {self.llm_model}")
        print(f"Genie Space: {self.genie_space_name}")
        print(f"Warehouse: {self.warehouse_id or 'auto-detect'}")
        print(f"Sample Questions: {len(self.sample_questions)} from config")
        print(f"Business Context: {self.business_context[:100]}...")
        print("=" * 80)
    
    def run(self) -> Dict[str, Any]:
        """Execute complete framework workflow."""
        start_time = time.time()
        result = {}
        
        try:
            # Step 1: Scan Metadata
            print("\n" + "=" * 80)
            print("STEP 1: SCANNING METADATA")
            print("=" * 80)
            exclude_patterns = self.config_handler.get('exclude_table_patterns', [])
            scanner = MetadataScanner(self.spark, self.catalog, self.schema, exclude_patterns)
            metadata = scanner.scan()
            result['metadata'] = metadata
            
            # Step 2: Generate Configuration with LLM
            print("\n" + "=" * 80)
            print("STEP 2: GENERATING METRICS CONFIGURATION")
            print("=" * 80)
            llm_orchestrator = LLMOrchestrator(
                business_context=self.business_context,
                llm_model=self.llm_model,
                sample_questions=self.sample_questions
            )
            llm_config = llm_orchestrator.generate_metrics_config(metadata)
            result['llm_config'] = llm_config
            
            # Log LLM decisions
            print("\n📋 LLM DECISIONS:")
            tables = llm_config.get('relevant_tables', [])
            print(f"  Selected Tables: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}")
            print(f"  Dimensions: {len(llm_config.get('dimensions', []))}")
            print(f"  Measures: {len(llm_config.get('measures', []))}")
            print(f"  Joins: {len(llm_config.get('joins', []))}")
            
            # Step 3: Create Metric Views
            print("\n" + "=" * 80)
            print("STEP 3: CREATING METRIC VIEWS")
            print("=" * 80)
            view_generator = MetricViewGenerator(self.spark, self.catalog, self.schema)
            metric_views_result = view_generator.create_metric_views(llm_config)
            result['metric_views'] = metric_views_result
            
            # Step 4: Create Genie Space
            print("\n" + "=" * 80)
            print("STEP 4: CREATING GENIE SPACE")
            print("=" * 80)
            genie_creator = GenieSpaceCreator(self.catalog, self.schema)
            genie_space = genie_creator.create_genie_space(
                config=llm_config,
                metric_views=metric_views_result.get('views', []),
                business_context=self.business_context,
                genie_space_name=self.genie_space_name,
                genie_description=self.genie_description
            )
            result['genie_space'] = genie_space
            
            # Summary
            elapsed_time = time.time() - start_time
            print("\n" + "=" * 80)
            print("✅ FRAMEWORK EXECUTION COMPLETE")
            print("=" * 80)
            print(f"⏱️  Total Time: {elapsed_time:.2f} seconds")
            print(f"📊 Tables Scanned: {len(metadata['tables'])}")
            print(f"📈 Metric Views Created: {len(metric_views_result.get('views', []))}")
            
            if metric_views_result.get('url'):
                print(f"\n🔗 METRIC VIEWS URL:")
                print(f"   {metric_views_result['url']}")
            
            print(f"\n✨ GENIE SPACE: {self.genie_space_name}")
            print(f"   ID: {genie_space['genie_space_id']}")
            print(f"   URL: {genie_space['url']}")
            print("=" * 80)
            
            return result
        
        except Exception as e:
            print(f"\n❌ FRAMEWORK EXECUTION FAILED: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
