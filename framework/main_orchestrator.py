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
        print("\U0001f680 AI-POWERED GENIE SPACE GENERATOR FRAMEWORK")
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
            print("\n\U0001f4cb LLM DECISIONS:")
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
            
            # Collect stats
            elapsed_time = time.time() - start_time
            n_tables = len(metadata['tables'])
            n_views = len(metric_views_result.get('views', []))
            n_dimensions = len(llm_config.get('dimensions', []))
            n_measures = len(llm_config.get('measures', []))
            n_joins = len(llm_config.get('joins', []))
            metric_views_url = metric_views_result.get('url', '')
            genie_url = genie_space.get('url', '')
            genie_id = genie_space.get('genie_space_id', '')

            # Display HTML result card
            self._display_result_html(
                elapsed_time=elapsed_time,
                n_tables=n_tables,
                n_views=n_views,
                n_dimensions=n_dimensions,
                n_measures=n_measures,
                n_joins=n_joins,
                metric_views_url=metric_views_url,
                genie_url=genie_url,
                genie_id=genie_id
            )
            
            return result
        
        except Exception as e:
            print(f"\n\u274c FRAMEWORK EXECUTION FAILED: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def _display_result_html(self, elapsed_time, n_tables, n_views,
                              n_dimensions, n_measures, n_joins,
                              metric_views_url, genie_url, genie_id):
        """Display an attractive HTML card with links to generated assets."""
        from IPython.display import display, HTML

        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        html = f"""
        <div style="
            margin: 24px 0;
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 8px 32px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.06);
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 720px;
        ">
            <!-- Hero Header -->
            <div style="
                background: linear-gradient(135deg, #0d47a1 0%, #1565c0 30%, #1e88e5 60%, #42a5f5 100%);
                padding: 28px 28px 22px;
                position: relative;
                overflow: hidden;
            ">
                <div style="
                    position: absolute; top: -30px; right: -30px;
                    width: 140px; height: 140px;
                    background: rgba(255,255,255,0.06);
                    border-radius: 50%;
                "></div>
                <div style="
                    position: absolute; bottom: -20px; right: 60px;
                    width: 80px; height: 80px;
                    background: rgba(255,255,255,0.04);
                    border-radius: 50%;
                "></div>
                <div style="display: flex; align-items: center; gap: 14px; position: relative;">
                    <div style="
                        width: 52px; height: 52px;
                        background: rgba(255,255,255,0.15);
                        backdrop-filter: blur(10px);
                        border-radius: 14px;
                        display: flex; align-items: center; justify-content: center;
                        font-size: 26px;
                    ">&#x2728;</div>
                    <div>
                        <div style="color: white; font-size: 20px; font-weight: 700; letter-spacing: -0.3px;">
                            {self.genie_space_name}
                        </div>
                        <div style="color: rgba(255,255,255,0.75); font-size: 13px; margin-top: 3px;">
                            {self.catalog}.{self.schema} &nbsp;&#x00b7;&nbsp; Generated in {time_str}
                        </div>
                    </div>
                </div>
            </div>

            <!-- Stats Grid -->
            <div style="
                background: #f8faff;
                padding: 18px 28px;
                display: grid;
                grid-template-columns: repeat(5, 1fr);
                gap: 8px;
                border-bottom: 1px solid #e3eaf5;
            ">
                <div style="text-align: center;">
                    <div style="font-size: 22px; font-weight: 700; color: #0d47a1;">{n_tables}</div>
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Tables</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 22px; font-weight: 700; color: #1565c0;">{n_views}</div>
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Metric Views</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 22px; font-weight: 700; color: #1e88e5;">{n_dimensions}</div>
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Dimensions</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 22px; font-weight: 700; color: #42a5f5;">{n_measures}</div>
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Measures</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 22px; font-weight: 700; color: #64b5f6;">{n_joins}</div>
                    <div style="font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Joins</div>
                </div>
            </div>

            <!-- Action Buttons -->
            <div style="
                background: white;
                padding: 22px 28px;
                display: flex;
                gap: 12px;
                align-items: center;
            ">
                <a href="{genie_url}" target="_blank" style="
                    flex: 1;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    gap: 8px;
                    padding: 14px 20px;
                    background: linear-gradient(135deg, #0d47a1, #1565c0);
                    color: white;
                    text-decoration: none;
                    border-radius: 10px;
                    font-size: 14px;
                    font-weight: 600;
                    box-shadow: 0 4px 14px rgba(13,71,161,0.35);
                    letter-spacing: -0.1px;
                ">
                    <span style="font-size: 18px;">&#x1f52e;</span>
                    Open Genie Space
                </a>

                <a href="{metric_views_url}" target="_blank" style="
                    flex: 1;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    gap: 8px;
                    padding: 14px 20px;
                    background: white;
                    color: #0d47a1;
                    text-decoration: none;
                    border-radius: 10px;
                    font-size: 14px;
                    font-weight: 600;
                    border: 2px solid #bbdefb;
                    letter-spacing: -0.1px;
                ">
                    <span style="font-size: 18px;">&#x1f4ca;</span>
                    View Metric Views
                </a>
            </div>

            <!-- Footer -->
            <div style="
                background: #f8faff;
                padding: 12px 28px;
                border-top: 1px solid #e3eaf5;
                display: flex;
                align-items: center;
                gap: 8px;
            ">
                <span style="font-size: 11px; color: #999;">
                    &#x1f916; Powered by <strong style="color: #666;">{self.llm_model}</strong>
                    &nbsp;&#x00b7;&nbsp; Genie ID: <code style="font-size: 10px; color: #888;">{genie_id}</code>
                </span>
            </div>
        </div>
        """
        display(HTML(html))
