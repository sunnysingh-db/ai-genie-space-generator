# 🤖 AI-Powered Genie Space Generator

> Automatically generate Unity Catalog metric views and Genie spaces from natural language using AI

[![Databricks](https://img.shields.io/badge/Databricks-Unity_Catalog-FF3621?style=flat&logo=databricks)](https://databricks.com)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python)](https://python.org)
[![LLM](https://img.shields.io/badge/LLM-Claude_Opus_4-5A67D8?style=flat)](https://anthropic.com)

---

## 🎯 What It Does

**Input:** Unity Catalog location (`catalog.schema`) + business context description  
**Output:** AI-generated metric views + Genie space with sample questions

**Benefits:**
* ✅ Zero manual metric definition - AI generates everything
* ✅ Production-ready Unity Catalog assets with semantic layer
* ✅ Natural language analytics via Genie
* ✅ End-to-end automation (~60-90 seconds)

---

## 🏗️ Architecture

### Execution Pipeline

```
┌──────────────────────────────────────────────────────────┐
│  User Input: catalog + schema + business_context        │
└────────────────────────┬─────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────┐
│              main_orchestrator.py                        │
│           (Coordinates entire workflow)                  │
└──────────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
┌─────────────┐  ┌──────────────┐  ┌────────────────┐
│   config    │  │   metadata   │  │      llm       │
│   handler   │─▶│   scanner    │─▶│ orchestrator   │
│             │  │              │  │                │
│ Parse YAML  │  │ • Scan UC    │  │ • Claude Opus 4│
│             │  │ • Infer FKs  │  │ • Generate     │
│             │  │ • Sample data│  │   metrics      │
└─────────────┘  └──────────────┘  └────────────────┘
                                            │
                         ┌──────────────────┼──────────────────┐
                         │                  │                  │
                         ▼                  ▼                  ▼
                ┌────────────────┐  ┌──────────────┐  ┌──────────────┐
                │ metric_view    │  │ genie_space  │  │   Output     │
                │ generator      │  │ creator      │  │              │
                │                │  │              │  │ • Metric views│
                │ • Build YAML   │  │ • Create     │  │ • Genie space│
                │ • CREATE MODEL │  │   space      │  │ • URL        │
                │                │  │ • Add tables │  │              │
                └────────────────┘  └──────────────┘  └──────────────┘
```

### Components

| Module | Purpose | Key Tech |
|--------|---------|----------|
| **config_handler** | Parse YAML config | YAML parsing |
| **metadata_scanner** | Scan Unity Catalog | Spark SQL, `information_schema` |
| **llm_orchestrator** | Generate metrics via AI | LangChain, Claude Opus 4 |
| **metric_view_generator** | Create UC metric views | YAML semantic layer, SQL DDL |
| **genie_space_creator** | Build Genie space | Databricks SDK, REST API |
| **main_orchestrator** | Coordinate workflow | Python orchestration |

---

## ⏱️ Execution Flow

Total time: **60-90 seconds**

### Phase 1: Metadata Scanning (10-20s)
```
📊 Scanning Unity Catalog metadata...
  • Query information_schema for tables/columns
  • Sample 100 rows from each table
  • Infer foreign key relationships
✅ Complete: X tables, Y columns, Z relationships
```

### Phase 2: LLM Generation (30-40s)
```
🤖 Calling Claude Opus 4...
  • Build prompt with metadata + business context
  • LLM analyzes and generates metrics
  • Parse JSON response
✅ Generated: X metrics, Y dimensions, Z joins, sample questions
```

### Phase 3: Metric View Creation (5-10s)
```
📊 Creating Unity Catalog metric views...
  • Build YAML semantic layer per table
  • Execute CREATE MODEL statements
✅ Created: catalog.schema.metrics_tablename
```

### Phase 4: Genie Space Creation (5-10s)
```
✨ Creating Genie space...
  • Generate instructions from sample questions
  • Call Genie API to create space
  • Add tables and metric views
✅ Genie Space URL: https://[workspace]/genie/spaces/[id]
```

---

## 🚀 Quick Start

### Prerequisites

* **Databricks Workspace** with Unity Catalog
* **Runtime**: 14.3 LTS or higher
* **Permissions**: SELECT on tables, CREATE MODEL on schema, Genie space creation

### Step 1: Open Notebook

Navigate to:
```
/Workspace/Users/[your-email]/My Demos & Scripts/GenAI/Frameworks/Genie App/AI-Powered Genie Space Generator
```

### Step 2: Run the Framework

The notebook has 3 cells:

**Cell 1:** How to Use (documentation) - Read this first

**Cell 2:** Restart Python (optional)
```python
dbutils.library.restartPython()
```
*Only run this if you modified framework code*

**Cell 3:** Execute Framework
```python
import sys
framework_path = '/Workspace/Users/sunny.singh@databricks.com/My Demos & Scripts/GenAI/Frameworks/Genie App'
sys.path.insert(0, framework_path)

# Install dependencies (run once per session)
%pip install databricks-sdk langchain-databricks pyyaml --quiet

# Configure
config_yaml = """
catalog: your_catalog
schema: your_schema
business_context: |
  Describe your business domain here.
  
  Key Business Areas:
  - Area 1: Description
  - Area 2: Description
  
  Important Metrics:
  - Metric 1
  - Metric 2
"""

# Run framework
from framework.main_orchestrator import GenieSpaceFramework

framework = GenieSpaceFramework(config_yaml=config_yaml)
result = framework.run()

print(f"✅ Genie Space URL: {result['genie_space']['url']}")
```

### Step 3: Use Your Genie Space

1. Open the Genie space URL from the output
2. Try the auto-generated sample questions
3. Ask your own natural language questions

---

## 📝 Configuration Guide

### YAML Structure

```yaml
catalog: string          # UC catalog name
schema: string           # UC schema name
business_context: |      # Multi-line business description
  [Your business domain description]
  
  Key Business Areas:
  - [Area 1: details]
  - [Area 2: details]
  
  Important Metrics:
  - [Metric 1]
  - [Metric 2]
```

### Example: E-commerce

```yaml
catalog: prod_analytics
schema: ecommerce
business_context: |
  E-commerce platform analytics for sales, customers, and inventory.
  
  Key Business Areas:
  - Sales: Revenue, orders, conversion rates, cart abandonment
  - Customers: Lifetime value, retention, segments
  - Products: Top sellers, inventory turnover, out-of-stock
  
  Important Metrics:
  - Total revenue, average order value, units sold
  - Conversion rate, customer lifetime value
  - Product views, add-to-cart rate, purchase rate
```

### Tips for Good Context

* ✅ **Be specific** about business areas and desired metrics
* ✅ **Use business language**, not technical/SQL terms
* ✅ **List priorities** - most important metrics first
* ✅ **Describe relationships** - "orders belong to users"
* ✅ **Keep concise** - 200-500 words ideal
* ❌ **Don't** use table names or SQL syntax (LLM discovers these)

---

## 🐛 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **"No tables found"** | • Verify catalog/schema names<br>• Check SELECT permissions<br>• Confirm tables exist: `SHOW TABLES IN catalog.schema` |
| **"LLM timeout"** | • Retry (endpoints may be busy)<br>• Reduce number of tables in schema<br>• Simplify business context |
| **"Metric view creation failed"** | • Check generated formulas for syntax errors<br>• Look for nested aggregations in debug output<br>• Verify CREATE MODEL permission |
| **"Genie space creation failed"** | • Verify Genie creation permission<br>• Check workspace has Genie enabled<br>• Try creating Genie space manually via UI |
| **"Metric references other metrics"** | • Expected behavior - framework auto-filters these<br>• Remaining metrics are valid and will be created |

### Debug Tips

1. Check Spark UI for SQL query errors
2. Review LLM prompt and response in console output
3. Verify Unity Catalog permissions:
   ```sql
   SHOW GRANT ON SCHEMA catalog.schema;
   SHOW GRANT ON CATALOG catalog;
   ```
4. Test metric formulas manually:
   ```sql
   SELECT COUNT(order_id) FROM catalog.schema.orders;
   ```

### Getting Help

When reporting issues, include:
* Databricks Runtime version
* Full error message and stack trace
* Business context (if not sensitive)
* Number of tables in schema
* Sample generated metric that failed

---

## 📊 Generated Assets

### 1. Unity Catalog Metric Views

**Location:** `{catalog}.{schema}.metrics_{table}`

**Structure:**
```sql
CREATE MODEL IF NOT EXISTS catalog.schema.metrics_orders
AS 'version: 1.1
source: catalog.schema.orders
measures:
  - name: total_orders
    expr: COUNT(order_id)
    title: Total Orders
    description: Total number of orders placed
dimensions:
  - name: order_status
    expr: status
    title: Order Status
joins:
  - left_table: source
    right_table: users
    on: source.user_id = users.user_id
    type: left'
```

**Query Examples:**
```sql
-- View all data
SELECT * FROM catalog.schema.metrics_orders

-- Use specific metrics
SELECT MEASURE(total_orders) FROM catalog.schema.metrics_orders
```

### 2. Genie Space

**Includes:**
* All relevant source tables
* All generated metric views
* Sample questions for guidance
* Business context as instructions

**Example Questions:**
* "What is the total number of orders?"
* "Show me average order value by month"
* "What's the user retention rate?"
* "Top 10 products by revenue"

---

## 🎓 Advanced Usage

### Customize LLM Behavior

Edit `llm_orchestrator.py` to modify the prompt or change model:

```python
# Change model
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-405b-instruct",  # Use different model
    temperature=0.1
)

# Customize prompt
prompt = f"""
You are a data analytics expert. Generate metrics for:
{self.business_context}

[Add custom instructions here]
"""
```

### Add Validation Rules

Edit `metric_view_generator.py` for custom validation:

```python
# In _build_measures_yaml method
if custom_condition_not_met:
    print(f"  ⚠️  Skipping {metric['name']}: [reason]")
    continue
```

### Run Programmatically

```python
import sys
sys.path.append('/Workspace/Users/.../Genie App')

from framework.main_orchestrator import GenieSpaceFramework

config = """
catalog: my_catalog
schema: my_schema
business_context: |
  My business description
"""

framework = GenieSpaceFramework(config_yaml=config)
result = framework.run()

# Access results
print(f"Created {len(result['metric_views'])} metric views")
print(f"Genie Space: {result['genie_space']['url']}")
```

---

## 📄 License & Credits

**Built with:**
* Databricks Unity Catalog, Model Serving, Genie
* Anthropic Claude Opus 4
* LangChain

**Framework:** Provided as-is for internal use. Customize as needed.

---

**Happy metric generation!** 🎉
